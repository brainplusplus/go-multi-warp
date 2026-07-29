package proxy

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/autoclaw/go-multi-warp/internal/pool"
	"go.uber.org/zap"
)

type HTTPProxyServer struct {
	state       *State
	log         *zap.Logger
	activeConns atomic.Int64
	drainWg     sync.WaitGroup
}

func NewHTTPProxyServer(state *State, log *zap.Logger) *HTTPProxyServer {
	return &HTTPProxyServer{state: state, log: log}
}

// drain waits for active connections to finish (up to timeout).
func (s *HTTPProxyServer) drain(ctx context.Context) {
	// Wait for active conns to drain with a deadline
	drainTimeout := s.state.Cfg.Control.DrainTimeout.Duration
	if drainTimeout <= 0 {
		drainTimeout = 30 * time.Second
	}
	deadline := time.After(drainTimeout)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for s.activeConns.Load() > 0 {
		select {
		case <-deadline:
			s.log.Warn("drain timeout, forcing close",
				zap.Int64("remaining_conns", s.activeConns.Load()))
			return
		case <-ticker.C:
			// keep waiting
		}
	}
	s.log.Info("all connections drained")
}

func (s *HTTPProxyServer) Serve(ctx context.Context) error {
	addr, err := net.ResolveTCPAddr("tcp", s.state.Cfg.Listen.HTTP)
	if err != nil {
		return err
	}
	ln, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return err
	}
	s.log.Info("HTTP proxy listening", zap.String("addr", s.state.Cfg.Listen.HTTP))

	// Graceful shutdown: stop accepting, drain active connections
	go func() {
		<-ctx.Done()
		ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				// Listener closed — drain remaining active connections
				s.drain(ctx)
				return nil
			}
			s.log.Warn("http accept error", zap.Error(err))
			continue
		}
		s.activeConns.Add(1)
		go func() {
			defer s.activeConns.Add(-1)
			s.handle(ctx, conn)
		}()
	}
}

func (s *HTTPProxyServer) handle(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	peer := conn.RemoteAddr()
	lease, err := s.state.Limiter.Acquire(peer)
	if err != nil {
		s.log.Debug("limiter reject", zap.String("peer", peer.String()), zap.Error(err))
		s.reply502(conn, "upstream limit")
		return
	}
	defer lease.Release()

	br := bufio.NewReader(conn)

	req, err := http.ReadRequest(br)
	if err != nil {
		s.log.Debug("bad http request", zap.Error(err))
		s.reply502(conn, "bad request")
		return
	}

	// auth
	if err := s.state.Auth.CheckHTTPHeader(req.Header.Get("Proxy-Authorization")); err != nil {
		if errors.Is(err, ErrAuthRequired) {
			s.reply407(conn)
		} else {
			s.reply407(conn)
		}
		return
	}

	sticky := pool.StickyKey(peer)

	if req.Method == http.MethodConnect {
		s.handleConnect(ctx, conn, req, sticky)
	} else {
		s.handlePlain(ctx, conn, req, br, sticky)
	}
}

func (s *HTTPProxyServer) handleConnect(ctx context.Context, conn net.Conn, req *http.Request, sticky uint64) {
	host, port, err := SplitHostPort(req.Host, 443)
	if err != nil {
		s.reply502(conn, "bad host")
		return
	}
	upstream, backendLease, err := DialViaSocks(ctx, s.state, host, port, sticky)
	if err != nil {
		s.reply502(conn, err.Error())
		return
	}
	defer backendLease.Release()
	defer upstream.Close()

	conn.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n"))

	// CONNECT tunneling: pass isStreaming=true — we don't know yet if the
	// inner connection is SSE, but we must not kill long-lived tunnels.
	_, _ = CopyBidirectional(conn, upstream, s.state.Cfg.Limits.IOTimeout.Duration, true)
}

func (s *HTTPProxyServer) handlePlain(ctx context.Context, conn net.Conn, req *http.Request, br *bufio.Reader, sticky uint64) {
	// absolute-form: http://host/path
	target := req.URL.String()
	if strings.HasPrefix(target, "http://") {
		rest := strings.TrimPrefix(target, "http://")
		slash := strings.Index(rest, "/")
		var hostport string
		if slash >= 0 {
			hostport = rest[:slash]
			req.URL.Path = rest[slash:]
			req.URL.RawQuery = ""
			if req.URL.Path == "" {
				req.URL.Path = "/"
			}
		} else {
			hostport = rest
			req.URL.Path = "/"
		}
		req.Host = hostport
	}

	host, port, err := SplitHostPort(req.Host, 80)
	if err != nil {
		s.reply502(conn, "bad host")
		return
	}

	upstream, backendLease, err := DialViaSocks(ctx, s.state, host, port, sticky)
	if err != nil {
		s.reply502(conn, err.Error())
		return
	}
	defer backendLease.Release()
	defer upstream.Close()

	// strip proxy headers, write request
	req.RequestURI = ""
	req.Header.Del("Proxy-Authorization")
	req.Header.Del("Proxy-Connection")
	req.Header.Set("Connection", "close")

	if err := req.Write(upstream); err != nil {
		s.reply502(conn, "write request")
		return
	}

	// Read response headers from upstream, then stream body directly.
	// CRITICAL for SSE: do NOT buffer the entire response.
	resp, err := http.ReadResponse(bufio.NewReader(upstream), req)
	if err != nil {
		s.reply502(conn, "bad upstream response")
		return
	}
	defer resp.Body.Close()

	// Write response headers to client
	if err := resp.Write(conn); err != nil {
		return
	}

	// Detect SSE streaming by Content-Type
	isStreaming := strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream")

	// Stream body directly — no buffering, pipe as chunks arrive.
	// Use idle timeout from config, auto-extended for SSE via CopyBidirectional.
	_, _ = CopyBidirectional(conn, &bodyWrapper{resp.Body, upstream}, s.state.Cfg.Limits.IOTimeout.Duration, isStreaming)
}

// bodyWrapper adapts http.Response.Body to net.Conn for CopyBidirectional.
type bodyWrapper struct {
	body io.ReadCloser
	conn net.Conn
}

func (bw *bodyWrapper) Read(p []byte) (int, error) {
	return bw.body.Read(p)
}

func (bw *bodyWrapper) Write(p []byte) (int, error) {
	return bw.conn.Write(p)
}

func (bw *bodyWrapper) Close() error {
	return bw.body.Close()
}

func (bw *bodyWrapper) LocalAddr() net.Addr  { return bw.conn.LocalAddr() }
func (bw *bodyWrapper) RemoteAddr() net.Addr { return bw.conn.RemoteAddr() }
func (bw *bodyWrapper) SetDeadline(t time.Time) error {
	return bw.conn.SetDeadline(t)
}
func (bw *bodyWrapper) SetReadDeadline(t time.Time) error {
	return bw.conn.SetReadDeadline(t)
}
func (bw *bodyWrapper) SetWriteDeadline(t time.Time) error {
	return bw.conn.SetWriteDeadline(t)
}

func (s *HTTPProxyServer) reply407(conn net.Conn) {
	body := "Proxy Authentication Required"
	resp := fmt.Sprintf(
		"HTTP/1.1 407 Proxy Authentication Required\r\n"+
			"Proxy-Authenticate: Basic realm=\"go-multi-warp\"\r\n"+
			"Content-Length: %d\r\n"+
			"Connection: close\r\n\r\n%s",
		len(body), body)
	conn.Write([]byte(resp))
}

func (s *HTTPProxyServer) reply502(conn net.Conn, msg string) {
	body := fmt.Sprintf("Bad Gateway: %s", msg)
	resp := fmt.Sprintf(
		"HTTP/1.1 502 Bad Gateway\r\n"+
			"Content-Length: %d\r\n"+
			"Connection: close\r\n\r\n%s",
		len(body), body)
	conn.Write([]byte(resp))
}
