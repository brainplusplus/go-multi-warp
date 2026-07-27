package proxy

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"

	"github.com/autoclaw/go-multi-warp/internal/pool"
	"go.uber.org/zap"
)

type HTTPProxyServer struct {
	state *State
	log   *zap.Logger
}

func NewHTTPProxyServer(state *State, log *zap.Logger) *HTTPProxyServer {
	return &HTTPProxyServer{state: state, log: log}
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

	go func() {
		<-ctx.Done()
		ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			s.log.Warn("http accept error", zap.Error(err))
			continue
		}
		go s.handle(ctx, conn)
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

	_, _ = CopyBidirectional(conn, upstream, s.state.Cfg.Limits.IOTimeout.Duration)
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

	// stream response back
	resp, err := http.ReadResponse(br, req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	resp.Write(conn)
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
