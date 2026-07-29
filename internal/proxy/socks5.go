package proxy

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/autoclaw/go-multi-warp/internal/pool"
	"go.uber.org/zap"
)

type Socks5Server struct {
	state       *State
	log         *zap.Logger
	activeConns atomic.Int64
	drainWg     sync.WaitGroup
}

func NewSocks5Server(state *State, log *zap.Logger) *Socks5Server {
	return &Socks5Server{state: state, log: log}
}

func (s *Socks5Server) Serve(ctx context.Context) error {
	addr, err := net.ResolveTCPAddr("tcp", s.state.Cfg.Listen.Socks5)
	if err != nil {
		return err
	}
	ln, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return err
	}
	s.log.Info("SOCKS5 listening", zap.String("addr", s.state.Cfg.Listen.Socks5))

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
			s.log.Warn("socks accept error", zap.Error(err))
			continue
		}
		s.activeConns.Add(1)
		go func() {
			defer s.activeConns.Add(-1)
			s.handle(ctx, conn)
		}()
	}
}

// drain waits for active connections to finish (up to timeout).
func (s *Socks5Server) drain(ctx context.Context) {
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
	s.log.Info("all SOCKS5 connections drained")
}

func (s *Socks5Server) handle(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	peer := conn.RemoteAddr()
	lease, err := s.state.Limiter.Acquire(peer)
	if err != nil {
		s.log.Debug("limiter reject", zap.String("peer", peer.String()), zap.Error(err))
		s.replyReject(conn, 0x01)
		return
	}
	defer lease.Release()

	br := bufio.NewReader(conn)

	// greeting
	hdr := make([]byte, 2)
	if _, err := io.ReadFull(br, hdr); err != nil {
		return
	}
	if hdr[0] != 0x05 {
		return
	}
	nmethods := int(hdr[1])
	methods := make([]byte, nmethods)
	if _, err := io.ReadFull(br, methods); err != nil {
		return
	}

	// auth
	if s.state.Auth.enabled {
		if !containsMethod(methods, 0x02) {
			s.replyReject(conn, 0xFF)
			return
		}
		if _, err := conn.Write([]byte{0x05, 0x02}); err != nil {
			return
		}
		// RFC 1929 username/password
		ver := make([]byte, 1)
		if _, err := io.ReadFull(br, ver); err != nil {
			return
		}
		ulen := make([]byte, 1)
		if _, err := io.ReadFull(br, ulen); err != nil {
			return
		}
		user := make([]byte, int(ulen[0]))
		if _, err := io.ReadFull(br, user); err != nil {
			return
		}
		plen := make([]byte, 1)
		if _, err := io.ReadFull(br, plen); err != nil {
			return
		}
		pass := make([]byte, int(plen[0]))
		if _, err := io.ReadFull(br, pass); err != nil {
			return
		}
		if err := s.state.Auth.Check(string(user), string(pass)); err != nil {
			s.replyReject(conn, 0x01)
			return
		}
		if _, err := conn.Write([]byte{0x01, 0x00}); err != nil {
			return
		}
	} else {
		if !containsMethod(methods, 0x00) {
			s.replyReject(conn, 0xFF)
			return
		}
		if _, err := conn.Write([]byte{0x05, 0x00}); err != nil {
			return
		}
	}

	// request
	req := make([]byte, 4)
	if _, err := io.ReadFull(br, req); err != nil {
		return
	}
	if req[0] != 0x05 {
		return
	}
	if req[1] != 0x01 {
		s.replyReject(conn, 0x07) // only CONNECT
		return
	}
	host, port, err := readSocksAddr(br, req[3])
	if err != nil {
		s.replyReject(conn, 0x04)
		return
	}

	sticky := pool.StickyKey(peer)
	upstream, backendLease, err := DialViaSocks(ctx, s.state, host, port, sticky)
	if err != nil {
		s.log.Info("dial failed",
			zap.String("peer", peer.String()),
			zap.String("target", net.JoinHostPort(host, fmt.Sprint(port))),
			zap.Error(err))
		s.replyReject(conn, 0x01)
		return
	}
	defer backendLease.Release()
	defer upstream.Close()

	// reply success
	conn.Write([]byte{0x05, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})

	// bidirectional copy
	_, _ = CopyBidirectional(conn, upstream, s.state.Cfg.Limits.IOTimeout.Duration, false, 0)
}

func containsMethod(methods []byte, m byte) bool {
	for _, v := range methods {
		if v == m {
			return true
		}
	}
	return false
}

func (s *Socks5Server) replyReject(conn net.Conn, rep byte) {
	conn.Write([]byte{0x05, rep, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})
}
