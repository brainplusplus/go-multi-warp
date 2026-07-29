package proxy

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/autoclaw/go-multi-warp/internal/config"
	"github.com/autoclaw/go-multi-warp/internal/pool"
	"go.uber.org/zap"
)

// fakeSOCKS5Backend is a minimal SOCKS5 server that accepts CONNECT and
// immediately echoes back a marker response. Used to test the proxy's
// backend dial + tunnel path without real WARP instances.
func fakeSOCKS5Backend(t *testing.T) (addr string, shutdown func()) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go handleBackendSOCKS(conn)
		}
	}()
	return ln.Addr().String(), func() { ln.Close() }
}

func handleBackendSOCKS(conn net.Conn) {
	defer conn.Close()
	br := bufio.NewReader(conn)
	// greeting
	hdr := make([]byte, 2)
	if _, err := io.ReadFull(br, hdr); err != nil {
		return
	}
	nmethods := int(hdr[1])
	methods := make([]byte, nmethods)
	io.ReadFull(br, methods)
	// accept no-auth
	conn.Write([]byte{0x05, 0x00})

	// request
	req := make([]byte, 4)
	io.ReadFull(br, req)
	// read address
	var host string
	switch req[3] {
	case 0x01:
		b := make([]byte, 4)
		io.ReadFull(br, b)
		host = net.IP(b).String()
	case 0x03:
		lb := make([]byte, 1)
		io.ReadFull(br, lb)
		name := make([]byte, int(lb[0]))
		io.ReadFull(br, name)
		host = string(name)
	case 0x04:
		b := make([]byte, 16)
		io.ReadFull(br, b)
		host = net.IP(b).String()
	}
	port := make([]byte, 2)
	io.ReadFull(br, port)

	// reply success
	conn.Write([]byte{0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0})

	// Echo a marker so the client can verify it got the right response.
	// Wrap in a valid HTTP/1.1 response so the proxy's ReadResponse succeeds.
	body := fmt.Sprintf("backend-ok:%s:%d\n", host, binary.BigEndian.Uint16(port))
	resp := fmt.Sprintf("HTTP/1.1 200 OK\r\nContent-Length: %d\r\nConnection: close\r\n\r\n%s", len(body), body)
	conn.Write([]byte(resp))
}

func buildTestConfig(backendAddr string) *config.Config {
	cfg := config.Default()
	cfg.Backends = []config.Backend{{ID: 1, Addr: backendAddr}}
	cfg.Pool.MaxFails = 1
	cfg.Pool.FailTimeout = config.Duration{time.Second}
	cfg.Pool.OpenAfter = config.Duration{time.Second}
	cfg.Pool.MaxInflightPerBE = 1000
	cfg.Limits.MaxConnGlobal = 10000
	cfg.Limits.MaxConnPerIP = 10000
	cfg.Limits.IOTimeout = config.Duration{5 * time.Second}
	cfg.Streaming.IdleTimeout = config.Duration{5 * time.Second}
	cfg.Control.DrainTimeout = config.Duration{5 * time.Second}
	return &cfg
}

func socks5ConnectRequest(targetHost string, targetPort int) []byte {
	var req []byte
	req = append(req, 0x05, 0x01, 0x00) // greeting: ver=5, 1 method, no-auth
	req = append(req, 0x05, 0x01, 0x00, 0x03) // request: ver=5, cmd=CONNECT, rsv=0, atyp=domain
	req = append(req, byte(len(targetHost)))
	req = append(req, []byte(targetHost)...)
	portBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(portBytes, uint16(targetPort))
	req = append(req, portBytes...)
	return req
}

func socks5ReadResponse(conn net.Conn) error {
	// greeting response
	greetResp := make([]byte, 2)
	if _, err := io.ReadFull(conn, greetResp); err != nil {
		return fmt.Errorf("greeting: %w", err)
	}
	if greetResp[0] != 0x05 || greetResp[1] != 0x00 {
		return fmt.Errorf("greeting rejected: %x", greetResp)
	}
	// connect response
	connResp := make([]byte, 4)
	if _, err := io.ReadFull(conn, connResp); err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	if connResp[1] != 0x00 {
		return fmt.Errorf("connect rejected: %x", connResp[1])
	}
	// drain bind addr
	var drain int
	switch connResp[3] {
	case 0x01:
		drain = 4 + 2
	case 0x03:
		lb := make([]byte, 1)
		io.ReadFull(conn, lb)
		drain = int(lb[0]) + 2
	case 0x04:
		drain = 16 + 2
	}
	if drain > 0 {
		io.ReadFull(conn, make([]byte, drain))
	}
	return nil
}

// TestSocks5_ConcurrentNoCrossTalk verifies that concurrent SOCKS5 clients
// never receive each other's data. Uses a real backend listener so the full
// dial → tunnel → echo path is exercised.
func TestSocks5_ConcurrentNoCrossTalk(t *testing.T) {
	backendAddr, shutdown := fakeSOCKS5Backend(t)
	defer shutdown()

	cfg := buildTestConfig(backendAddr)
	p := pool.New(cfg)
	// Mark backend as healthy AND admitted to pool.
	p.Get(1).SetAdmit(pool.AdmitInPool, "")
	p.ForceState(1, pool.StateHealthy)
	state := NewState(cfg, p)
	server := NewSocks5Server(state, zap.NewNop())

	// Start the SOCKS5 server on a real TCP listener.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go server.handle(context.Background(), conn)
		}
	}()
	proxyAddr := ln.Addr().String()

	const N = 30
	var wg sync.WaitGroup
	var mixed atomic.Int64

	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			targetHost := fmt.Sprintf("socks-client-%d.example.com", i)
			targetPort := 443

			// Dial the proxy.
			conn, err := net.DialTimeout("tcp", proxyAddr, 2*time.Second)
			if err != nil {
				t.Errorf("client %d: dial proxy: %v", i, err)
				return
			}
			defer conn.Close()

			// Write SOCKS5 request.
			conn.Write(socks5ConnectRequest(targetHost, targetPort))

			// Read SOCKS5 response.
			if err := socks5ReadResponse(conn); err != nil {
				t.Errorf("client %d: socks5 handshake: %v", i, err)
				return
			}

			// Read the backend echo marker.
			marker := make([]byte, 256)
			n, err := conn.Read(marker)
			if err != nil && err != io.EOF {
				t.Errorf("client %d: read marker: %v", i, err)
				return
			}
			markerStr := string(marker[:n])
			expected := fmt.Sprintf("backend-ok:%s:%d", targetHost, targetPort)
			if !strings.Contains(markerStr, expected) {
				mixed.Add(1)
				t.Errorf("client %d: wrong marker: %q (expected %q)", i, markerStr, expected)
			}
		}(i)
	}
	wg.Wait()

	if mixed.Load() > 0 {
		t.Fatalf("cross-talk detected: %d clients got wrong responses", mixed.Load())
	}
}

// TestSocks5_AuthRequired verifies auth enforcement when enabled.
func TestSocks5_AuthRequired(t *testing.T) {
	cfg := buildTestConfig("127.0.0.1:1")
	cfg.Auth.Username = "user"
	cfg.Auth.Password = "pass"
	p := pool.New(cfg)
	state := NewState(cfg, p)
	server := NewSocks5Server(state, zap.NewNop())

	// Start server on real listener.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go server.handle(context.Background(), conn)
		}
	}()

	// Client connects and sends no-auth method (0x00).
	conn, err := net.DialTimeout("tcp", ln.Addr().String(), 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	conn.Write([]byte{0x05, 0x01, 0x00})

	resp := make([]byte, 2)
	if _, err := io.ReadFull(conn, resp); err != nil {
		t.Fatalf("read greeting response: %v", err)
	}
	if resp[0] != 0x05 || resp[1] != 0xFF {
		t.Fatalf("expected 0x05 0xFF, got %x %x", resp[0], resp[1])
	}
}

// TestSocks5_OnlyConnect verifies that non-CONNECT commands are rejected.
func TestSocks5_OnlyConnect(t *testing.T) {
	cfg := buildTestConfig("127.0.0.1:1")
	p := pool.New(cfg)
	state := NewState(cfg, p)
	server := NewSocks5Server(state, zap.NewNop())

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go server.handle(context.Background(), conn)
		}
	}()

	conn, err := net.DialTimeout("tcp", ln.Addr().String(), 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	conn.Write([]byte{0x05, 0x01, 0x00}) // greeting
	conn.Write([]byte{0x05, 0x02, 0x00, 0x01, 0, 0, 0, 0, 0, 0}) // BIND

	resp := make([]byte, 2)
	io.ReadFull(conn, resp)
	if resp[0] != 0x05 || resp[1] != 0x00 {
		t.Fatalf("expected greeting accept, got %x", resp)
	}
	reject := make([]byte, 10)
	io.ReadFull(conn, reject)
	if reject[1] != 0x07 {
		t.Fatalf("expected 0x07 (command not supported), got %x", reject[1])
	}
}

// TestSocks5_GracefulDrain verifies that shutdown waits for active connections.
func TestSocks5_GracefulDrain(t *testing.T) {
	backendAddr, shutdown := fakeSOCKS5Backend(t)
	defer shutdown()

	cfg := buildTestConfig(backendAddr)
	cfg.Control.DrainTimeout = config.Duration{2 * time.Second}
	p := pool.New(cfg)
	p.ForceState(1, pool.StateHealthy)
	state := NewState(cfg, p)
	server := NewSocks5Server(state, zap.NewNop())

	// Start a connection that will stay open.
	pr, pw := net.Pipe()
	defer pr.Close()
	defer pw.Close()

	server.activeConns.Add(1)
	done := make(chan struct{})
	go func() {
		server.handle(context.Background(), pr)
		server.activeConns.Add(-1)
		close(done)
	}()

	// Wait a bit for the connection to be active.
	time.Sleep(50 * time.Millisecond)

	// Trigger drain in background.
	drainDone := make(chan struct{})
	go func() {
		server.drain(context.Background())
		close(drainDone)
	}()

	// Drain should block because connection is active.
	select {
	case <-drainDone:
		t.Fatal("drain should not complete while connection is active")
	case <-time.After(200 * time.Millisecond):
		// expected
	}

	// Close the connection — drain should now complete.
	pw.Close()
	pr.Close()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not exit after conn close")
	}

	select {
	case <-drainDone:
	case <-time.After(3 * time.Second):
		t.Fatal("drain did not complete after conn close")
	}
}
