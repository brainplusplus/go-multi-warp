package proxy

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/autoclaw/go-multi-warp/internal/pool"
	"go.uber.org/zap"
)

// Integration test: verify that concurrent HTTP clients never receive
// each other's responses (no cross-talk through the proxy data plane).

// fakeBackendSOCKS5 is a minimal SOCKS5 server that echoes the target back
// so the test can verify the proxy tunnels correctly.
func fakeBackendSOCKS5(t *testing.T) (addr string, shutdown func()) {
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
			go handleSOCKS(conn)
		}
	}()
	return ln.Addr().String(), func() { ln.Close() }
}

func handleSOCKS(conn net.Conn) {
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
	// host+port (assume IPv4 for simplicity in test)
	host := make([]byte, 4)
	io.ReadFull(br, host)
	port := make([]byte, 2)
	io.ReadFull(br, port)

	// reply success
	conn.Write([]byte{0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0})

	// Echo the HTTP request target back so the test can verify it.
	// Read the HTTP request line.
	line, _ := br.ReadString('\n')
	// Write a simple HTTP response embedding the request line.
	body := fmt.Sprintf("response-for:%s", strings.TrimSpace(line))
	resp := fmt.Sprintf("HTTP/1.1 200 OK\r\nContent-Length: %d\r\nConnection: close\r\n\r\n%s", len(body), body)
	conn.Write([]byte(resp))
}

func TestHTTPProxy_ConcurrentNoCrossTalk(t *testing.T) {
	backendAddr, shutdown := fakeSOCKS5Backend(t)
	defer shutdown()

	cfg := buildTestConfig(backendAddr)
	p := pool.New(cfg)
	// Mark backend as healthy AND admitted to pool.
	p.Get(1).SetAdmit(pool.AdmitInPool, "")
	p.ForceState(1, pool.StateHealthy)
	state := NewState(cfg, p)
	server := NewHTTPProxyServer(state, zap.NewNop())

	// Start HTTP proxy on a real TCP listener.
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

			targetHost := fmt.Sprintf("client-%d.example.com", i)

			// Dial the proxy.
			conn, err := net.DialTimeout("tcp", proxyAddr, 2*time.Second)
			if err != nil {
				t.Errorf("client %d: dial proxy: %v", i, err)
				return
			}
			defer conn.Close()

			// Write CONNECT request (tunnel mode — backend is a SOCKS5 server).
			rawReq := fmt.Sprintf("CONNECT %s:443 HTTP/1.1\r\nHost: %s:443\r\n\r\n",
				targetHost, targetHost)
			conn.Write([]byte(rawReq))

			// Read CONNECT response.
			resp := make([]byte, 4096)
			n, err := conn.Read(resp)
			if err != nil {
				t.Errorf("client %d: read CONNECT response: %v", i, err)
				return
			}
			respStr := string(resp[:n])
			if !strings.Contains(respStr, "200") {
				t.Errorf("client %d: CONNECT failed: %q", i, respStr)
				return
			}

			// After CONNECT, the proxy is in tunnel mode. The backend echoes
			// a marker wrapped in HTTP — read it directly.
			marker := make([]byte, 256)
			n, err = conn.Read(marker)
			if err != nil && err != io.EOF {
				t.Errorf("client %d: read marker: %v", i, err)
				return
			}
			markerStr := string(marker[:n])
			expected := fmt.Sprintf("backend-ok:%s:443", targetHost)
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
