package proxy

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/autoclaw/go-multi-warp/internal/pool"
)

// DialViaSocks dials target through a selected WARP SOCKS5 backend.
// Returns (conn, lease, error). Lease must be Released when conn closes.
func DialViaSocks(ctx context.Context, st *State, host string, port int, stickyKey uint64) (net.Conn, *pool.Lease, error) {
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		lease, err := st.Pool.Select(stickyKey)
		if err != nil {
			return nil, nil, err
		}
		backend := lease.Addr()

		conn, err := connectSOCKS5(ctx, backend, host, port, st.Cfg.Limits.DialTimeout.Duration)
		if err != nil {
			lease.MarkFailure(err.Error())
			lease.Release()
			lastErr = err
			continue
		}
		lease.MarkSuccess()
		return conn, lease, nil
	}
	return nil, nil, lastErr
}

func connectSOCKS5(ctx context.Context, proxyAddr, host string, port int, timeout time.Duration) (net.Conn, error) {
	var d net.Dialer
	d.Timeout = timeout
	conn, err := d.DialContext(ctx, "tcp", proxyAddr)
	if err != nil {
		return nil, fmt.Errorf("connect backend %s: %w", proxyAddr, err)
	}

	// greeting: ver=5, 1 method, no-auth
	if _, err := conn.Write([]byte{0x05, 0x01, 0x00}); err != nil {
		conn.Close()
		return nil, fmt.Errorf("socks greeting: %w", err)
	}
	resp := make([]byte, 2)
	if _, err := io.ReadFull(conn, resp); err != nil {
		conn.Close()
		return nil, fmt.Errorf("socks method response: %w", err)
	}
	if resp[0] != 0x05 || resp[1] != 0x00 {
		conn.Close()
		return nil, fmt.Errorf("socks method rejected: %02x", resp[1])
	}

	// CONNECT request
	req := bytes.Buffer{}
	req.WriteByte(0x05) // ver
	req.WriteByte(0x01) // CONNECT
	req.WriteByte(0x00) // rsv
	ip := net.ParseIP(host)
	if ip == nil {
		// domain
		if len(host) > 255 {
			conn.Close()
			return nil, fmt.Errorf("hostname too long")
		}
		req.WriteByte(0x03) // domain
		req.WriteByte(byte(len(host)))
		req.WriteString(host)
	} else if ip4 := ip.To4(); ip4 != nil {
		req.WriteByte(0x01) // IPv4
		req.Write(ip4)
	} else {
		req.WriteByte(0x04) // IPv6
		req.Write(ip.To16())
	}
	binary.Write(&req, binary.BigEndian, uint16(port))
	if _, err := conn.Write(req.Bytes()); err != nil {
		conn.Close()
		return nil, fmt.Errorf("socks connect request: %w", err)
	}

	// response header
	hdr := make([]byte, 4)
	if _, err := io.ReadFull(conn, hdr); err != nil {
		conn.Close()
		return nil, fmt.Errorf("socks connect response: %w", err)
	}
	if hdr[0] != 0x05 {
		conn.Close()
		return nil, fmt.Errorf("bad socks ver %d", hdr[0])
	}
	if hdr[1] != 0x00 {
		conn.Close()
		return nil, fmt.Errorf("socks connect status %d", hdr[1])
	}
	// drain bind addr
	var drain int
	switch hdr[3] {
	case 0x01:
		drain = 4 + 2
	case 0x03:
		lb := make([]byte, 1)
		if _, err := io.ReadFull(conn, lb); err != nil {
			conn.Close()
			return nil, fmt.Errorf("socks bind addr len: %w", err)
		}
		drain = int(lb[0]) + 2
	case 0x04:
		drain = 16 + 2
	default:
		conn.Close()
		return nil, fmt.Errorf("unknown atyp %d", hdr[3])
	}
	if drain > 0 {
		if _, err := io.ReadFull(conn, make([]byte, drain)); err != nil {
			conn.Close()
			return nil, fmt.Errorf("socks bind addr: %w", err)
		}
	}
	return conn, nil
}

// CopyBidirectional streams both directions until either side closes or timeout.
func CopyBidirectional(a, b net.Conn, timeout time.Duration) (int64, int64) {
	var wg sync.WaitGroup
	wg.Add(2)
	var upBytes, downBytes int64

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	go func() {
		defer wg.Done()
		n, _ := io.Copy(b, a)
		downBytes = n
		// signal other side to stop reading
		if tcp, ok := b.(*net.TCPConn); ok {
			tcp.CloseWrite()
		}
	}()
	go func() {
		defer wg.Done()
		n, _ := io.Copy(a, b)
		upBytes = n
		if tcp, ok := a.(*net.TCPConn); ok {
			tcp.CloseWrite()
		}
	}()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return upBytes, downBytes
	case <-ctx.Done():
		a.Close()
		b.Close()
		return upBytes, downBytes
	}
}

// readSocksAddr parses a SOCKS address (IPv4, IPv6, or domain).
func readSocksAddr(r *bufio.Reader, atyp byte) (string, int, error) {
	var host string
	var port int
	switch atyp {
	case 0x01:
		b := make([]byte, 4)
		if _, err := io.ReadFull(r, b); err != nil {
			return "", 0, err
		}
		host = net.IP(b).String()
	case 0x03:
		lb := make([]byte, 1)
		if _, err := io.ReadFull(r, lb); err != nil {
			return "", 0, err
		}
		name := make([]byte, int(lb[0]))
		if _, err := io.ReadFull(r, name); err != nil {
			return "", 0, err
		}
		host = string(name)
	case 0x04:
		b := make([]byte, 16)
		if _, err := io.ReadFull(r, b); err != nil {
			return "", 0, err
		}
		host = net.IP(b).String()
	default:
		return "", 0, fmt.Errorf("unknown atyp %d", atyp)
	}
	pb := make([]byte, 2)
	if _, err := io.ReadFull(r, pb); err != nil {
		return "", 0, err
	}
	port = int(binary.BigEndian.Uint16(pb))
	return host, port, nil
}
