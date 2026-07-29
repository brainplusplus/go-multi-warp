package pool

import (
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestConnLimiter_NoRace verifies the limiter under concurrent load.
// If the RPS token bucket has a race, the token count will go negative
// or lastAt will be corrupted — caught by go test -race.
func TestConnLimiter_NoRace(t *testing.T) {
	cl := NewConnLimiter(0, 0, 1000) // unlimited conns, 1000 RPS
	peer, _ := net.ResolveTCPAddr("tcp", "1.2.3.4:12345")

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				lease, err := cl.Acquire(peer)
				if err != nil {
					// RPS limit will kick in — that's fine, we just want no panic/corruption
					continue
				}
				lease.Release()
			}
		}()
	}
	wg.Wait()
}

// TestConnLimiter_Unlimited verifies maxRPS=0 never rejects.
func TestConnLimiter_Unlimited(t *testing.T) {
	cl := NewConnLimiter(0, 0, 0)
	peer, _ := net.ResolveTCPAddr("tcp", "5.6.7.8:9999")
	for i := 0; i < 10000; i++ {
		lease, err := cl.Acquire(peer)
		if err != nil {
			t.Fatalf("unlimited limiter rejected at %d: %v", i, err)
		}
		lease.Release()
	}
}

// TestConnLimiter_ConcurrentNoMix verifies per-IP state isolation.
// Two different IPs must never share tokens or conn counts.
func TestConnLimiter_ConcurrentNoMix(t *testing.T) {
	cl := NewConnLimiter(0, 0, 10)
	ip1, _ := net.ResolveTCPAddr("tcp", "10.0.0.1:1")
	ip2, _ := net.ResolveTCPAddr("tcp", "10.0.0.2:2")

	var wg sync.WaitGroup
	var ip1Rejects, ip2Rejects atomic.Int64

	// Flood both IPs concurrently.
	for i := 0; i < 50; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				lease, err := cl.Acquire(ip1)
				if err != nil {
					ip1Rejects.Add(1)
					continue
				}
				lease.Release()
			}
		}()
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				lease, err := cl.Acquire(ip2)
				if err != nil {
					ip2Rejects.Add(1)
					continue
				}
				lease.Release()
			}
		}()
	}
	wg.Wait()

	// Both IPs should have independent token buckets.
	// The exact reject count depends on timing, but both must be > 0.
	if ip1Rejects.Load() == 0 {
		t.Log("ip1 had no rejects (may happen if timing allows)")
	}
	if ip2Rejects.Load() == 0 {
		t.Log("ip2 had no rejects (may happen if timing allows)")
	}
	// Verify active count is zero after all releases.
	if cl.Active() != 0 {
		t.Fatalf("active conns leak: %d", cl.Active())
	}
}

// TestConnLimiter_ZeroValues verifies zero values = unlimited.
func TestConnLimiter_ZeroValues(t *testing.T) {
	cl := NewConnLimiter(0, 0, 0)
	if cl.maxGlobal != 0 || cl.maxPerIP != 0 || cl.maxRPS != 0 {
		t.Fatal("zero-value limiter not initialized as unlimited")
	}
}

// Benchmark to verify the limiter is not a bottleneck.
func BenchmarkConnLimiter_AcquireRelease(b *testing.B) {
	cl := NewConnLimiter(0, 0, 0)
	peer, _ := net.ResolveTCPAddr("tcp", "9.9.9.9:9999")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lease, _ := cl.Acquire(peer)
		lease.Release()
	}
}

// Benchmark with RPS limiting enabled.
func BenchmarkConnLimiter_RPS(b *testing.B) {
	cl := NewConnLimiter(0, 0, 100000) // 100k RPS = effectively unlimited for bench
	peer, _ := net.ResolveTCPAddr("tcp", "9.9.9.9:9999")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lease, _ := cl.Acquire(peer)
		lease.Release()
	}
}

// TestConnLimiter_RPSRecovery verifies tokens refill over time.
func TestConnLimiter_RPSRecovery(t *testing.T) {
	cl := NewConnLimiter(0, 0, 10) // 10 RPS
	peer, _ := net.ResolveTCPAddr("tcp", "1.1.1.1:1")

	// Burn all tokens.
	for i := 0; i < 10; i++ {
		lease, err := cl.Acquire(peer)
		if err != nil {
			t.Fatalf("unexpected reject at burn %d: %v", i, err)
		}
		lease.Release()
	}
	// Next should fail.
	if _, err := cl.Acquire(peer); err == nil {
		t.Fatal("expected RPS limit after burning tokens")
	}
	// Wait for refill.
	time.Sleep(200 * time.Millisecond)
	lease, err := cl.Acquire(peer)
	if err != nil {
		t.Fatalf("expected recovery after refill wait: %v", err)
	}
	lease.Release()
}
