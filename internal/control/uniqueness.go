package control

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/autoclaw/go-multi-warp/internal/config"
	"github.com/autoclaw/go-multi-warp/internal/pool"
	"go.uber.org/zap"
	"golang.org/x/net/proxy"
)

// UniquenessEngine progressively expands the selectable pool:
//   - first healthy backend is admitted immediately (serve ASAP)
//   - later backends try unique IPv4 vs current pool, with bounded re-reg attempts
//   - when instances > MaxInstances, uniqueness is skipped (admit on health only)
//
// This is best-effort. Cloudflare free WARP often shares a small IPv4 egress pool.
type UniquenessEngine struct {
	cfg  *config.Config
	pool *pool.Pool
	ctl  *WarpController // may be nil in attach mode
	log  *zap.Logger

	mu       sync.Mutex // serializes uniqueness admits + re-reg
	started  sync.Once
	firstMu  sync.Mutex
	firstOK  bool
}

func NewUniquenessEngine(cfg *config.Config, p *pool.Pool, ctl *WarpController, log *zap.Logger) *UniquenessEngine {
	return &UniquenessEngine{cfg: cfg, pool: p, ctl: ctl, log: log}
}

func (e *UniquenessEngine) Run(ctx context.Context) {
	e.started.Do(func() {
		effort := e.cfg.UniqueEffortActive()
		e.log.Info("uniqueness engine started",
			zap.Bool("enabled", e.cfg.Uniqueness.Enabled),
			zap.Bool("effort_active", effort),
			zap.Int("instances", e.cfg.Instances),
			zap.Int("max_instances", e.cfg.Uniqueness.MaxInstances),
			zap.Int("max_attempts", e.cfg.Uniqueness.MaxAttempts),
			zap.Bool("strict", e.cfg.Uniqueness.Strict),
			zap.String("probe_url", e.probeURL()),
		)
	})

	// Fast path: poll quickly until first admit so proxy becomes usable ASAP.
	fast := time.NewTicker(2 * time.Second)
	defer fast.Stop()
	slow := time.NewTicker(5 * time.Second)
	defer slow.Stop()

	for {
		select {
		case <-ctx.Done():
			e.log.Info("uniqueness engine shutting down")
			return
		case <-fast.C:
			e.tick(ctx)
			if e.hasFirst() {
				fast.Stop()
			}
		case <-slow.C:
			e.tick(ctx)
		}
	}
}

func (e *UniquenessEngine) hasFirst() bool {
	e.firstMu.Lock()
	defer e.firstMu.Unlock()
	return e.firstOK
}

func (e *UniquenessEngine) markFirst() {
	e.firstMu.Lock()
	e.firstOK = true
	e.firstMu.Unlock()
}

func (e *UniquenessEngine) probeURL() string {
	u := strings.TrimSpace(e.cfg.Uniqueness.ProbeURL)
	if u == "" {
		return "https://api.ipify.org"
	}
	return u
}

func (e *UniquenessEngine) maxAttempts() int {
	n := e.cfg.Uniqueness.MaxAttempts
	if n <= 0 {
		return 8
	}
	return n
}

func (e *UniquenessEngine) backoff() time.Duration {
	d := e.cfg.Uniqueness.RetryBackoff.Duration
	if d <= 0 {
		return 8 * time.Second
	}
	return d
}

func (e *UniquenessEngine) tick(ctx context.Context) {
	// Serialize to avoid two slots claiming the same new IP concurrently.
	e.mu.Lock()
	defer e.mu.Unlock()

	effort := e.cfg.UniqueEffortActive()
	for _, b := range e.pool.Backends() {
		if ctx.Err() != nil {
			return
		}
		phase := b.AdmitPhase()
		if phase == pool.AdmitInPool || phase == pool.AdmitParked {
			continue
		}

		// Must be health-connected enough to probe via SOCKS.
		st := b.State()
		if st != pool.StateHealthy && st != pool.StateDegraded && st != pool.StateUnknown {
			continue
		}

		ip, err := probeEgressIPv4(ctx, b.Addr, e.probeURL())
		if err != nil {
			e.log.Debug("unique probe skip (not ready)",
				zap.Int("id", b.ID), zap.Error(err))
			continue
		}

		// First healthy backend: admit immediately, no uniqueness gate.
		if !e.hasFirst() {
			e.pool.Admit(b.ID, ip)
			e.markFirst()
			e.log.Info("first backend admitted (serve ASAP)",
				zap.Int("id", b.ID),
				zap.String("egress_ipv4", ip),
				zap.String("addr", b.Addr))
			continue
		}

		// Uniqueness disabled or instances > cap: admit on health only.
		if !effort {
			e.pool.Admit(b.ID, ip)
			e.log.Info("backend admitted (uniqueness effort off)",
				zap.Int("id", b.ID), zap.String("egress_ipv4", ip))
			continue
		}

		used := e.pool.InPoolIPv4Set()
		if owner, ok := used[ip]; !ok || owner == b.ID {
			e.pool.Admit(b.ID, ip)
			e.log.Info("backend admitted with unique IPv4",
				zap.Int("id", b.ID),
				zap.String("egress_ipv4", ip),
				zap.Int("unique_ipv4_pool", len(e.pool.InPoolIPv4Set())))
			continue
		}

		// Duplicate IPv4 vs existing pool.
		tries := b.IncUniqueAttempt()
		max := e.maxAttempts()
		e.log.Info("duplicate egress IPv4; attempting reregister",
			zap.Int("id", b.ID),
			zap.String("egress_ipv4", ip),
			zap.Int("attempt", tries),
			zap.Int("max_attempts", max),
			zap.Int("collision_with", used[ip]))

		if tries > max {
			if e.cfg.Uniqueness.Strict {
				e.pool.Park(b.ID, ip)
				e.log.Warn("unique attempts exhausted; parked (strict)",
					zap.Int("id", b.ID), zap.String("egress_ipv4", ip))
			} else {
				// Best-effort: still admit for concurrency.
				e.pool.Admit(b.ID, ip)
				e.log.Warn("unique attempts exhausted; admitted non-unique",
					zap.Int("id", b.ID), zap.String("egress_ipv4", ip))
			}
			continue
		}

		if e.ctl == nil {
			// Attach mode cannot reregister WARP devices.
			if e.cfg.Uniqueness.Strict {
				e.pool.Park(b.ID, ip)
			} else {
				e.pool.Admit(b.ID, ip)
			}
			continue
		}

		if err := e.ctl.Reregister(b.ID); err != nil {
			e.log.Warn("reregister failed", zap.Int("id", b.ID), zap.Error(err))
		} else {
			e.pool.ForceState(b.ID, pool.StateUnknown)
			e.pool.SetWarming(b.ID)
		}
		// Backoff before next probe of this / others to ease CF API pressure.
		select {
		case <-ctx.Done():
			return
		case <-time.After(e.backoff()):
		}
	}
}

func probeEgressIPv4(ctx context.Context, socksAddr, rawURL string) (string, error) {
	dialer, err := proxy.SOCKS5("tcp", socksAddr, nil, &net.Dialer{Timeout: 5 * time.Second})
	if err != nil {
		return "", fmt.Errorf("socks dialer: %w", err)
	}
	transport := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return dialer.Dial(network, addr)
		},
		TLSHandshakeTimeout: 5 * time.Second,
		// Prefer IPv4 for uniqueness signal.
		// (Dialer above is SOCKS; CF exit may still be dual-stack; ipify returns IPv4 by default.)
	}
	client := &http.Client{Transport: transport, Timeout: 10 * time.Second}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return "", err
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 128))
	if err != nil {
		return "", err
	}
	ip := strings.TrimSpace(string(body))
	// ipify plain text; also accept JSON {"ip":"..."} lightly
	if strings.HasPrefix(ip, "{") {
		if i := strings.Index(ip, `"ip"`); i >= 0 {
			rest := ip[i+4:]
			if j := strings.Index(rest, `"`); j >= 0 {
				rest = rest[j+1:]
				if k := strings.Index(rest, `"`); k >= 0 {
					ip = rest[:k]
				}
			}
		}
	}
	parsed := net.ParseIP(ip)
	if parsed == nil {
		return "", fmt.Errorf("bad ip body %q", ip)
	}
	// Prefer IPv4 form for set membership.
	if v4 := parsed.To4(); v4 != nil {
		return v4.String(), nil
	}
	return parsed.String(), nil
}
