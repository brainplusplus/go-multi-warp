package control

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/autoclaw/go-multi-warp/internal/config"
	"github.com/autoclaw/go-multi-warp/internal/pool"
	"go.uber.org/zap"
	"golang.org/x/net/proxy"
)

// UniquenessEngine progressively expands the selectable pool:
//   - first healthy backend is admitted immediately (serve ASAP)
//   - later backends try unique IPv4 vs current pool, with bounded re-reg attempts
//   - safer re-reg: stagger + concurrent cap + exponential backoff
//   - post-admit recheck de-dupes collisions in the background without dropping capacity
//   - when instances > MaxInstances, uniqueness is skipped (admit on health only)
//
// This is best-effort. Cloudflare free WARP often shares a small IPv4 egress pool.
type UniquenessEngine struct {
	cfg  *config.Config
	pool *pool.Pool
	ctl  *WarpController // may be nil in attach mode
	log  *zap.Logger

	mu      sync.Mutex // serializes uniqueness admits
	started sync.Once
	firstMu sync.Mutex
	firstOK bool

	// Safer re-reg gates
	regSem       chan struct{}
	lastRegStart time.Time
	nextRetry    map[int]time.Time // id -> earliest next attempt
	recheckN     map[int]int       // id -> post-admit recheck re-regs done
	lastRecheck  time.Time

	// Metrics (atomic for admin)
	stats UniqueStats
}

// UniqueStats is exposed on /metrics for honest operator visibility.
type UniqueStats struct {
	Collisions       atomic.Uint64
	ReregAttempts    atomic.Uint64
	ReregSuccess     atomic.Uint64
	ReregFail        atomic.Uint64
	AdmittedUnique   atomic.Uint64
	AdmittedResidual atomic.Uint64
	RecheckRuns      atomic.Uint64
	RecheckCollisions atomic.Uint64
	CeilingHit       atomic.Uint64
}

func NewUniquenessEngine(cfg *config.Config, p *pool.Pool, ctl *WarpController, log *zap.Logger) *UniquenessEngine {
	capN := cfg.Uniqueness.MaxConcurrentReg
	if capN <= 0 {
		capN = 2
	}
	return &UniquenessEngine{
		cfg:       cfg,
		pool:      p,
		ctl:       ctl,
		log:       log,
		regSem:    make(chan struct{}, capN),
		nextRetry: make(map[int]time.Time),
		recheckN:  make(map[int]int),
	}
}

func (e *UniquenessEngine) Stats() *UniqueStats { return &e.stats }

func (e *UniquenessEngine) SnapshotStats() map[string]any {
	inPool, _, _, uniqueIPs := e.pool.AdmitStats()
	ceiling := uniqueIPs > 0 && inPool >= e.cfg.Instances && uniqueIPs < e.cfg.Instances
	hist := e.egressHistogram()
	regCap := regCapacity(e.regSem)
	return map[string]any{
		"collisions":         e.stats.Collisions.Load(),
		"rereg_attempts":     e.stats.ReregAttempts.Load(),
		"rereg_success":      e.stats.ReregSuccess.Load(),
		"rereg_fail":         e.stats.ReregFail.Load(),
		"admitted_unique":    e.stats.AdmittedUnique.Load(),
		"admitted_residual":  e.stats.AdmittedResidual.Load(),
		"recheck_runs":       e.stats.RecheckRuns.Load(),
		"recheck_collisions": e.stats.RecheckCollisions.Load(),
		"ceiling_hit":        e.stats.CeilingHit.Load() > 0 || ceiling,
		"unique_ipv4":        uniqueIPs,
		"in_pool":            inPool,
		"egress_histogram":   hist,
		"max_concurrent_reg": regCap,
		"recheck_every_ms":   e.recheckEvery().Milliseconds(),
	}
}

// regCapacity returns the buffered size of the re-reg semaphore (no shadowing builtin).
func regCapacity(ch chan struct{}) int {
	if ch == nil {
		return 0
	}
	return cap(ch)
}

func (e *UniquenessEngine) egressHistogram() map[string]int {
	out := make(map[string]int)
	for _, b := range e.pool.Backends() {
		if b.AdmitPhase() != pool.AdmitInPool {
			continue
		}
		ip := b.EgressIPv4()
		if ip == "" {
			continue
		}
		out[ip]++
	}
	return out
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
			zap.Int("max_concurrent_reg", regCapacity(e.regSem)),
			zap.Duration("stagger", e.stagger()),
			zap.Duration("backoff_base", e.backoffBase()),
			zap.Duration("recheck_every", e.recheckEvery()),
			zap.Int("recheck_max", e.recheckMax()),
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
			e.recheckInPool(ctx)
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

func (e *UniquenessEngine) backoffBase() time.Duration {
	d := e.cfg.Uniqueness.RetryBackoff.Duration
	if d <= 0 {
		return 8 * time.Second
	}
	return d
}

// attemptBackoff grows with attempt number: base * 2^(attempt-1), capped at 60s.
func (e *UniquenessEngine) attemptBackoff(attempt int) time.Duration {
	base := e.backoffBase()
	if attempt < 1 {
		attempt = 1
	}
	// shift safely
	shift := attempt - 1
	if shift > 3 {
		shift = 3 // max 8x base
	}
	d := base << shift
	if d > 60*time.Second {
		d = 60 * time.Second
	}
	return d
}

func (e *UniquenessEngine) stagger() time.Duration {
	d := e.cfg.Uniqueness.Stagger.Duration
	if d < 0 {
		return 0
	}
	if d == 0 {
		return 3 * time.Second
	}
	return d
}

func (e *UniquenessEngine) recheckEvery() time.Duration {
	d := e.cfg.Uniqueness.RecheckEvery.Duration
	if d <= 0 {
		return 60 * time.Second
	}
	return d
}

func (e *UniquenessEngine) recheckMax() int {
	n := e.cfg.Uniqueness.RecheckMax
	if n < 0 {
		return 0
	}
	return n
}

func (e *UniquenessEngine) tick(ctx context.Context) {
	// Serialize to avoid two slots claiming the same new IP concurrently.
	e.mu.Lock()
	defer e.mu.Unlock()

	effort := e.cfg.UniqueEffortActive()
	now := time.Now()

	// Stable order: lower IDs first so #0 prefers first-admit.
	backends := e.pool.Backends()
	sort.SliceStable(backends, func(i, j int) bool { return backends[i].ID < backends[j].ID })

	for _, b := range backends {
		if ctx.Err() != nil {
			return
		}
		phase := b.AdmitPhase()
		if phase == pool.AdmitInPool || phase == pool.AdmitParked {
			continue
		}

		// Per-backend retry gate (backoff after failed/duplicate re-reg).
		if t, ok := e.nextRetry[b.ID]; ok && now.Before(t) {
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
			e.stats.AdmittedUnique.Add(1)
			e.log.Info("first backend admitted (serve ASAP)",
				zap.Int("id", b.ID),
				zap.String("egress_ipv4", ip),
				zap.String("addr", b.Addr))
			continue
		}

		// Uniqueness disabled or instances > cap: admit on health only.
		if !effort {
			e.pool.Admit(b.ID, ip)
			e.stats.AdmittedResidual.Add(1)
			e.log.Info("backend admitted (uniqueness effort off)",
				zap.Int("id", b.ID), zap.String("egress_ipv4", ip))
			continue
		}

		used := e.pool.InPoolIPv4Set()
		if owner, ok := used[ip]; !ok || owner == b.ID {
			e.pool.Admit(b.ID, ip)
			e.stats.AdmittedUnique.Add(1)
			delete(e.nextRetry, b.ID)
			e.log.Info("backend admitted with unique IPv4",
				zap.Int("id", b.ID),
				zap.String("egress_ipv4", ip),
				zap.Int("unique_ipv4_pool", len(e.pool.InPoolIPv4Set())))
			continue
		}

		// Duplicate IPv4 vs existing pool.
		e.stats.Collisions.Add(1)
		tries := b.IncUniqueAttempt()
		max := e.maxAttempts()
		e.log.Info("duplicate egress IPv4; attempting reregister",
			zap.Int("id", b.ID),
			zap.String("egress_ipv4", ip),
			zap.Int("attempt", tries),
			zap.Int("max_attempts", max),
			zap.Int("collision_with", used[ip]))

		if tries > max {
			e.stats.CeilingHit.Add(1)
			if e.cfg.Uniqueness.Strict {
				e.pool.Park(b.ID, ip)
				e.log.Warn("unique attempts exhausted; parked (strict)",
					zap.Int("id", b.ID), zap.String("egress_ipv4", ip))
			} else {
				// Best-effort: still admit for concurrency.
				e.pool.Admit(b.ID, ip)
				e.stats.AdmittedResidual.Add(1)
				e.log.Warn("unique attempts exhausted; admitted non-unique",
					zap.Int("id", b.ID), zap.String("egress_ipv4", ip),
					zap.Int("unique_ipv4_pool", len(e.pool.InPoolIPv4Set())))
			}
			delete(e.nextRetry, b.ID)
			continue
		}

		if e.ctl == nil {
			// Attach mode cannot reregister WARP devices.
			if e.cfg.Uniqueness.Strict {
				e.pool.Park(b.ID, ip)
			} else {
				e.pool.Admit(b.ID, ip)
				e.stats.AdmittedResidual.Add(1)
			}
			continue
		}

		if !e.tryReregister(ctx, b.ID, tries) {
			// Cap full or stagger wait — retry later with short gate.
			e.nextRetry[b.ID] = time.Now().Add(e.stagger())
			continue
		}
		// Schedule exponential backoff before next probe of this backend.
		e.nextRetry[b.ID] = time.Now().Add(e.attemptBackoff(tries))
	}
}

// tryReregister acquires concurrent cap + enforces stagger. Returns false if deferred.
func (e *UniquenessEngine) tryReregister(ctx context.Context, id, attempt int) bool {
	// Stagger: min gap since last re-reg start.
	gap := e.stagger()
	if gap > 0 && !e.lastRegStart.IsZero() {
		wait := gap - time.Since(e.lastRegStart)
		if wait > 0 {
			e.log.Debug("reregister stagger wait",
				zap.Int("id", id), zap.Duration("wait", wait))
			return false
		}
	}

	select {
	case e.regSem <- struct{}{}:
		// acquired
	default:
		e.log.Debug("reregister concurrent cap full", zap.Int("id", id), zap.Int("cap", regCapacity(e.regSem)))
		return false
	}
	e.lastRegStart = time.Now()
	e.stats.ReregAttempts.Add(1)

	// Run re-reg outside the holding of nothing else critical; we still hold e.mu
	// which is intentional: one re-reg decision at a time for admit correctness.
	err := e.ctl.Reregister(id)
	<-e.regSem

	if err != nil {
		e.stats.ReregFail.Add(1)
		e.log.Warn("reregister failed", zap.Int("id", id), zap.Int("attempt", attempt), zap.Error(err))
		return true // consumed an attempt slot; backoff will apply
	}
	e.stats.ReregSuccess.Add(1)
	e.pool.ForceState(id, pool.StateUnknown)
	e.pool.SetWarming(id)
	e.log.Info("reregister scheduled complete",
		zap.Int("id", id),
		zap.Int("attempt", attempt),
		zap.Duration("next_backoff", e.attemptBackoff(attempt)))
	return true
}

// recheckInPool probes admitted backends and re-regs the higher-ID duplicate owner.
// Never drops the whole pool: residual keep serving; only opportunistic re-diversify.
func (e *UniquenessEngine) recheckInPool(ctx context.Context) {
	if !e.cfg.UniqueEffortActive() || e.ctl == nil {
		return
	}
	every := e.recheckEvery()
	if every <= 0 {
		return
	}
	if !e.lastRecheck.IsZero() && time.Since(e.lastRecheck) < every {
		return
	}
	e.lastRecheck = time.Now()
	e.stats.RecheckRuns.Add(1)

	e.mu.Lock()
	defer e.mu.Unlock()

	type owner struct {
		id int
		ip string
	}
	// Refresh live IPv4 for in-pool backends (best-effort, skip failures).
	seen := make(map[string]int) // ip -> first owner id
	var colliders []owner

	backends := e.pool.Backends()
	sort.SliceStable(backends, func(i, j int) bool { return backends[i].ID < backends[j].ID })

	for _, b := range backends {
		if ctx.Err() != nil {
			return
		}
		if b.AdmitPhase() != pool.AdmitInPool {
			continue
		}
		st := b.State()
		if st != pool.StateHealthy && st != pool.StateDegraded {
			continue
		}
		ip, err := probeEgressIPv4(ctx, b.Addr, e.probeURL())
		if err != nil {
			continue
		}
		// Keep tag fresh even if unique.
		e.pool.Admit(b.ID, ip)

		if first, ok := seen[ip]; ok && first != b.ID {
			e.stats.RecheckCollisions.Add(1)
			colliders = append(colliders, owner{id: b.ID, ip: ip})
			e.log.Info("post-admit collision detected",
				zap.Int("id", b.ID),
				zap.String("egress_ipv4", ip),
				zap.Int("kept_owner", first))
		} else {
			seen[ip] = b.ID
		}
	}

	maxPost := e.recheckMax()
	for _, c := range colliders {
		if maxPost <= 0 {
			break
		}
		n := e.recheckN[c.id]
		if n >= maxPost {
			e.log.Debug("post-admit recheck budget exhausted",
				zap.Int("id", c.id), zap.Int("done", n), zap.Int("max", maxPost))
			continue
		}
		// Per-backend retry gate
		if t, ok := e.nextRetry[c.id]; ok && time.Now().Before(t) {
			continue
		}
		if !e.tryReregister(ctx, c.id, n+1) {
			e.nextRetry[c.id] = time.Now().Add(e.stagger())
			continue
		}
		e.recheckN[c.id] = n + 1
		e.nextRetry[c.id] = time.Now().Add(e.attemptBackoff(n + 1))
		// Only one post-admit re-reg per recheck pass to stay gentle.
		break
	}

	// Mark ceiling when pool is full but unique count stagnated below instances.
	inPool, _, _, uniqueIPs := e.pool.AdmitStats()
	if inPool > 0 && uniqueIPs > 0 && inPool >= min(e.cfg.Instances, len(e.pool.Backends())) && uniqueIPs < inPool {
		e.stats.CeilingHit.Add(1)
		e.log.Debug("unique ipv4 ceiling observed",
			zap.Int("in_pool", inPool),
			zap.Int("unique_ipv4", uniqueIPs),
			zap.Any("histogram", e.egressHistogram()))
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
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
	if v4 := parsed.To4(); v4 != nil {
		return v4.String(), nil
	}
	return parsed.String(), nil
}
