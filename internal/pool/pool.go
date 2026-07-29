package pool

import (
	"errors"
	"hash/fnv"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/autoclaw/go-multi-warp/internal/config"
)

var (
	ErrNoHealthyBackend = errors.New("no healthy WARP backend available")
	ErrGlobalConnLimit  = errors.New("global connection limit reached")
	ErrPerIPConnLimit   = errors.New("per-ip connection limit reached")
	ErrPerIPRPSLimit    = errors.New("per-ip rate limit exceeded")
)

type Metrics struct {
	Selects     atomic.Uint64
	SelectFail  atomic.Uint64
	Success     atomic.Uint64
	Failure     atomic.Uint64
}

type Lease struct {
	backend *Backend
	pool    *Pool
}

func (l *Lease) Backend() *Backend { return l.backend }
func (l *Lease) Addr() string      { return l.backend.Addr }
func (l *Lease) ID() int           { return l.backend.ID }

func (l *Lease) MarkSuccess() { l.backend.MarkSuccess() }
func (l *Lease) MarkFailure(reason string) {
	l.pool.noteFailure(l.backend, reason)
}

// Release decrements inflight counter. Called automatically when lease is done.
func (l *Lease) Release() {
	if l != nil && l.backend != nil {
		l.backend.Release()
	}
}

type Pool struct {
	mu       sync.RWMutex
	backends []*Backend
	index    map[int]*Backend

	strategy  config.PoolStrategy
	stickyTTL time.Duration
	sticky    sync.Map // key -> *stickyEntry
	rr        atomic.Uint64
	metrics   *Metrics
}

type stickyEntry struct {
	id      int
	updated time.Time
}

func New(cfg *config.Config) *Pool {
	p := &Pool{
		strategy:  cfg.Pool.Strategy,
		stickyTTL: cfg.Pool.StickyTTL.Duration,
		metrics:   &Metrics{},
		index:     make(map[int]*Backend),
	}
	for _, b := range cfg.BackendAddrs() {
		_, _, err := net.SplitHostPort(b.Addr)
		if err != nil {
			// fallback assume localhost:port missing colon -> skip but log
			continue
		}
		be := NewBackend(
			b.ID, b.Addr,
			cfg.Pool.MaxFails,
			cfg.Pool.FailTimeout.Duration,
			cfg.Pool.OpenAfter.Duration,
			int32(cfg.Pool.MaxInflightPerBE),
		)
		p.backends = append(p.backends, be)
		p.index[b.ID] = be
	}
	return p
}

func (p *Pool) Metrics() *Metrics { return p.metrics }

func (p *Pool) Backends() []*Backend {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make([]*Backend, len(p.backends))
	copy(out, p.backends)
	return out
}

func (p *Pool) Healthy() []*Backend {
	var out []*Backend
	for _, b := range p.Backends() {
		if b.IsSelectable() {
			out = append(out, b)
		}
	}
	return out
}

func (p *Pool) Snapshots() []Snapshot {
	all := p.Backends()
	out := make([]Snapshot, 0, len(all))
	for _, b := range all {
		out = append(out, b.Snapshot())
	}
	return out
}

func (p *Pool) MarkProbeOK(id int, info *ProbeInfo) {
	if b := p.Get(id); b != nil {
		b.MarkProbeOK(info)
		p.metrics.Success.Add(1)
	}
}

func (p *Pool) MarkProbeFail(id int, reason string) {
	if b := p.Get(id); b != nil {
		b.MarkProbeFail(reason)
		p.metrics.Failure.Add(1)
	}
}

func (p *Pool) Get(id int) *Backend {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.index[id]
}

func (p *Pool) noteFailure(b *Backend, reason string) {
	b.MarkFailure(reason)
}

func (p *Pool) ForceState(id int, state State) {
	if b := p.Get(id); b != nil {
		b.ForceState(state)
	}
}

// Select picks a backend by strategy. Returns a lease (must call Release).
func (p *Pool) Select(stickyKey uint64) (*Lease, error) {
	p.metrics.Selects.Add(1)

	if p.strategy == config.StrategySticky {
		if v, ok := p.sticky.Load(stickyKey); ok {
			e := v.(*stickyEntry)
			if time.Since(e.updated) < p.stickyTTL {
				if b := p.Get(e.id); b != nil && b.TryAcquire() {
					return &Lease{backend: b, pool: p}, nil
				}
			}
		}
	}

	var picked *Backend
	switch p.strategy {
	case config.StrategyRoundRobin:
		picked = p.pickRR()
	case config.StrategyLeastConn:
		picked = p.pickLeastConn()
	case config.StrategySticky:
		picked = p.pickSticky(stickyKey)
		if picked == nil {
			picked = p.pickLeastConn()
		}
	default:
		picked = p.pickLeastConn()
	}

	if picked == nil {
		p.metrics.SelectFail.Add(1)
		return nil, ErrNoHealthyBackend
	}

	if p.strategy == config.StrategySticky {
		p.sticky.Store(stickyKey, &stickyEntry{id: picked.ID, updated: time.Now()})
	}
	return &Lease{backend: picked, pool: p}, nil
}

func (p *Pool) pickRR() *Backend {
	healthy := p.Healthy()
	if len(healthy) == 0 {
		return nil
	}
	start := int(p.rr.Add(1)-1) % len(healthy)
	for i := 0; i < len(healthy); i++ {
		b := healthy[(start+i)%len(healthy)]
		if b.TryAcquire() {
			return b
		}
	}
	return nil
}

func (p *Pool) pickLeastConn() *Backend {
	healthy := p.Healthy()
	var best *Backend
	bestLoad := int32(-1)
	for _, b := range healthy {
		load := b.Inflight()
		if load < bestLoad || bestLoad < 0 {
			if b.HasCapacity() {
				bestLoad = load
				best = b
			}
		}
	}
	if best != nil && best.TryAcquire() {
		return best
	}
	// fallback any acquirable
	for _, b := range p.Healthy() {
		if b.TryAcquire() {
			return b
		}
	}
	return nil
}

func (p *Pool) pickSticky(key uint64) *Backend {
	healthy := p.Healthy()
	if len(healthy) == 0 {
		return nil
	}
	idx := int(key) % len(healthy)
	b := healthy[idx]
	if b.TryAcquire() {
		return b
	}
	return nil
}

// HealthyCount returns number of selectable backends.
func (p *Pool) HealthyCount() int {
	count := 0
	for _, b := range p.Backends() {
		if b.IsSelectable() {
			count++
		}
	}
	return count
}

// Admit adds backend to selector pool with optional egress IPv4 tag.
func (p *Pool) Admit(id int, egressIPv4 string) bool {
	b := p.Get(id)
	if b == nil {
		return false
	}
	b.SetAdmit(AdmitInPool, egressIPv4)
	return true
}

// Park keeps backend out of selector (strict uniqueness exhausted).
func (p *Pool) Park(id int, egressIPv4 string) bool {
	b := p.Get(id)
	if b == nil {
		return false
	}
	b.SetAdmit(AdmitParked, egressIPv4)
	return true
}

// SetWarming marks backend as not yet selectable.
func (p *Pool) SetWarming(id int) bool {
	b := p.Get(id)
	if b == nil {
		return false
	}
	b.SetAdmit(AdmitWarming, "")
	return true
}

// InPoolIPv4Set returns current egress IPv4 addresses of admitted backends.
func (p *Pool) InPoolIPv4Set() map[string]int {
	out := make(map[string]int)
	for _, b := range p.Backends() {
		if b.AdmitPhase() != AdmitInPool {
			continue
		}
		ip := b.EgressIPv4()
		if ip == "" {
			continue
		}
		out[ip] = b.ID
	}
	return out
}

// AdmitStats returns progressive pool membership counters.
func (p *Pool) AdmitStats() (inPool, warming, parked int, uniqueIPs int) {
	ips := make(map[string]struct{})
	for _, b := range p.Backends() {
		switch b.AdmitPhase() {
		case AdmitInPool:
			inPool++
			if ip := b.EgressIPv4(); ip != "" {
				ips[ip] = struct{}{}
			}
		case AdmitParked:
			parked++
		default:
			warming++
		}
	}
	return inPool, warming, parked, len(ips)
}

// ConnLimiter tracks global + per-IP counters with acquire/release semantics.
// Also enforces a per-IP request-per-second token bucket when maxRPS > 0.
type ConnLimiter struct {
	global    atomic.Int64
	maxGlobal int64
	maxPerIP  int64
	maxRPS    int64
	perIP     sync.Map // string IP -> *ipState
}

type ipState struct {
	conns  atomic.Int64
	tokens atomic.Int64
	lastAt atomic.Int64 // unixnano
	// tokensLock serialises token-bucket mutation so concurrent goroutines
	// never race on tokens/lastAt. The conn counter stays lock-free.
	mu sync.Mutex
}

func NewConnLimiter(maxGlobal, maxPerIP, maxRPS int) *ConnLimiter {
	return &ConnLimiter{
		maxGlobal: int64(maxGlobal),
		maxPerIP:  int64(maxPerIP),
		maxRPS:    int64(maxRPS),
	}
}

func (cl *ConnLimiter) Acquire(peer net.Addr) (*ConnLease, error) {
	ip := peerKey(peer)
	g := cl.global.Add(1)
	if cl.maxGlobal > 0 && g > cl.maxGlobal {
		cl.global.Add(-1)
		return nil, ErrGlobalConnLimit
	}

	if ip == "" {
		// still count global; skip per-IP tracking
		return &ConnLease{limiter: cl, ip: ""}, nil
	}

	var st *ipState
	if v, ok := cl.perIP.Load(ip); ok {
		st = v.(*ipState)
	} else {
		st = &ipState{}
		actual, _ := cl.perIP.LoadOrStore(ip, st)
		st = actual.(*ipState)
	}
	n := st.conns.Add(1)
	if cl.maxPerIP > 0 && n > cl.maxPerIP {
		st.conns.Add(-1)
		cl.global.Add(-1)
		return nil, ErrPerIPConnLimit
	}

	// RPS token bucket: refill based on elapsed time, cost 1 token per request.
	// Serialised via per-IP mutex — race-free under any concurrency.
	if cl.maxRPS > 0 {
		st.mu.Lock()
		now := time.Now().UnixNano()
		last := st.lastAt.Load()
		elapsed := time.Duration(now - last)
		add := int64(elapsed.Seconds() * float64(cl.maxRPS))
		tokens := st.tokens.Load() + add
		if tokens > cl.maxRPS {
			tokens = cl.maxRPS
		}
		if tokens < 1 {
			st.mu.Unlock()
			st.conns.Add(-1)
			cl.global.Add(-1)
			return nil, ErrPerIPRPSLimit
		}
		st.tokens.Store(tokens - 1)
		st.lastAt.Store(now)
		st.mu.Unlock()
	}

	return &ConnLease{limiter: cl, ip: ip}, nil
}

type ConnLease struct {
	limiter *ConnLimiter
	ip      string
}

func (cl *ConnLease) Release() {
	if cl == nil || cl.limiter == nil {
		return
	}
	cl.limiter.global.Add(-1)
	if cl.ip == "" {
		return
	}
	if v, ok := cl.limiter.perIP.Load(cl.ip); ok {
		v.(*ipState).conns.Add(-1)
	}
}

func peerKey(peer net.Addr) string {
	if peer == nil {
		return ""
	}
	host, _, err := net.SplitHostPort(peer.String())
	if err != nil {
		return peer.String()
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip.String()
	}
	return host
}

// StickyKey derives a stable sticky key from peer IP.
func StickyKey(peer net.Addr) uint64 {
	ip := peerKey(peer)
	if ip == "" {
		return 0
	}
	h := fnv.New64a()
	h.Write([]byte(ip))
	return h.Sum64()
}

func (cl *ConnLimiter) Active() int64 {
	return cl.global.Load()
}
