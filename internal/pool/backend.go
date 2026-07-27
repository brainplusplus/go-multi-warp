package pool

import (
	"sync"
	"sync/atomic"
	"time"
)

type State int

const (
	StateUnknown State = iota
	StateHealthy
	StateDegraded
	StateUnhealthy
	StateCircuitOpen
)

func (s State) String() string {
	switch s {
	case StateHealthy:
		return "healthy"
	case StateDegraded:
		return "degraded"
	case StateUnhealthy:
		return "unhealthy"
	case StateCircuitOpen:
		return "circuit_open"
	default:
		return "unknown"
	}
}

type ProbeInfo struct {
	Warp      string `json:"warp"`
	IP        string `json:"ip"`
	Loc       string `json:"loc"`
	LatencyMS int64  `json:"latency_ms"`
}

type Snapshot struct {
	ID             int        `json:"id"`
	Addr           string     `json:"addr"`
	State          string     `json:"state"`
	Fails          int        `json:"fails"`
	Inflight       int32      `json:"inflight"`
	SuccessTotal   uint64     `json:"success_total"`
	FailTotal      uint64     `json:"fail_total"`
	LastProbe      *ProbeInfo `json:"last_probe,omitempty"`
	LastError      string     `json:"last_error,omitempty"`
	LastChangeAgo  int64      `json:"last_change_ms_ago"`
}

type innerState struct {
	mu        sync.RWMutex
	state     State
	fails     int
	lastError string
	lastProbe *ProbeInfo
	lastChange time.Time
	openedAt   *time.Time
}

type Backend struct {
	ID           int
	Addr         string
	MaxFails     int
	FailTimeout  time.Duration
	OpenAfter    time.Duration
	MaxInflight  int32
	inflight     atomic.Int32
	successTotal atomic.Uint64
	failTotal    atomic.Uint64
	inner        innerState
}

func NewBackend(id int, addr string, maxFails int, failTimeout, openAfter time.Duration, maxInflight int32) *Backend {
	if maxFails < 1 {
		maxFails = 1
	}
	if maxInflight < 1 {
		maxInflight = 1
	}
	b := &Backend{
		ID:          id,
		Addr:        addr,
		MaxFails:    maxFails,
		FailTimeout: failTimeout,
		OpenAfter:   openAfter,
		MaxInflight: maxInflight,
	}
	b.inner.state = StateUnknown
	b.inner.lastChange = time.Now()
	return b
}

func (b *Backend) Inflight() int32 { return b.inflight.Load() }

func (b *Backend) HasCapacity() bool { return b.Inflight() < b.MaxInflight }

func (b *Backend) TryAcquire() bool {
	if !b.IsSelectable() {
		return false
	}
	cur := b.inflight.Load()
	for {
		if cur >= b.MaxInflight {
			return false
		}
		if b.inflight.CompareAndSwap(cur, cur+1) {
			return true
		}
		cur = b.inflight.Load()
	}
}

func (b *Backend) Release() { b.inflight.Add(-1) }

func (b *Backend) IsSelectable() bool {
	b.inner.mu.Lock()
	defer b.inner.mu.Unlock()
	switch b.inner.state {
	case StateHealthy, StateDegraded, StateUnknown:
		return true
	case StateUnhealthy:
		if time.Since(b.inner.lastChange) >= b.FailTimeout {
			b.inner.state = StateDegraded
			b.inner.lastChange = time.Now()
			return true
		}
		return false
	case StateCircuitOpen:
		if b.inner.openedAt != nil && time.Since(*b.inner.openedAt) >= b.OpenAfter {
			b.inner.state = StateDegraded
			b.inner.fails = 0
			b.inner.lastChange = time.Now()
			b.inner.openedAt = nil
			return true
		}
		return false
	}
	return false
}

func (b *Backend) MarkSuccess() {
	b.successTotal.Add(1)
	b.inner.mu.Lock()
	defer b.inner.mu.Unlock()
	b.inner.fails = 0
	if b.inner.state != StateHealthy {
		b.inner.state = StateHealthy
		b.inner.lastChange = time.Now()
	}
	b.inner.lastError = ""
	b.inner.openedAt = nil
}

func (b *Backend) markFailureLocked(reason string) {
	b.failTotal.Add(1)
	b.inner.fails++
	b.inner.lastError = reason
	b.inner.lastChange = time.Now()
	if b.inner.fails >= b.MaxFails {
		b.inner.state = StateCircuitOpen
		now := time.Now()
		b.inner.openedAt = &now
	} else if b.inner.state == StateHealthy || b.inner.state == StateUnknown {
		b.inner.state = StateDegraded
	}
}

func (b *Backend) MarkFailure(reason string) {
	b.inner.mu.Lock()
	defer b.inner.mu.Unlock()
	b.markFailureLocked(reason)
}

func (b *Backend) MarkProbeOK(info *ProbeInfo) {
	b.successTotal.Add(1)
	b.inner.mu.Lock()
	defer b.inner.mu.Unlock()
	b.inner.fails = 0
	b.inner.state = StateHealthy
	b.inner.lastProbe = info
	b.inner.lastError = ""
	b.inner.lastChange = time.Now()
	b.inner.openedAt = nil
}

func (b *Backend) MarkProbeFail(reason string) {
	b.inner.mu.Lock()
	defer b.inner.mu.Unlock()
	b.markFailureLocked(reason)
	if b.inner.state == StateDegraded && b.inner.fails >= b.MaxFails {
		b.inner.state = StateUnhealthy
	}
}

func (b *Backend) ForceState(state State) {
	b.inner.mu.Lock()
	defer b.inner.mu.Unlock()
	b.inner.state = state
	b.inner.lastChange = time.Now()
	if state == StateCircuitOpen {
		now := time.Now()
		b.inner.openedAt = &now
	}
	if state == StateHealthy {
		b.inner.fails = 0
		b.inner.openedAt = nil
	}
}

func (b *Backend) State() State {
	b.inner.mu.RLock()
	defer b.inner.mu.RUnlock()
	return b.inner.state
}

func (b *Backend) Fails() int {
	b.inner.mu.RLock()
	defer b.inner.mu.RUnlock()
	return b.inner.fails
}

func (b *Backend) Snapshot() Snapshot {
	b.inner.mu.RLock()
	defer b.inner.mu.RUnlock()
	var probeCopy *ProbeInfo
	if b.inner.lastProbe != nil {
		c := *b.inner.lastProbe
		probeCopy = &c
	}
	return Snapshot{
		ID:            b.ID,
		Addr:          b.Addr,
		State:         b.inner.state.String(),
		Fails:         b.inner.fails,
		Inflight:      b.inflight.Load(),
		SuccessTotal:  b.successTotal.Load(),
		FailTotal:     b.failTotal.Load(),
		LastProbe:     probeCopy,
		LastError:     b.inner.lastError,
		LastChangeAgo: time.Since(b.inner.lastChange).Milliseconds(),
	}
}
