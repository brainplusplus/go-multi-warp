package admin

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/autoclaw/go-multi-warp/internal/config"
	"github.com/autoclaw/go-multi-warp/internal/pool"
	"github.com/autoclaw/go-multi-warp/internal/proxy"
	"go.uber.org/zap"
)

type State struct {
	Pool        *pool.Pool
	Proxy       *proxy.State
	Cfg         *config.Config
	Started     time.Time
	Log         *zap.Logger
	UniqueStats func() map[string]any // optional; set by main from uniqueness engine
}

type HealthResponse struct {
	Status          string `json:"status"`
	HealthyBackends int    `json:"healthy_backends"`
	InPool          int    `json:"in_pool"`
	Warming         int    `json:"warming"`
	Parked          int    `json:"parked"`
	UniqueIPv4      int    `json:"unique_ipv4"`
	CeilingHit      bool   `json:"ceiling_hit"`
	TotalBackends   int    `json:"total_backends"`
	ActiveConns     int64  `json:"active_conns"`
	UptimeSec       int64  `json:"uptime_sec"`
}

type MetricsResponse struct {
	UptimeSec       int64           `json:"uptime_sec"`
	HealthyBackends int             `json:"healthy_backends"`
	InPool          int             `json:"in_pool"`
	Warming         int             `json:"warming"`
	Parked          int             `json:"parked"`
	UniqueIPv4      int             `json:"unique_ipv4"`
	TotalBackends   int             `json:"total_backends"`
	ActiveConns     int64           `json:"active_conns"`
	Selects         uint64          `json:"selects"`
	SelectFail      uint64          `json:"select_fail"`
	ProbeSuccess    uint64          `json:"probe_success"`
	ProbeFailure    uint64          `json:"probe_failure"`
	Strategy        string          `json:"strategy"`
	Mode            string          `json:"mode"`
	UniqueEffort    bool            `json:"unique_ipv4_effort"`
	UniqueStrict    bool            `json:"unique_ipv4_strict"`
	Unique          map[string]any  `json:"unique,omitempty"`
	Backends        []pool.Snapshot `json:"backends"`
}

func New(cfg *config.Config, p *pool.Pool, st *proxy.State, log *zap.Logger) *State {
	return &State{
		Pool:    p,
		Proxy:   st,
		Cfg:     cfg,
		Started: time.Now(),
		Log:     log,
	}
}

func (s *State) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.healthz)
	mux.HandleFunc("/readyz", s.readyz)
	mux.HandleFunc("/metrics", s.metrics)
	mux.HandleFunc("/backends", s.backends)
	mux.HandleFunc("/v1/reconnect/", s.reconnect)
	return mux
}

func (s *State) healthz(w http.ResponseWriter, r *http.Request) {
	healthy := s.Pool.HealthyCount()
	inPool, warming, parked, uniqueIPs := s.Pool.AdmitStats()
	total := len(s.Pool.Backends())
	status := "ok"
	if healthy == 0 || inPool == 0 {
		status = "degraded"
	}
	ceiling := uniqueIPs > 0 && inPool >= total && uniqueIPs < inPool
	if s.UniqueStats != nil {
		if st := s.UniqueStats(); st != nil {
			if v, ok := st["ceiling_hit"].(bool); ok && v {
				ceiling = true
			}
		}
	}
	json.NewEncoder(w).Encode(HealthResponse{
		Status:          status,
		HealthyBackends: healthy,
		InPool:          inPool,
		Warming:         warming,
		Parked:          parked,
		UniqueIPv4:      uniqueIPs,
		CeilingHit:      ceiling,
		TotalBackends:   total,
		ActiveConns:     s.Proxy.Limiter.Active(),
		UptimeSec:       int64(time.Since(s.Started).Seconds()),
	})
}

func (s *State) readyz(w http.ResponseWriter, r *http.Request) {
	if s.Pool.HealthyCount() > 0 {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ready"))
		return
	}
	w.WriteHeader(http.StatusServiceUnavailable)
	w.Write([]byte("no healthy backends"))
}

func (s *State) metrics(w http.ResponseWriter, r *http.Request) {
	m := s.Pool.Metrics()
	inPool, warming, parked, uniqueIPs := s.Pool.AdmitStats()
	var unique map[string]any
	if s.UniqueStats != nil {
		unique = s.UniqueStats()
	}
	json.NewEncoder(w).Encode(MetricsResponse{
		UptimeSec:       int64(time.Since(s.Started).Seconds()),
		HealthyBackends: s.Pool.HealthyCount(),
		InPool:          inPool,
		Warming:         warming,
		Parked:          parked,
		UniqueIPv4:      uniqueIPs,
		TotalBackends:   len(s.Pool.Backends()),
		ActiveConns:     s.Proxy.Limiter.Active(),
		Selects:         m.Selects.Load(),
		SelectFail:      m.SelectFail.Load(),
		ProbeSuccess:    m.Success.Load(),
		ProbeFailure:    m.Failure.Load(),
		Strategy:        string(s.Cfg.Pool.Strategy),
		Mode:            string(s.Cfg.Mode),
		UniqueEffort:    s.Cfg.UniqueEffortActive(),
		UniqueStrict:    s.Cfg.Uniqueness.Strict,
		Unique:          unique,
		Backends:        s.Pool.Snapshots(),
	})
}

func (s *State) backends(w http.ResponseWriter, r *http.Request) {
	json.NewEncoder(w).Encode(s.Pool.Snapshots())
}

func (s *State) reconnect(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	idStr := r.URL.Path[len("/v1/reconnect/"):]
	id, err := strconv.Atoi(idStr)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "bad id"})
		return
	}
	found := false
	for _, b := range s.Pool.Backends() {
		if b.ID == id {
			found = true
			break
		}
	}
	if !found {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": "unknown id"})
		return
	}
	s.Pool.ForceState(id, pool.StateUnknown)
	json.NewEncoder(w).Encode(map[string]any{"ok": true, "id": id, "action": "reset_state"})
}

func (s *State) Serve(ctx context.Context) error {
	addr, err := net.ResolveTCPAddr("tcp", s.Cfg.Listen.Admin)
	if err != nil {
		return err
	}
	ln, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return err
	}
	s.Log.Info("admin API listening", zap.String("addr", s.Cfg.Listen.Admin))

	go func() {
		<-ctx.Done()
		ln.Close()
	}()

	srv := &http.Server{
		Handler: s.Handler(),
	}
	go func() {
		<-ctx.Done()
		srv.Shutdown(context.Background())
	}()

	return srv.Serve(ln)
}
