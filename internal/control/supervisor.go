package control

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"github.com/autoclaw/go-multi-warp/internal/config"
	"github.com/autoclaw/go-multi-warp/internal/pool"
	"go.uber.org/zap"
)

type Supervisor struct {
	pool       *pool.Pool
	cfg        *config.Config
	log        *zap.Logger
	controller *WarpController
	unique     *UniquenessEngine
}

func New(cfg *config.Config, p *pool.Pool, log *zap.Logger) *Supervisor {
	var ctl *WarpController
	if cfg.Mode == config.ModeManaged {
		ctl = NewWarpController(cfg, log)
	}
	s := &Supervisor{pool: p, cfg: cfg, log: log, controller: ctl}
	s.unique = NewUniquenessEngine(cfg, p, ctl, log)
	return s
}

func (s *Supervisor) Controller() *WarpController { return s.controller }

func (s *Supervisor) Uniqueness() *UniquenessEngine { return s.unique }

func (s *Supervisor) Bootstrap(ctx context.Context) error {
	if s.controller == nil {
		s.log.Info("attach mode: using existing backend ports",
			zap.Int("backends", len(s.pool.Backends())))
		// In attach mode backends may already be live — uniqueness will admit progressively.
		return nil
	}
	if runtime.GOOS == "windows" {
		return fmt.Errorf("managed mode is Linux-only; use attach mode on Windows host")
	}
	// Non-blocking: start all warp-svc in background so proxy listeners can open immediately.
	// Uniqueness engine admits first healthy instance ASAP; others expand in background.
	go func() {
		if err := s.controller.StartAll(ctx); err != nil {
			s.log.Error("control-plane start_all failed", zap.Error(err))
		}
	}()
	s.log.Info("managed bootstrap: warp instances starting in background (progressive admit)")
	return nil
}

func (s *Supervisor) Run(ctx context.Context) {
	if s.unique != nil {
		go s.unique.Run(ctx)
	}
	s.runReconnectLoop(ctx)
}

func (s *Supervisor) runReconnectLoop(ctx context.Context) {
	interval := s.cfg.ProbeEvery.Duration * 2
	if interval < 5*time.Second {
		interval = 5 * time.Second
	}
	s.log.Info("supervisor started")

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.tick()
		case <-ctx.Done():
			s.log.Info("supervisor shutting down")
			if s.controller != nil {
				if err := s.controller.StopAll(ctx); err != nil {
					s.log.Warn("stop_all failed", zap.Error(err))
				}
			}
			return
		}
	}
}

func (s *Supervisor) tick() {
	for _, snap := range s.pool.Snapshots() {
		needsSoft := (snap.State == "unhealthy" || snap.State == "circuit_open") &&
			snap.Fails >= s.cfg.Control.ReconnectFails
		needsHard := snap.Fails >= s.cfg.Control.HardRestartFails
		if !needsSoft && !needsHard {
			continue
		}

		if s.controller != nil {
			if needsHard {
				s.log.Warn("hard restart warp instance",
					zap.Int("id", snap.ID), zap.Int("fails", snap.Fails))
				if err := s.controller.Restart(snap.ID); err != nil {
					s.log.Warn("restart failed", zap.Int("id", snap.ID), zap.Error(err))
				} else {
					s.pool.ForceState(snap.ID, pool.StateUnknown)
				}
			} else if needsSoft {
				s.log.Warn("soft reconnect warp instance",
					zap.Int("id", snap.ID), zap.Int("fails", snap.Fails))
				if err := s.controller.Reconnect(snap.ID); err != nil {
					s.log.Warn("reconnect failed", zap.Int("id", snap.ID), zap.Error(err))
				}
			}
		} else {
			s.log.Debug("attach mode: waiting for backend recovery",
				zap.Int("id", snap.ID), zap.Int("fails", snap.Fails))
		}
	}
}
