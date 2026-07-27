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
}

func New(cfg *config.Config, p *pool.Pool, log *zap.Logger) *Supervisor {
	var ctl *WarpController
	if cfg.Mode == config.ModeManaged {
		ctl = NewWarpController(cfg, log)
	}
	return &Supervisor{pool: p, cfg: cfg, log: log, controller: ctl}
}

func (s *Supervisor) Bootstrap(ctx context.Context) error {
	if s.controller == nil {
		s.log.Info("attach mode: using existing backend ports",
			zap.Int("backends", len(s.pool.Backends())))
		return nil
	}
	if runtime.GOOS == "windows" {
		return fmt.Errorf("managed mode is Linux-only; use attach mode on Windows host")
	}
	return s.controller.StartAll(ctx)
}

func (s *Supervisor) Run(ctx context.Context) {
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
