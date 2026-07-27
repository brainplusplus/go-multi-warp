//go:build windows

package control

import (
	"context"
	"fmt"

	"github.com/autoclaw/go-multi-warp/internal/config"
	"go.uber.org/zap"
)

// WarpController is a no-op on Windows — warp-svc spawn is Linux-only.
type WarpController struct {
	cfg *config.Config
	log *zap.Logger
}

func NewWarpController(cfg *config.Config, log *zap.Logger) *WarpController {
	return &WarpController{cfg: cfg, log: log}
}

func (w *WarpController) StartAll(ctx context.Context) error {
	w.log.Warn("managed mode warp-svc spawn is Linux-only; use attach mode on Windows host")
	return nil
}

func (w *WarpController) StopAll(ctx context.Context) error { return nil }

func (w *WarpController) StartOne(id int) error {
	return fmt.Errorf("managed mode warp-svc spawn is Linux-only")
}

func (w *WarpController) Reconnect(id int) error {
	return fmt.Errorf("managed mode warp-svc spawn is Linux-only")
}

func (w *WarpController) Restart(id int) error {
	return fmt.Errorf("managed mode warp-svc spawn is Linux-only")
}

func (w *WarpController) Reregister(id int) error {
	return fmt.Errorf("managed mode warp-svc spawn is Linux-only")
}
