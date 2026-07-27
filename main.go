package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/autoclaw/go-multi-warp/internal/admin"
	"github.com/autoclaw/go-multi-warp/internal/config"
	"github.com/autoclaw/go-multi-warp/internal/control"
	"github.com/autoclaw/go-multi-warp/internal/health"
	"github.com/autoclaw/go-multi-warp/internal/pool"
	"github.com/autoclaw/go-multi-warp/internal/proxy"
	"go.uber.org/zap"
)

var configPath = flag.String("config", "", "Path to config.yaml")

func main() {
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		fatal(err)
	}

	// logger
	var log *zap.Logger
	var logCfg zap.Config
	if cfg.Logging.JSON {
		logCfg = zap.NewProductionConfig()
	} else {
		logCfg = zap.NewDevelopmentConfig()
		logCfg.EncoderConfig.TimeKey = "ts"
	}
	if lvl, err := zap.ParseAtomicLevel(cfg.Logging.Level); err == nil {
		logCfg.Level = lvl
	}
	log, err = logCfg.Build()
	if err != nil {
		fatal(err)
	}
	defer log.Sync()

	log.Info("go-multi-warp starting",
		zap.String("version", "0.1.0"),
		zap.String("mode", string(cfg.Mode)),
		zap.Int("instances", cfg.Instances),
		zap.Int("backends", len(cfg.BackendAddrs())),
		zap.String("strategy", string(cfg.Pool.Strategy)),
		zap.String("os", runtime.GOOS),
	)

	p := pool.New(cfg)
	st := proxy.NewState(cfg, p)

	// control plane bootstrap
	supervisor := control.New(cfg, p, log)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := supervisor.Bootstrap(ctx); err != nil {
		log.Error("control-plane bootstrap failed", zap.Error(err))
	}

	// health probes
	prober := health.New(cfg, p, log)
	go prober.Run(ctx)

	// supervisor reconnect loop
	go supervisor.Run(ctx)

	// data plane
	socksServer := proxy.NewSocks5Server(st, log)
	go func() {
		if err := socksServer.Serve(ctx); err != nil {
			log.Error("socks5 server exited", zap.Error(err))
			cancel()
		}
	}()

	httpServer := proxy.NewHTTPProxyServer(st, log)
	go func() {
		if err := httpServer.Serve(ctx); err != nil {
			log.Error("http server exited", zap.Error(err))
			cancel()
		}
	}()

	// admin API
	adminState := admin.New(cfg, p, st, log)
	go func() {
		if err := adminState.Serve(ctx); err != nil {
			log.Error("admin server exited", zap.Error(err))
			cancel()
		}
	}()

	log.Info("all listeners up",
		zap.String("socks", cfg.Listen.Socks5),
		zap.String("http", cfg.Listen.HTTP),
		zap.String("admin", cfg.Listen.Admin),
	)

	// wait for signal
	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh
	log.Info("shutdown signal received")
	cancel()
	time.Sleep(500 * time.Millisecond)
	log.Info("go-multi-warp stopped")
}

func fatal(err error) {
	os.Stderr.WriteString("fatal: " + err.Error() + "\n")
	os.Exit(1)
}
