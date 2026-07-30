package health

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/brainplusplus/go-multi-warp/internal/config"
	"github.com/brainplusplus/go-multi-warp/internal/pool"
	"go.uber.org/zap"
	"golang.org/x/net/proxy"
)

type Prober struct {
	pool *pool.Pool
	cfg  *config.Config
	log  *zap.Logger
}

func New(cfg *config.Config, p *pool.Pool, log *zap.Logger) *Prober {
	return &Prober{pool: p, cfg: cfg, log: log}
}

func (p *Prober) Run(ctx context.Context) {
	interval := p.cfg.ProbeEvery.Duration
	p.log.Info("health probe started", zap.Duration("interval", interval))

	// first pass immediately
	p.probeAll()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			p.probeAll()
		case <-ctx.Done():
			p.log.Info("health probe shutting down")
			return
		}
	}
}

func (p *Prober) probeAll() {
	var wg sync.WaitGroup
	for _, b := range p.pool.Backends() {
		wg.Add(1)
		go func(b *pool.Backend) {
			defer wg.Done()
			info, err := probeOne(context.Background(), b.Addr, p.cfg.ProbeURL, p.cfg.RequireWarp)
			if err != nil {
				p.log.Warn("probe fail",
					zap.Int("id", b.ID),
					zap.String("addr", b.Addr),
					zap.Error(err))
				p.pool.MarkProbeFail(b.ID, err.Error())
				if b.Fails() >= 3 {
					p.pool.ForceState(b.ID, pool.StateUnhealthy)
				}
				return
			}
			p.log.Debug("probe ok",
				zap.Int("id", b.ID),
				zap.String("ip", info.IP),
				zap.String("warp", info.Warp),
				zap.Int64("latency_ms", info.LatencyMS))
			p.pool.MarkProbeOK(b.ID, info)
		}(b)
	}
	wg.Wait()
}

func probeOne(ctx context.Context, socksAddr, rawURL string, requireWarp bool) (*pool.ProbeInfo, error) {
	dialer, err := proxy.SOCKS5("tcp", socksAddr, nil, &net.Dialer{Timeout: 5 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("socks5 dialer: %w", err)
	}

	transport := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return dialer.Dial(network, addr)
		},
		TLSHandshakeTimeout: 5 * time.Second,
	}
	client := &http.Client{Transport: transport, Timeout: 8 * time.Second}

	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	_ = u

	t0 := time.Now()
	resp, err := client.Get(rawURL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	latency := time.Since(t0).Milliseconds()

	info := &pool.ProbeInfo{LatencyMS: latency}
	for _, line := range strings.Split(string(body), "\n") {
		if v, ok := strings.CutPrefix(line, "warp="); ok {
			info.Warp = strings.TrimSpace(v)
		} else if v, ok := strings.CutPrefix(line, "ip="); ok {
			info.IP = strings.TrimSpace(v)
		} else if v, ok := strings.CutPrefix(line, "loc="); ok {
			info.Loc = strings.TrimSpace(v)
		}
	}

	if requireWarp {
		switch info.Warp {
		case "on", "plus":
		default:
			return nil, fmt.Errorf("warp not active (got %q)", info.Warp)
		}
	}
	return info, nil
}
