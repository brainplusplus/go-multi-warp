package proxy

import (
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"strings"

	"github.com/brainplusplus/go-multi-warp/internal/config"
	"github.com/brainplusplus/go-multi-warp/internal/pool"
)

var (
	ErrAuthRequired = errors.New("authentication required")
	ErrAuthFailed   = errors.New("authentication failed")
	ErrNoHealthy    = errors.New("no healthy WARP backend available")
)

type State struct {
	Pool    *pool.Pool
	Limiter *pool.ConnLimiter
	Cfg     *config.Config
	Auth    *ProxyAuth
}

func NewState(cfg *config.Config, p *pool.Pool) *State {
	limiter := pool.NewConnLimiter(cfg.Limits.MaxConnGlobal, cfg.Limits.MaxConnPerIP, cfg.Limits.MaxRPSPerIP)
	auth := NewProxyAuth(cfg.Auth.Username, cfg.Auth.Password)
	return &State{
		Pool:    p,
		Limiter: limiter,
		Cfg:     cfg,
		Auth:    auth,
	}
}

type ProxyAuth struct {
	enabled  bool
	username string
	password string
}

func NewProxyAuth(user, pass string) *ProxyAuth {
	return &ProxyAuth{
		enabled:  user != "" && pass != "",
		username: user,
		password: pass,
	}
}

func (pa *ProxyAuth) Check(user, pass string) error {
	if !pa.enabled {
		return nil
	}
	if user == pa.username && pass == pa.password {
		return nil
	}
	return ErrAuthFailed
}

func (pa *ProxyAuth) CheckHTTPHeader(header string) error {
	if !pa.enabled {
		return nil
	}
	if header == "" {
		return ErrAuthRequired
	}
	header = strings.TrimSpace(header)
	if !strings.HasPrefix(strings.ToLower(header), "basic ") {
		return ErrAuthFailed
	}
	raw := header[6:]
	dec, err := base64.StdEncoding.DecodeString(strings.TrimSpace(raw))
	if err != nil {
		return ErrAuthFailed
	}
	u, p, ok := strings.Cut(string(dec), ":")
	if !ok {
		return ErrAuthFailed
	}
	return pa.Check(u, p)
}

// splitHostPort parses "host:port" or "host" with default port.
func SplitHostPort(target string, defaultPort int) (string, int, error) {
	if host, portStr, err := net.SplitHostPort(target); err == nil {
		var port int
		if _, err := fmt.Sscanf(portStr, "%d", &port); err != nil {
			return "", 0, fmt.Errorf("bad port %q", portStr)
		}
		return host, port, nil
	}
	return target, defaultPort, nil
}
