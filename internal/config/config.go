package config

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// RunMode controls whether the manager spawns WARP processes.
type RunMode string

const (
	ModeManaged RunMode = "managed" // spawn/control warp-svc (Linux only)
	ModeAttach  RunMode = "attach"  // LB across existing SOCKS backends (cross-platform)
)

type PoolStrategy string

const (
	StrategyRoundRobin PoolStrategy = "round_robin"
	StrategyLeastConn  PoolStrategy = "least_conn"
	StrategySticky     PoolStrategy = "sticky"
)

type Config struct {
	Listen       Listen     `yaml:"listen"`
	Auth         Auth       `yaml:"auth"`
	Mode         RunMode    `yaml:"mode"`
	Instances    int        `yaml:"instances"`
	BasePort     int        `yaml:"base_port"`
	ConnTO       Duration   `yaml:"connect_timeout_ms"`
	ProbeEvery   Duration   `yaml:"probe_interval_ms"`
	ProbeURL     string     `yaml:"probe_url"`
	RequireWarp  bool       `yaml:"probe_require_warp"`
	Pool         Pool       `yaml:"pool"`
	Limits       Limits     `yaml:"limits"`
	Control      Control    `yaml:"control"`
	Logging      Logging    `yaml:"logging"`
	Backends     []Backend  `yaml:"backends"`
}

type Listen struct {
	Socks5 string `yaml:"socks5"`
	HTTP   string `yaml:"http"`
	Admin  string `yaml:"admin"`
}

type Auth struct {
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

type Pool struct {
	Strategy         PoolStrategy `yaml:"strategy"`
	MaxFails         int          `yaml:"max_fails"`
	FailTimeout      Duration     `yaml:"fail_timeout_ms"`
	OpenAfter        Duration     `yaml:"open_after_ms"`
	MaxInflightPerBE int          `yaml:"max_inflight_per_backend"`
	StickyTTL        Duration     `yaml:"sticky_ttl_ms"`
}

type Limits struct {
	MaxConnGlobal int      `yaml:"max_conn_global"`
	MaxConnPerIP  int      `yaml:"max_conn_per_ip"`
	MaxRPSPerIP   int      `yaml:"max_rps_per_ip"`
	DialTimeout   Duration `yaml:"dial_timeout_ms"`
	IOTimeout     Duration `yaml:"io_timeout_ms"`
}

type Control struct {
	// WarpConnTimeout is in seconds when loaded from yaml integer (see UnmarshalYAML seconds helper).
	WarpConnTimeout  DurationSec `yaml:"warp_connect_timeout_sec"`
	StartStagger     Duration `yaml:"start_stagger_ms"`
	ReconnectFails   int      `yaml:"reconnect_after_fails"`
	HardRestartFails int      `yaml:"hard_restart_after_fails"`
	LicenseKeys      []string `yaml:"license_keys"`
	Org              string   `yaml:"org"`
	AuthClientID     string   `yaml:"auth_client_id"`
	AuthClientSecret string   `yaml:"auth_client_secret"`
	DataRoot         string   `yaml:"data_root"`
	RuntimeRoot      string   `yaml:"runtime_root"`
}

// DurationSec is like Duration but bare integers are seconds (for warp_connect_timeout_sec).
type DurationSec struct {
	time.Duration
}

func (d *DurationSec) UnmarshalYAML(value *yaml.Node) error {
	var sec int64
	if err := value.Decode(&sec); err == nil {
		d.Duration = time.Duration(sec) * time.Second
		return nil
	}
	var s string
	if err := value.Decode(&s); err != nil {
		return err
	}
	t, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	d.Duration = t
	return nil
}

type Logging struct {
	Level string `yaml:"level"`
	JSON  bool   `yaml:"json"`
}

type Backend struct {
	ID   int    `yaml:"id"`
	Addr string `yaml:"addr"`
}

// Duration wraps time.Duration with YAML ms-aware parsing.
type Duration struct {
	time.Duration
}

func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	var ms int64
	if err := value.Decode(&ms); err == nil {
		d.Duration = time.Duration(ms) * time.Millisecond
		return nil
	}
	var s string
	if err := value.Decode(&s); err != nil {
		return err
	}
	t, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	d.Duration = t
	return nil
}

func Default() Config {
	return Config{
		Listen:      Listen{Socks5: "0.0.0.0:11080", HTTP: "0.0.0.0:18080", Admin: "0.0.0.0:9090"},
		Mode:        ModeAttach,
		Instances:   5,
		BasePort:    40000,
		ConnTO:      Duration{5 * time.Second},
		ProbeEvery:  Duration{5 * time.Second},
		ProbeURL:    "https://cloudflare.com/cdn-cgi/trace",
		RequireWarp: true,
		Pool: Pool{
			Strategy:         StrategyLeastConn,
			MaxFails:         3,
			FailTimeout:      Duration{15 * time.Second},
			OpenAfter:        Duration{10 * time.Second},
			MaxInflightPerBE: 512,
			StickyTTL:        Duration{5 * time.Minute},
		},
		Limits: Limits{
			MaxConnGlobal: 20000,
			MaxConnPerIP:  500,
			DialTimeout:   Duration{8 * time.Second},
			IOTimeout:     Duration{2 * time.Minute},
		},
		Control: Control{
			WarpConnTimeout:  DurationSec{60 * time.Second},
			StartStagger:     Duration{3 * time.Second},
			ReconnectFails:   2,
			HardRestartFails: 6,
			DataRoot:         defaultDataRoot(),
			RuntimeRoot:      filepath.Join(os.TempDir(), "go-multi-warp"),
		},
		Logging: Logging{Level: "info", JSON: false},
	}
}

func defaultDataRoot() string {
	if runtime.GOOS == "windows" {
		return filepath.Join(os.Getenv("LOCALAPPDATA"), "go-multi-warp", "data")
	}
	return "/var/lib/cloudflare-warp"
}

// Load reads config from path (if provided), config.yaml, or /etc/multi-warp/config.yaml,
// applies env overrides, then validates.
func Load(path string) (*Config, error) {
	cfg := Default()

	if path == "" {
		path = os.Getenv("MULTI_WARP_CONFIG")
	}
	if path == "" {
		if _, err := os.Stat("config.yaml"); err == nil {
			path = "config.yaml"
		} else if _, err := os.Stat("/etc/multi-warp/config.yaml"); err == nil {
			path = "/etc/multi-warp/config.yaml"
		}
	}

	if path != "" {
		if err := loadFile(path, &cfg); err != nil {
			return nil, err
		}
	}

	cfg.applyEnv()

	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func loadFile(path string, cfg *Config) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read config %s: %w", path, err)
	}
	tmp := *cfg
	if err := yaml.Unmarshal(raw, &tmp); err != nil {
		return fmt.Errorf("parse config %s: %w", path, err)
	}
	*cfg = tmp
	return nil
}

func (c *Config) applyEnv() {
	if v := os.Getenv("WARP_INSTANCES"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			c.Instances = n
		}
	}
	if v := os.Getenv("MULTI_WARP_INSTANCES"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			c.Instances = n
		}
	}
	if v := os.Getenv("MULTI_WARP_BASE_PORT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			c.BasePort = n
		}
	}
	if v := os.Getenv("MULTI_WARP_MODE"); v != "" {
		switch strings.ToLower(v) {
		case "managed":
			c.Mode = ModeManaged
		case "attach":
			c.Mode = ModeAttach
		}
	}
	if v := os.Getenv("PROXY_USER"); v != "" {
		c.Auth.Username = v
	}
	if v := os.Getenv("PROXY_PASS"); v != "" {
		c.Auth.Password = v
	}
	if v := os.Getenv("PROXY_MAX_CONN"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			c.Limits.MaxConnPerIP = n
		}
	}
	if v := os.Getenv("MULTI_WARP_SOCKS"); v != "" {
		c.Listen.Socks5 = v
	}
	if v := os.Getenv("MULTI_WARP_HTTP"); v != "" {
		c.Listen.HTTP = v
	}
	if v := os.Getenv("MULTI_WARP_ADMIN"); v != "" {
		c.Listen.Admin = v
	}
	if v := os.Getenv("WARP_LICENSE_KEY"); v != "" {
		c.Control.LicenseKeys = splitCSV(v)
	}
	if v := os.Getenv("WARP_ORG"); v != "" {
		c.Control.Org = v
	}
	if v := os.Getenv("WARP_AUTH_CLIENT_ID"); v != "" {
		c.Control.AuthClientID = v
	}
	if v := os.Getenv("WARP_AUTH_CLIENT_SECRET"); v != "" {
		c.Control.AuthClientSecret = v
	}
	if v := os.Getenv("RUST_LOG"); v != "" {
		c.Logging.Level = v
	}
	if v := os.Getenv("MULTI_WARP_BACKENDS"); v != "" {
		var list []Backend
		for i, part := range strings.Split(v, ",") {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			list = append(list, Backend{ID: i, Addr: part})
		}
		if len(list) > 0 {
			c.Backends = list
		}
	}
}

func splitCSV(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func (c *Config) Validate() error {
	if c.Instances <= 0 && len(c.Backends) == 0 {
		return fmt.Errorf("instances must be >= 1 or backends must be non-empty")
	}
	if c.BasePort == 0 {
		return fmt.Errorf("base_port must be non-zero")
	}
	if c.Mode == ModeManaged && runtime.GOOS == "windows" {
		return fmt.Errorf("managed mode is Linux-only; use attach mode on Windows")
	}
	if c.Control.Org != "" {
		if c.Control.AuthClientID == "" || c.Control.AuthClientSecret == "" {
			return fmt.Errorf("WARP_ORG requires auth_client_id and auth_client_secret")
		}
		if len(c.Control.LicenseKeys) > 0 {
			return fmt.Errorf("org and license_keys are mutually exclusive")
		}
	}
	for _, label := range []struct{ name, addr string }{
		{"listen.socks5", c.Listen.Socks5},
		{"listen.http", c.Listen.HTTP},
		{"listen.admin", c.Listen.Admin},
	} {
		if _, err := net.ResolveTCPAddr("tcp", label.addr); err != nil {
			return fmt.Errorf("%s: %w", label.name, err)
		}
	}
	return nil
}

func (c *Config) BackendAddrs() []Backend {
	if len(c.Backends) > 0 {
		return c.Backends
	}
	out := make([]Backend, 0, c.Instances)
	for i := 0; i < c.Instances; i++ {
		out = append(out, Backend{ID: i, Addr: fmt.Sprintf("127.0.0.1:%d", c.BasePort+i)})
	}
	return out
}

func (c *Config) AuthEnabled() bool {
	return c.Auth.Username != "" && c.Auth.Password != ""
}

func (c *Config) DataDir(id int) string {
	return filepath.Join(c.Control.DataRoot, fmt.Sprintf("instance-%d", id))
}

func (c *Config) RuntimeDir(id int) string {
	return filepath.Join(c.Control.RuntimeRoot, fmt.Sprintf("warp-%d", id))
}

func (c *Config) DBusDir(id int) string {
	return filepath.Join(c.Control.RuntimeRoot, fmt.Sprintf("dbus-%d", id))
}
