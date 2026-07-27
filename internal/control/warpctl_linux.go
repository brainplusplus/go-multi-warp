//go:build !windows

package control

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/autoclaw/go-multi-warp/internal/config"
	"go.uber.org/zap"
)

type WarpController struct {
	cfg *config.Config
	log *zap.Logger
}

func NewWarpController(cfg *config.Config, log *zap.Logger) *WarpController {
	return &WarpController{cfg: cfg, log: log}
}

func (w *WarpController) StartAll(ctx context.Context) error {
	n := w.cfg.Instances
	for i := 0; i < n; i++ {
		if err := w.StartOne(i); err != nil {
			w.log.Warn("failed to start instance", zap.Int("id", i), zap.Error(err))
		}
		time.Sleep(w.cfg.Control.StartStagger.Duration)
	}
	return nil
}

func (w *WarpController) StopAll(ctx context.Context) error {
	// kill processes started by controller; registration cleanup optional
	return nil
}

func (w *WarpController) StartOne(id int) error {
	dataDir := w.cfg.DataDir(id)
	runDir := w.cfg.RuntimeDir(id)
	dbusDir := w.cfg.DBusDir(id)
	os.MkdirAll(dataDir, 0o755)
	os.MkdirAll(runDir, 0o755)
	os.MkdirAll(dbusDir, 0o755)

	port := w.cfg.BasePort + id
	dbusSock := filepath.Join(dbusDir, "system_bus_socket")

	if w.cfg.Control.Org != "" {
		if err := w.writeMDM(dataDir, port); err != nil {
			w.log.Warn("write mdm failed", zap.Int("id", id), zap.Error(err))
		}
	}

	// Start dbus daemon for this instance (best-effort)
	dbus := exec.Command("dbus-daemon",
		fmt.Sprintf("--address=unix:path=%s", dbusSock),
		"--config-file=/usr/share/dbus-1/system.conf",
		"--nopidfile", "--nofork")
	dbus.Stdout = nil
	dbus.Stderr = nil
	_ = dbus.Start()
	time.Sleep(400 * time.Millisecond)

	cmd := exec.Command("warp-svc", "--accept-tos")
	cmd.Env = append(os.Environ(),
		fmt.Sprintf("STATE_DIRECTORY=%s", dataDir),
		fmt.Sprintf("RUNTIME_DIRECTORY=%s", runDir),
		fmt.Sprintf("DBUS_SYSTEM_BUS_ADDRESS=unix:path=%s", dbusSock),
	)
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("spawn warp-svc: %w", err)
	}
	w.log.Info("warp-svc spawned", zap.Int("id", id), zap.Int("pid", cmd.Process.Pid))

	if err := w.waitReady(id); err != nil {
		w.log.Warn("instance not ready within timeout", zap.Int("id", id), zap.Error(err))
	}
	if err := w.configureProxyMode(id, port); err != nil {
		return err
	}
	w.log.Info("warp instance ready", zap.Int("id", id), zap.Int("port", port))
	return nil
}

func (w *WarpController) waitReady(id int) error {
	timeout := w.cfg.Control.WarpConnTimeout.Duration
	start := time.Now()
	for {
		if time.Since(start) > timeout {
			return fmt.Errorf("instance %d not ready within timeout", id)
		}
		out, err := w.wcli(id, "status")
		if err == nil {
			if strings.Contains(out, "Status") || strings.Contains(out, "Connected") || strings.Contains(out, "Connecting") {
				return nil
			}
		}
		time.Sleep(800 * time.Millisecond)
	}
}

func (w *WarpController) configureProxyMode(id int, port int) error {
	if w.cfg.Control.Org != "" {
		_, _ = w.wcli(id, "connect")
		_, _ = w.wcli(id, "debug", "qlog", "disable")
		return nil
	}

	dataDir := w.cfg.DataDir(id)
	regFile := filepath.Join(dataDir, "reg.json")
	keyFile := filepath.Join(dataDir, "applied_license.txt")
	wantKey := w.cfg.LicenseKeyFor(id)

	// Drop registration if forced or if assigned license changed.
	if w.cfg.ForceReregister() {
		_ = os.Remove(regFile)
		_ = os.Remove(keyFile)
		_, _ = w.wcli(id, "registration", "delete")
		w.log.Info("force re-register", zap.Int("id", id))
	} else if wantKey != "" {
		if prev, err := os.ReadFile(keyFile); err == nil {
			if strings.TrimSpace(string(prev)) != wantKey {
				_ = os.Remove(regFile)
				_, _ = w.wcli(id, "registration", "delete")
				w.log.Info("license changed; re-register", zap.Int("id", id))
			}
		}
	}

	if _, err := os.Stat(regFile); os.IsNotExist(err) {
		for attempt := 1; attempt <= 12; attempt++ {
			if _, err := w.wcli(id, "registration", "new"); err == nil {
				break
			} else {
				backoff := time.Duration(1<<uint(attempt)) * time.Second
				if backoff > 60*time.Second {
					backoff = 60 * time.Second
				}
				w.log.Warn("registration failed",
					zap.Int("id", id), zap.Int("attempt", attempt), zap.Error(err))
				time.Sleep(backoff)
			}
		}

		// Bind 1 key per instance (key = keys[id % len(keys)]).
		if wantKey != "" {
			if _, err := w.wcli(id, "registration", "license", wantKey); err != nil {
				w.log.Warn("license apply failed",
					zap.Int("id", id),
					zap.String("key_tail", tailKey(wantKey)),
					zap.Error(err))
			} else {
				_ = os.WriteFile(keyFile, []byte(wantKey+"\n"), 0o600)
				w.log.Info("license applied",
					zap.Int("id", id),
					zap.String("key_tail", tailKey(wantKey)))
			}
		} else {
			w.log.Warn("no license key for instance; free registration only",
				zap.Int("id", id),
				zap.Int("available_keys", len(w.cfg.Control.LicenseKeys)))
		}
	}

	if _, err := w.wcli(id, "mode", "proxy"); err != nil {
		return err
	}
	if _, err := w.wcli(id, "proxy", "port", fmt.Sprintf("%d", port)); err != nil {
		return err
	}
	if _, err := w.wcli(id, "connect"); err != nil {
		return err
	}
	_, _ = w.wcli(id, "debug", "qlog", "disable")
	return nil
}

func tailKey(k string) string {
	k = strings.TrimSpace(k)
	if len(k) <= 6 {
		return k
	}
	return "..." + k[len(k)-6:]
}

func (w *WarpController) Reconnect(id int) error {
	_, err := w.wcli(id, "connect")
	return err
}

func (w *WarpController) Restart(id int) error {
	// kill existing via wcli disconnect then start again
	_, _ = w.wcli(id, "disconnect")
	time.Sleep(500 * time.Millisecond)
	return w.StartOne(id)
}

// Reregister drops device registration and creates a fresh free (or licensed) registration.
// Used by progressive unique-IPv4 background expansion. Best-effort only.
func (w *WarpController) Reregister(id int) error {
	dataDir := w.cfg.DataDir(id)
	regFile := filepath.Join(dataDir, "reg.json")
	keyFile := filepath.Join(dataDir, "applied_license.txt")
	_, _ = w.wcli(id, "disconnect")
	_, _ = w.wcli(id, "registration", "delete")
	_ = os.Remove(regFile)
	_ = os.Remove(keyFile)
	time.Sleep(500 * time.Millisecond)

	regOK := false
	for attempt := 1; attempt <= 8; attempt++ {
		if _, err := w.wcli(id, "registration", "new"); err == nil {
			regOK = true
			break
		} else {
			backoff := time.Duration(attempt) * 2 * time.Second
			if backoff > 20*time.Second {
				backoff = 20 * time.Second
			}
			w.log.Warn("reregister registration failed",
				zap.Int("id", id), zap.Int("attempt", attempt), zap.Error(err))
			time.Sleep(backoff)
		}
	}
	if !regOK {
		return fmt.Errorf("reregister: registration new failed for id=%d", id)
	}

	wantKey := w.cfg.LicenseKeyFor(id)
	if wantKey != "" {
		if _, err := w.wcli(id, "registration", "license", wantKey); err != nil {
			w.log.Warn("reregister license apply failed",
				zap.Int("id", id), zap.String("key_tail", tailKey(wantKey)), zap.Error(err))
		} else {
			_ = os.WriteFile(keyFile, []byte(wantKey+"\n"), 0o600)
		}
	}

	port := w.cfg.BasePort + id
	if _, err := w.wcli(id, "mode", "proxy"); err != nil {
		return err
	}
	if _, err := w.wcli(id, "proxy", "port", fmt.Sprintf("%d", port)); err != nil {
		return err
	}
	if _, err := w.wcli(id, "connect"); err != nil {
		return err
	}
	_, _ = w.wcli(id, "debug", "qlog", "disable")
	w.log.Info("reregister complete", zap.Int("id", id), zap.Int("port", port))
	return nil
}

func (w *WarpController) wcli(id int, args ...string) (string, error) {
	runDir := w.cfg.RuntimeDir(id)
	dbusSock := filepath.Join(w.cfg.DBusDir(id), "system_bus_socket")
	cmd := exec.Command("warp-cli", append([]string{"--accept-tos"}, args...)...)
	cmd.Env = append(os.Environ(),
		fmt.Sprintf("RUNTIME_DIRECTORY=%s", runDir),
		fmt.Sprintf("DBUS_SYSTEM_BUS_ADDRESS=unix:path=%s", dbusSock),
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return string(out), fmt.Errorf("warp-cli %v: %w: %s", args, err, out)
	}
	return string(out), nil
}

func (w *WarpController) writeMDM(dataDir string, port int) error {
	body := fmt.Sprintf(`<dict>
  <key>organization</key>
  <string>%s</string>
  <key>auth_client_id</key>
  <string>%s</string>
  <key>auth_client_secret</key>
  <string>%s</string>
  <key>service_mode</key>
  <string>proxy</string>
  <key>proxy_port</key>
  <integer>%d</integer>
  <key>auto_connect</key>
  <integer>1</integer>
  <key>switch_locked</key>
  <true/>
  <key>onboarding</key>
  <false/>
</dict>
`, w.cfg.Control.Org, w.cfg.Control.AuthClientID, w.cfg.Control.AuthClientSecret, port)
	path := filepath.Join(dataDir, "mdm.xml")
	return os.WriteFile(path, []byte(body), 0o600)
}
