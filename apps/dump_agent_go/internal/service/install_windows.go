//go:build windows

package service

import (
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/sys/windows"
	"golang.org/x/sys/windows/registry"
	"golang.org/x/sys/windows/svc"
	"golang.org/x/sys/windows/svc/mgr"
)

// Install registra CnesDumpAgent no SCM. Aceita flags:
//   --config <path>    (opcional) caminho para arquivo .env da service
//   --start-type auto  (default) auto | manual | disabled
func Install(args []string) int {
	fs := flag.NewFlagSet("install", flag.ExitOnError)
	configPath := fs.String("config", "", "path to .env file")
	startType := fs.String("start-type", "auto", "auto|manual|disabled")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	exe, err := os.Executable()
	if err != nil {
		fmt.Fprintf(os.Stderr, "executable path: %v\n", err)
		return 1
	}
	absExe, err := filepath.Abs(exe)
	if err != nil {
		fmt.Fprintf(os.Stderr, "abs path: %v\n", err)
		return 1
	}

	m, err := mgr.Connect()
	if err != nil {
		fmt.Fprintf(os.Stderr, "scm_connect: %v\n", err)
		return 1
	}
	defer m.Disconnect()

	existing, err := m.OpenService(ServiceName)
	if err == nil {
		existing.Close()
		fmt.Fprintln(os.Stderr, "service_already_exists: run `dumpagent uninstall` first")
		return 1
	}

	cfg := mgr.Config{
		DisplayName:  DisplayName,
		StartType:    mapStartType(*startType),
		ErrorControl: mgr.ErrorNormal,
		ServiceType:  0x10, // SERVICE_WIN32_OWN_PROCESS
	}

	s, err := m.CreateService(ServiceName, absExe, cfg, "service")
	if err != nil {
		fmt.Fprintf(os.Stderr, "create_service: %v\n", err)
		return 1
	}
	defer s.Close()

	if err := s.SetRecoveryActions([]mgr.RecoveryAction{
		{Type: mgr.ServiceRestart, Delay: 60 * time.Second},
		{Type: mgr.ServiceRestart, Delay: 120 * time.Second},
		{Type: mgr.ServiceRestart, Delay: 300 * time.Second},
	}, 86400); err != nil {
		slog.Warn("set_recovery_actions_failed", "err", err.Error())
	}

	if *configPath != "" {
		if err := writeServiceEnvironment(*configPath); err != nil {
			fmt.Fprintf(os.Stderr, "warn: config not applied to service environment: %v\n", err)
			fmt.Fprintln(os.Stderr, "warn: service will fail to boot until `dumpagent set-secret` "+
				"and a non-secret env config are provided; see docs/runbooks/dumpagent-install-windows.md")
		}
	}

	fmt.Printf("installed service=%s exe=%s\n", ServiceName, absExe)
	return 0
}

// Uninstall stops (if running), removes CnesDumpAgent from the SCM, then
// deregisters its eventlog source, in that order (see lifecycle.go).
// Idempotent: an absent service is success.
func Uninstall() int {
	return uninstallService(defaultSCMConnector{}, RemoveEventSource)
}

// writeServiceEnvironment parses a flat KEY=VALUE config file and writes
// the non-secret entries into the service's own registry Environment value
// (REG_MULTI_SZ under its HKLM\...\Services\<name> key), which the SCM
// injects into the service process at start. This is how --config actually
// reaches the running service (previously a no-op — see H4). Secret-like
// keys are rejected; use `dumpagent set-secret` for credentials instead.
func writeServiceEnvironment(configPath string) error {
	lines, err := parseEnvFile(configPath)
	if err != nil {
		return err
	}
	safe, rejected := splitSecretLines(lines)
	if len(rejected) > 0 {
		fmt.Fprintf(os.Stderr, "warn: rejected secret-like keys from --config (use "+
			"`dumpagent set-secret` instead, never plain env): %v\n", rejected)
	}
	key, _, err := registry.CreateKey(registry.LOCAL_MACHINE,
		`SYSTEM\CurrentControlSet\Services\`+ServiceName, registry.SET_VALUE)
	if err != nil {
		return fmt.Errorf("open_service_key: %w", err)
	}
	defer key.Close()
	if err := key.SetStringsValue("Environment", safe); err != nil {
		return fmt.Errorf("set_environment: %w", err)
	}
	return nil
}

func mapStartType(s string) uint32 {
	switch s {
	case "auto":
		return mgr.StartAutomatic
	case "manual":
		return mgr.StartManual
	case "disabled":
		return mgr.StartDisabled
	}
	return mgr.StartAutomatic
}

// windowsService adapts *mgr.Service (+ its owning *mgr.Mgr connection) to
// the portable scmService seam defined in lifecycle.go.
type windowsService struct {
	svc *mgr.Service
	mgr *mgr.Mgr
}

func (w *windowsService) State() (State, error) {
	status, err := w.svc.Query()
	if err != nil {
		return StateUnknown, err
	}
	return fromSvcState(status.State), nil
}

func (w *windowsService) Stop() error {
	_, err := w.svc.Control(svc.Stop)
	return err
}

func (w *windowsService) Delete() error { return w.svc.Delete() }

func (w *windowsService) Close() error {
	svcErr := w.svc.Close()
	mgrErr := w.mgr.Disconnect()
	if svcErr != nil {
		return svcErr
	}
	return mgrErr
}

func fromSvcState(s svc.State) State {
	switch s {
	case svc.Stopped:
		return StateStopped
	case svc.Running:
		return StateRunning
	case svc.StartPending:
		return StateStartPending
	case svc.StopPending:
		return StateStopPending
	default:
		return StateOther
	}
}

// defaultSCMConnector is the production scmConnector, backed by the real SCM.
type defaultSCMConnector struct{}

var _ scmConnector = defaultSCMConnector{}

func (defaultSCMConnector) Open(name string) (scmService, error) {
	m, err := mgr.Connect()
	if err != nil {
		return nil, fmt.Errorf("scm_connect: %w", err)
	}
	s, err := m.OpenService(name)
	if err != nil {
		_ = m.Disconnect()
		if errors.Is(err, windows.ERROR_SERVICE_DOES_NOT_EXIST) {
			return nil, ErrServiceNotFound
		}
		return nil, fmt.Errorf("open_service: %w", err)
	}
	return &windowsService{svc: s, mgr: m}, nil
}
