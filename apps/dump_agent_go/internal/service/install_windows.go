//go:build windows

package service

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

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

	svcArgs := []string{"service"}
	if *configPath != "" {
		svcArgs = append(svcArgs, "--config", *configPath)
	}

	s, err := m.CreateService(ServiceName, absExe, cfg, svcArgs...)
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

	fmt.Printf("installed service=%s exe=%s\n", ServiceName, absExe)
	return 0
}

// Uninstall remove CnesDumpAgent do SCM.
func Uninstall() int {
	m, err := mgr.Connect()
	if err != nil {
		fmt.Fprintf(os.Stderr, "scm_connect: %v\n", err)
		return 1
	}
	defer m.Disconnect()

	s, err := m.OpenService(ServiceName)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open_service: %v\n", err)
		return 1
	}
	defer s.Close()

	if err := s.Delete(); err != nil {
		fmt.Fprintf(os.Stderr, "delete_service: %v\n", err)
		return 1
	}
	fmt.Printf("uninstalled service=%s\n", ServiceName)
	return 0
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
