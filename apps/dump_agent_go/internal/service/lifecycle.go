package service

import (
	"errors"
	"fmt"
	"os"
	"time"
)

// ErrServiceNotFound indicates the named service is not registered in the
// SCM. Uninstall treats this as success (idempotent).
var ErrServiceNotFound = errors.New("service_not_found")

// State is a platform-independent mirror of the Windows SCM service
// states this package needs. Keeping it portable (no golang.org/x/sys/
// windows/svc types) is what makes uninstallService testable on any GOOS —
// that package tree is windows-only and can't be imported from here.
type State int

const (
	StateUnknown State = iota
	StateStopped
	StateRunning
	StateStartPending
	StateStopPending
	StateOther
)

// scmService is the seam over one open SCM service handle. install_windows.go
// implements it over golang.org/x/sys/windows/svc/mgr.Service; tests provide
// a fake.
type scmService interface {
	State() (State, error)
	Stop() error
	Delete() error
	Close() error
}

// scmConnector opens a named service. Open must return an error satisfying
// errors.Is(err, ErrServiceNotFound) when the service isn't registered.
type scmConnector interface {
	Open(name string) (scmService, error)
}

// Overridable in tests so the timeout path doesn't cost stopWaitTimeout of
// wall-clock time per test run.
var (
	stopPollInterval = 500 * time.Millisecond
	stopWaitTimeout  = 30 * time.Second
)

// uninstallService stops a running service, deletes it from the SCM, then
// deregisters its eventlog source — in that order. A running service must
// not keep logging through a deregistered source (H7), and a failed
// stop/delete must not deregister the source anyway. Idempotent: an absent
// service is success, matching RemoveEventSource's own idempotency (H1/H2).
func uninstallService(connector scmConnector, removeEventSource func(string) error) int {
	svcHandle, err := connector.Open(ServiceName)
	if errors.Is(err, ErrServiceNotFound) {
		if cleanupErr := removeEventSource(EventSourceName); cleanupErr != nil {
			fmt.Fprintf(os.Stderr, "warn=eventlog_source_cleanup err=%v\n", cleanupErr)
		}
		fmt.Printf("uninstall=service_absent service=%s\n", ServiceName)
		return 0
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "open_service=%v\n", err)
		return 1
	}
	defer svcHandle.Close()

	if err := stopIfRunning(svcHandle); err != nil {
		fmt.Fprintf(os.Stderr, "stop_service=%v\n", err)
		return 1
	}
	if err := svcHandle.Delete(); err != nil {
		fmt.Fprintf(os.Stderr, "delete_service=%v\n", err)
		return 1
	}
	if err := removeEventSource(EventSourceName); err != nil {
		fmt.Fprintf(os.Stderr, "warn=eventlog_source_removal err=%v\n", err)
	}
	fmt.Printf("uninstalled service=%s\n", ServiceName)
	return 0
}

// stopIfRunning issues Stop and polls until the service reports Stopped or
// stopWaitTimeout elapses. No-op if the service is already stopped.
func stopIfRunning(s scmService) error {
	state, err := s.State()
	if err != nil {
		return fmt.Errorf("query_state=%w", err)
	}
	if state == StateStopped {
		return nil
	}
	if state != StateStopPending {
		if err := s.Stop(); err != nil {
			return fmt.Errorf("control_stop=%w", err)
		}
	}
	return waitForStopped(s)
}

func waitForStopped(s scmService) error {
	deadline := time.Now().Add(stopWaitTimeout)
	for time.Now().Before(deadline) {
		state, err := s.State()
		if err != nil {
			return fmt.Errorf("query_state=%w", err)
		}
		if state == StateStopped {
			return nil
		}
		time.Sleep(stopPollInterval)
	}
	return fmt.Errorf("stop_timeout=%s", stopWaitTimeout)
}
