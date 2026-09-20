package service

import (
	"errors"
	"testing"
	"time"
)

type fakeService struct {
	state       State
	stopCalls   int
	deleteCalls int
	closeCalls  int
	stopErr     error
	deleteErr   error
	// stopSetsState, if true, simulates a Stop() call transitioning the
	// service to Stopped immediately (as a fake SCM would after the
	// controlled process exits).
	stopSetsState bool
}

func (f *fakeService) State() (State, error) { return f.state, nil }

func (f *fakeService) Stop() error {
	f.stopCalls++
	if f.stopErr != nil {
		return f.stopErr
	}
	if f.stopSetsState {
		f.state = StateStopped
	}
	return nil
}

func (f *fakeService) Delete() error {
	f.deleteCalls++
	return f.deleteErr
}

func (f *fakeService) Close() error {
	f.closeCalls++
	return nil
}

type fakeConnector struct {
	svc *fakeService
	err error
}

func (c *fakeConnector) Open(_ string) (scmService, error) {
	if c.err != nil {
		return nil, c.err
	}
	return c.svc, nil
}

func withFastStopTimeout(t *testing.T) {
	t.Helper()
	origInterval, origTimeout := stopPollInterval, stopWaitTimeout
	stopPollInterval, stopWaitTimeout = time.Millisecond, 10*time.Millisecond
	t.Cleanup(func() { stopPollInterval, stopWaitTimeout = origInterval, origTimeout })
}

func noopRemoveEventSource(string) error { return nil }

func TestUninstall_ServicoRodando_ParaAntesDeExcluir(t *testing.T) {
	withFastStopTimeout(t)
	svc := &fakeService{state: StateRunning, stopSetsState: true}
	rc := uninstallService(&fakeConnector{svc: svc}, noopRemoveEventSource)
	if rc != 0 {
		t.Fatalf("rc = %d, want 0", rc)
	}
	if svc.stopCalls != 1 {
		t.Fatalf("stopCalls = %d, want 1 (must stop before delete)", svc.stopCalls)
	}
	if svc.deleteCalls != 1 {
		t.Fatalf("deleteCalls = %d, want 1", svc.deleteCalls)
	}
}

func TestUninstall_ServicoParado_NaoChamaStop(t *testing.T) {
	svc := &fakeService{state: StateStopped}
	rc := uninstallService(&fakeConnector{svc: svc}, noopRemoveEventSource)
	if rc != 0 {
		t.Fatalf("rc = %d, want 0", rc)
	}
	if svc.stopCalls != 0 {
		t.Fatalf("stopCalls = %d, want 0 (already stopped)", svc.stopCalls)
	}
	if svc.deleteCalls != 1 {
		t.Fatalf("deleteCalls = %d, want 1", svc.deleteCalls)
	}
}

func TestUninstall_ServicoAusente_EhIdempotente(t *testing.T) {
	rc := uninstallService(&fakeConnector{err: ErrServiceNotFound}, noopRemoveEventSource)
	if rc != 0 {
		t.Fatalf("rc = %d, want 0 (absent service = success)", rc)
	}
}

func TestUninstall_ErroAoAbrirServicoNaoAusente_RetornaFalha(t *testing.T) {
	rc := uninstallService(&fakeConnector{err: errors.New("access_denied")}, noopRemoveEventSource)
	if rc != 1 {
		t.Fatalf("rc = %d, want 1 (real open error must not be swallowed)", rc)
	}
}

func TestUninstall_FalhaAoParar_NaoExcluiENaoRemoveEventSource(t *testing.T) {
	withFastStopTimeout(t)
	svc := &fakeService{state: StateRunning, stopErr: errors.New("control_denied")}
	eventSourceRemoved := false
	rc := uninstallService(&fakeConnector{svc: svc}, func(string) error {
		eventSourceRemoved = true
		return nil
	})
	if rc != 1 {
		t.Fatalf("rc = %d, want 1", rc)
	}
	if svc.deleteCalls != 0 {
		t.Fatalf("deleteCalls = %d, want 0 (must not delete after failed stop)", svc.deleteCalls)
	}
	if eventSourceRemoved {
		t.Fatal("event source must not be removed when stop/delete failed")
	}
}

func TestUninstall_FalhaAoExcluir_NaoRemoveEventSource(t *testing.T) {
	svc := &fakeService{state: StateStopped, deleteErr: errors.New("delete_denied")}
	eventSourceRemoved := false
	rc := uninstallService(&fakeConnector{svc: svc}, func(string) error {
		eventSourceRemoved = true
		return nil
	})
	if rc != 1 {
		t.Fatalf("rc = %d, want 1", rc)
	}
	if eventSourceRemoved {
		t.Fatal("event source must not be removed when delete failed (H7 ordering)")
	}
}

func TestUninstall_RemoveEventSourceApenasAposDeleteComSucesso(t *testing.T) {
	svc := &fakeService{state: StateStopped}
	order := []string{}
	rc := uninstallService(&fakeConnector{svc: svc}, func(string) error {
		order = append(order, "remove_event_source")
		return nil
	})
	if rc != 0 {
		t.Fatalf("rc = %d, want 0", rc)
	}
	if svc.deleteCalls != 1 || len(order) != 1 {
		t.Fatalf("expected exactly one delete and one event-source removal, got delete=%d order=%v",
			svc.deleteCalls, order)
	}
}

func TestUninstall_TimeoutAoAguardarParada_RetornaFalha(t *testing.T) {
	withFastStopTimeout(t)
	// stopSetsState=false: Stop() succeeds but State() never reports Stopped,
	// simulating a service stuck in StopPending until the timeout fires.
	svc := &fakeService{state: StateRunning, stopSetsState: false}
	rc := uninstallService(&fakeConnector{svc: svc}, noopRemoveEventSource)
	if rc != 1 {
		t.Fatalf("rc = %d, want 1 (must not hang forever nor delete a still-running service)", rc)
	}
	if svc.deleteCalls != 0 {
		t.Fatal("must not delete a service that never confirmed stopped")
	}
}

func TestUninstall_FechaHandleAoTerminar(t *testing.T) {
	svc := &fakeService{state: StateStopped}
	uninstallService(&fakeConnector{svc: svc}, noopRemoveEventSource)
	if svc.closeCalls != 1 {
		t.Fatalf("closeCalls = %d, want 1 (handle leak)", svc.closeCalls)
	}
}
