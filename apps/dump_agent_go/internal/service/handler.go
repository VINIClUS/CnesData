//go:build windows

package service

import (
	"context"
	"log/slog"

	"golang.org/x/sys/windows/svc"
)

// ServiceName nome registrado no SCM.
const ServiceName = "CnesDumpAgent"

// DisplayName nome exibido no Services.msc.
const DisplayName = "CnesData Edge Agent"

type handler struct {
	version string
}

func (h *handler) Execute(args []string, r <-chan svc.ChangeRequest, s chan<- svc.Status) (bool, uint32) {
	const accepts = svc.AcceptStop | svc.AcceptShutdown

	s <- svc.Status{State: svc.StartPending}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan int, 1)

	go func() {
		done <- runnerFn(ctx, false)
	}()

	s <- svc.Status{State: svc.Running, Accepts: accepts}
	slog.Info("service_started", "version", h.version)

	for {
		select {
		case req := <-r:
			switch req.Cmd {
			case svc.Interrogate:
				s <- req.CurrentStatus
			case svc.Stop, svc.Shutdown:
				slog.Info("service_stop_received")
				s <- svc.Status{State: svc.StopPending}
				cancel()
				exitCode := <-done
				s <- svc.Status{State: svc.Stopped}
				if exitCode != 0 {
					return false, uint32(exitCode)
				}
				return false, 0
			}
		case exitCode := <-done:
			if exitCode != 0 {
				slog.Error("runner_exited_non_zero", "code", exitCode)
				s <- svc.Status{State: svc.Stopped}
				return false, uint32(exitCode)
			}
			s <- svc.Status{State: svc.Stopped}
			return false, 0
		}
	}
}

var runnerFn = func(_ context.Context, _ bool) int { return 0 }

// SetRunner permite main package injetar runForeground (evita import cycle).
func SetRunner(fn func(ctx context.Context, verbose bool) int) {
	runnerFn = fn
}

// RunAsService invocada pelo SCM. svc.Run bloqueia até serviço parar.
func RunAsService(version string) int {
	if err := svc.Run(ServiceName, &handler{version: version}); err != nil {
		slog.Error("svc_run_failed", "err", err.Error())
		return 1
	}
	return 0
}
