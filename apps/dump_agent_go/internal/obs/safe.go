package obs

import (
	"fmt"
	"log/slog"
	"runtime/debug"
)

// SafeRun executa fn síncrono, convertendo qualquer panic em error.
// Log ERROR com scope + stack trace emitido em caso de panic.
func SafeRun(fn func() error, scope string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			stack := debug.Stack()
			slog.Error("panic_recovered",
				"scope", scope,
				"panic", fmt.Sprintf("%v", r),
				"stack", string(stack),
			)
			err = fmt.Errorf("panic in %s: %v", scope, r)
		}
	}()
	return fn()
}

// SafeGo spawn goroutine com recover, retorna channel emitindo erro final.
// Channel é fechado após goroutine terminar.
func SafeGo(fn func() error, scope string) <-chan error {
	errCh := make(chan error, 1)
	go func() {
		defer close(errCh)
		if err := SafeRun(fn, scope); err != nil {
			errCh <- err
		}
	}()
	return errCh
}
