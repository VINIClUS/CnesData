package main

import (
	"context"
	"log/slog"
	"path/filepath"
	"time"

	"github.com/cnesdata/dumpagent/internal/breaker"
	"github.com/cnesdata/dumpagent/internal/obs"
	"github.com/cnesdata/dumpagent/internal/queue"
	"github.com/cnesdata/dumpagent/internal/worker"
)

// openOutboxAndStartDrain opens the bbolt outbox, constructs the breaker,
// wraps the inner client with OutboxAdapter, and spawns the drain goroutine
// + watcher. Returns (outbox, wrappedClient, true) on success;
// (nil, nil, false) on outbox open failure (caller should exit boot).
func openOutboxAndStartDrain(
	ctx context.Context,
	appData string,
	innerAPIClient worker.JobAPIClient,
) (*queue.Outbox, worker.JobAPIClient, bool) {
	outboxPath := filepath.Join(appData, "queue", "outbox.db")
	outbox, err := queue.Open(outboxPath)
	if err != nil {
		slog.Error("outbox_open_failed",
			"event_id", obs.EventQueueOpenFailed,
			"path", outboxPath,
			"err", err.Error())
		return nil, nil, false
	}
	apiBreaker := breaker.New(5, 60*time.Second, "central_api")
	wrapped := worker.NewOutboxAdapter(innerAPIClient, outbox)
	startDrainWithWatcher(ctx, outbox, apiBreaker, innerAPIClient)
	return outbox, wrapped, true
}

// startDrainWithWatcher spawns the drain goroutine and a watcher that
// relaunches it after a brief backoff if the goroutine exits with an err
// (panic recovered by obs.SafeGo). Both spawn under SafeGo so panics in
// the watcher itself are also recovered.
func startDrainWithWatcher(
	ctx context.Context,
	outbox *queue.Outbox,
	br *breaker.CircuitBreaker,
	inner worker.JobAPIClient,
) {
	startOne := func() <-chan error {
		return obs.SafeGo(func() error {
			d := worker.NewDrainer(outbox, br, inner)
			return d.Run(ctx)
		}, "outbox_drain")
	}

	_ = obs.SafeGo(func() error {
		drainCh := startOne()
		for {
			select {
			case <-ctx.Done():
				return nil
			case err, ok := <-drainCh:
				if !ok {
					return nil
				}
				if err != nil {
					slog.Error("drain_relaunch", "err", err.Error())
					select {
					case <-ctx.Done():
						return nil
					case <-time.After(5 * time.Second):
					}
					drainCh = startOne()
				}
			}
		}
	}, "drain_watcher")
}
