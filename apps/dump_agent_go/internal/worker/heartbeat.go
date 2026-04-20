package worker

import (
	"context"
	"log/slog"
	"time"
)

// HeartbeatClient envia heartbeat para o central_api.
type HeartbeatClient interface {
	SendHeartbeat(ctx context.Context, jobID string) error
}

// HeartbeatLoop envia heartbeats periódicos até ctx cancelar.
// Falhas são logadas mas nunca propagadas — lease reaper do central
// cuida de lease expiration.
func HeartbeatLoop(ctx context.Context, client HeartbeatClient, jobID string, interval time.Duration) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	fails := 0
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := client.SendHeartbeat(ctx, jobID); err != nil {
				fails++
				if fails%3 == 0 {
					slog.Warn("heartbeat_failing",
						"job_id", jobID, "consecutive_fails", fails, "err", err.Error())
				}
				continue
			}
			fails = 0
		}
	}
}
