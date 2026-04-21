package obs

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v4"
)

// BackoffConfig agrega parâmetros exponential backoff.
type BackoffConfig struct {
	InitialInterval time.Duration
	MaxInterval     time.Duration
	MaxElapsedTime  time.Duration
}

// DefaultBackoffConfig valores usados pela maioria dos call sites.
func DefaultBackoffConfig() BackoffConfig {
	return BackoffConfig{
		InitialInterval: 1 * time.Second,
		MaxInterval:     60 * time.Second,
		MaxElapsedTime:  5 * time.Minute,
	}
}

// Permanent marca erro como não-retryable pelo WithBackoff.
func Permanent(err error) error {
	return backoff.Permanent(err)
}

// WithBackoff executa op com retry exponencial + jitter. Aborta em:
// ctx cancelado, erro marcado Permanent, ou MaxElapsedTime excedido.
func WithBackoff(ctx context.Context, op func() error, cfg BackoffConfig, scope string) error {
	b := backoff.WithContext(
		backoff.NewExponentialBackOff(
			backoff.WithInitialInterval(cfg.InitialInterval),
			backoff.WithMaxInterval(cfg.MaxInterval),
			backoff.WithMaxElapsedTime(cfg.MaxElapsedTime),
		),
		ctx,
	)
	return backoff.Retry(op, b)
}
