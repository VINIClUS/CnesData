package worker

import (
	"context"
	"log/slog"
	"math/rand"
	"time"

	"github.com/cnesdata/dumpagent/internal/obs"
)

// JobAPIClient contrato que Consumer precisa.
type JobAPIClient interface {
	AcquireJob(ctx context.Context) (*Job, error)
	CompleteJob(ctx context.Context, job Job, sizeBytes int64) error
	FailJob(ctx context.Context, job Job, cause error) error
	HeartbeatClient
}

// JobExecutorIface permite injetar stub em testes.
type JobExecutorIface interface {
	Run(ctx context.Context, job Job) (int64, error)
}

// ConsumerConfig tunáveis do loop.
type ConsumerConfig struct {
	PollInterval      time.Duration
	InterJobJitterMax time.Duration
	HeartbeatInterval time.Duration
}

// Consumer loop principal.
type Consumer struct {
	api      JobAPIClient
	executor JobExecutorIface
	config   ConsumerConfig
}

// NewConsumer construtor.
func NewConsumer(api JobAPIClient, executor JobExecutorIface, cfg ConsumerConfig) *Consumer {
	return &Consumer{api: api, executor: executor, config: cfg}
}

// Loop executa acquire → exec → complete/fail repetidamente.
// Exit em ctx.Done(). Panic recovered.
func (c *Consumer) Loop(ctx context.Context) (err error) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("consumer_panic", "panic", r)
			err = nil
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		job, acqErr := c.api.AcquireJob(ctx)
		if acqErr != nil {
			slog.Warn("acquire_failed", "err", acqErr.Error())
			c.sleep(ctx, c.config.PollInterval)
			continue
		}
		if job == nil {
			c.sleep(ctx, c.config.PollInterval)
			continue
		}

		c.processJob(ctx, *job)
		c.sleepJitter(ctx)
	}
}

func (c *Consumer) processJob(ctx context.Context, job Job) {
	jobCtx, jobCancel := context.WithCancel(ctx)
	defer jobCancel()

	hbCh := obs.SafeGo(func() error {
		return HeartbeatLoop(jobCtx, c.api, job.ID, c.config.HeartbeatInterval)
	}, "heartbeat")

	size, execErr := c.executor.Run(jobCtx, job)
	jobCancel()
	<-hbCh

	if execErr != nil {
		if err := c.api.FailJob(ctx, job, execErr); err != nil {
			slog.Error("fail_job_api_error", "job_id", job.ID, "err", err.Error())
		}
		return
	}
	if err := c.api.CompleteJob(ctx, job, size); err != nil {
		slog.Error("complete_job_api_error", "job_id", job.ID, "err", err.Error())
	}
}

func (c *Consumer) sleep(ctx context.Context, d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

func (c *Consumer) sleepJitter(ctx context.Context) {
	if c.config.InterJobJitterMax <= 0 {
		return
	}
	n := rand.Int63n(int64(c.config.InterJobJitterMax))
	c.sleep(ctx, time.Duration(n))
}
