package worker

import (
	"context"
	"log/slog"
	"math/rand"
	"time"

	"github.com/cnesdata/dumpagent/internal/obs"
)

// JobAPIClient contrato que Consumer precisa.
// v2: agent registra extraction em vez de long-poll por jobs prontos.
type JobAPIClient interface {
	RegisterJob(ctx context.Context, spec JobSpec) (*Job, error)
	CompleteJob(ctx context.Context, job Job, sizeBytes int64) error
	FailJob(ctx context.Context, job Job, cause error) error
	HeartbeatClient
}

// JobExecutorIface permite injetar stub em testes.
type JobExecutorIface interface {
	Run(ctx context.Context, job Job) (int64, error)
}

// JobSpecSource produz o próximo JobSpec a ser registrado, ou nil/err.
// Retornar (nil,nil) significa "sem trabalho agora" — consumer aguarda PollInterval.
type JobSpecSource interface {
	Next(ctx context.Context) (*JobSpec, error)
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
	source   JobSpecSource
	executor JobExecutorIface
	config   ConsumerConfig
}

// NewConsumer construtor.
func NewConsumer(api JobAPIClient, source JobSpecSource, executor JobExecutorIface, cfg ConsumerConfig) *Consumer {
	return &Consumer{api: api, source: source, executor: executor, config: cfg}
}

// Loop executa next_spec → register → exec → complete/fail repetidamente.
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

		spec, specErr := c.source.Next(ctx)
		if specErr != nil {
			slog.Warn("source_next_failed", "err", specErr.Error())
			c.sleep(ctx, c.config.PollInterval)
			continue
		}
		if spec == nil {
			c.sleep(ctx, c.config.PollInterval)
			continue
		}

		job, regErr := c.api.RegisterJob(ctx, *spec)
		if regErr != nil {
			slog.Warn("register_failed", "err", regErr.Error())
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
