package worker

import (
	"context"
	"fmt"

	"github.com/google/uuid"
)

// StaticSpec pré-define um único JobSpec a ser emitido a cada call.
// Cada Next() gera novo JobID (UUID v4) para evitar colisão em /jobs/register.
type StaticSpec struct {
	FonteSistema string
	TipoExtracao string
	Competencia  int
	Intent       string
}

// NewStaticSource cria JobSpecSource que sempre retorna o mesmo JobSpec
// mas com JobID novo por chamada. Útil para edge agent que roda 1 extracao
// por execução via env vars.
func NewStaticSource(s StaticSpec) JobSpecSource {
	return &staticSource{spec: s}
}

type staticSource struct {
	spec StaticSpec
}

func (s *staticSource) Next(_ context.Context) (*JobSpec, error) {
	return &JobSpec{
		JobID:        uuid.NewString(),
		FonteSistema: s.spec.FonteSistema,
		TipoExtracao: s.spec.TipoExtracao,
		Competencia:  s.spec.Competencia,
		Intent:       s.spec.Intent,
	}, nil
}

// DispatchConfig agrega configs específicas de cada source_type.
// Apenas BPA_MAG e SIA_LOCAL têm dispatch aqui — CNES_LOCAL/SIHD seguem
// pelo JobExecutor clássico (intentPipelines).
type DispatchConfig struct {
	BPA BPAPipelineConfig
	SIA SIAPipelineConfig
}

// Dispatch roteia ClaimedJob para o pipeline apropriado por source_type.
// Retorna erro para source_type desconhecido (CNES/SIHD não passam aqui).
func Dispatch(ctx context.Context, job ClaimedJob, cfg DispatchConfig) error {
	switch job.SourceType {
	case "BPA_MAG":
		return RunBPAPipeline(ctx, cfg.BPA, job)
	case "SIA_LOCAL":
		return RunSIAPipeline(ctx, cfg.SIA, job)
	}
	return fmt.Errorf("unsupported_source_type=%s", job.SourceType)
}
