package worker

import (
	"context"

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
