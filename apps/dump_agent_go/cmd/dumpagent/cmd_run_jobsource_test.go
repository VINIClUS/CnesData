package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// central_api's _FATO_SUBTYPE_FOR (apps/central_api/src/central_api/routes/
// jobs.py) only recognizes intent keys prefixed with the source: this is
// the exact string it must equal, not just "non-empty" — the previous
// unprefixed default ("estabelecimentos") 422'd on every upload-url mint
// attempt on a fresh install (H10, docs/edge-agent-audit-2026-09-20.md).
const expectedDefaultIntent = "cnes_estabelecimentos"

func TestBuildJobSource_IntentPadraoCasaComCentralAPI(t *testing.T) {
	t.Setenv("COMPETENCIA_YYYYMM", "202601")
	t.Setenv("INTENT", "")
	t.Setenv("TIPO_EXTRACAO", "")
	t.Setenv("FONTE_SISTEMA", "")

	src, err := buildJobSource()
	require.NoError(t, err)

	spec, err := src.Next(context.Background())
	require.NoError(t, err)
	require.Equal(t, expectedDefaultIntent, spec.Intent)
	require.Equal(t, expectedDefaultIntent, spec.TipoExtracao)
	require.Equal(t, "CNES_LOCAL", spec.FonteSistema)
}

func TestBuildJobSource_IntentHonraOverrideDeAmbiente(t *testing.T) {
	t.Setenv("COMPETENCIA_YYYYMM", "202601")
	t.Setenv("INTENT", "cnes_profissionais")

	src, err := buildJobSource()
	require.NoError(t, err)

	spec, err := src.Next(context.Background())
	require.NoError(t, err)
	require.Equal(t, "cnes_profissionais", spec.Intent)
}

func TestBuildJobSource_CompetenciaAusente_RetornaErro(t *testing.T) {
	t.Setenv("COMPETENCIA_YYYYMM", "")

	_, err := buildJobSource()
	require.Error(t, err)
}
