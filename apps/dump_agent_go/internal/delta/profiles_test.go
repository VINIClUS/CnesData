package delta

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProfileFor_FiveProfilesDefined(t *testing.T) {
	combos := []struct {
		source string
		intent string
	}{
		{"cnes", "estabelecimentos"},
		{"cnes", "profissionais"},
		{"cnes", "equipes"},
		{"sihd", "aih"},
		{"bpa", "linhas"},
	}
	for _, c := range combos {
		p := ProfileFor(c.source, c.intent)
		require.Equal(t, c.source, p.Source)
		require.Equal(t, c.intent, p.Intent)
		require.NotEmpty(t, p.FingerprintColumns,
			"fp columns empty for %s/%s", c.source, c.intent)
		require.NotNil(t, p.PKExtractor)
	}
}

func TestProfileFor_FingerprintColumnsSorted(t *testing.T) {
	for _, c := range []struct{ s, i string }{
		{"cnes", "estabelecimentos"}, {"cnes", "profissionais"},
		{"cnes", "equipes"}, {"sihd", "aih"}, {"bpa", "linhas"},
	} {
		p := ProfileFor(c.s, c.i)
		copy_ := append([]string(nil), p.FingerprintColumns...)
		sort.Strings(copy_)
		require.Equal(t, copy_, p.FingerprintColumns,
			"%s/%s fp columns not sorted", c.s, c.i)
	}
}

func TestProfileFor_CNESEstabelecimentosPK(t *testing.T) {
	p := ProfileFor("cnes", "estabelecimentos")
	r := Row{"CNES": "2978319", "NOME_FANTA": "X"}
	require.Equal(t, "2978319", p.PKExtractor(r))
}

func TestProfileFor_CNESProfissionaisCompositePK(t *testing.T) {
	p := ProfileFor("cnes", "profissionais")
	r := Row{"CPF_PROF": "12345678901", "CNES": "2978319", "COD_CBO": "225155"}
	require.Equal(t, "12345678901|2978319|225155", p.PKExtractor(r))
}

func TestProfileFor_BPALinhasCompositePK(t *testing.T) {
	p := ProfileFor("bpa", "linhas")
	r := Row{"CPF": "111", "COMPETEN": "202605", "COD_PROC": "0301010029"}
	require.Equal(t, "111|202605|0301010029", p.PKExtractor(r))
}

func TestProfileFor_UnknownReturnsZero(t *testing.T) {
	p := ProfileFor("xyz", "abc")
	require.Equal(t, "", p.Source)
	require.Empty(t, p.FingerprintColumns)
	require.Nil(t, p.PKExtractor)
}
