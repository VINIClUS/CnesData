package worker

import (
	"testing"

	"github.com/cnesdata/dumpagent/internal/extractor"
)

// splitIntent must resolve both intent forms job.Params.Intent actually
// carries: bare (extractor.IntentCnes*, used by direct Job construction —
// see executor_rundelta_test.go) and prefixed (what MintUploadURL emits
// since H10, docs/edge-agent-audit-2026-09-20.md). It normalizes through
// canonicalIntent, the same helper PipelineFor/DeltaPipelineFor already use
// for this exact bidirectional tolerance, instead of duplicating that logic.
func TestSplitIntent_ResolveFormaBareEPrefixada(t *testing.T) {
	cases := []struct {
		intent, wantSource, wantBase string
	}{
		{extractor.IntentCnesProfissionais, "cnes", "profissionais"},
		{extractor.IntentCnesEstabelecimentos, "cnes", "estabelecimentos"},
		{extractor.IntentCnesEquipes, "cnes", "equipes"},
		{"cnes_profissionais", "cnes", "profissionais"},
		{"cnes_estabelecimentos", "cnes", "estabelecimentos"},
		{"cnes_equipes", "cnes", "equipes"},
	}
	for _, c := range cases {
		src, base := splitIntent(c.intent)
		if src != c.wantSource || base != c.wantBase {
			t.Errorf("splitIntent(%q) = (%q, %q), want (%q, %q)",
				c.intent, src, base, c.wantSource, c.wantBase)
		}
	}
}

// sihd_producao is the one genuine irregularity: the SIHD delta profile
// (internal/delta/profiles.go) calls its intent "aih", not "producao" — a
// generic split would silently miss the profile lookup.
func TestSplitIntent_SihdProducaoMapeiaParaAih(t *testing.T) {
	src, base := splitIntent(extractor.IntentSihdProducao)
	if src != "sihd" || base != "aih" {
		t.Errorf("splitIntent(%q) = (%q, %q), want (sihd, aih)",
			extractor.IntentSihdProducao, src, base)
	}
}

func TestDeltaKeyFromParams_UsaSplitIntent(t *testing.T) {
	p := extractor.ExtractionParams{Intent: "cnes_estabelecimentos", Competencia: "202601"}
	key := deltaKeyFromParams(p)
	if key.Source != "cnes" || key.Intent != "estabelecimentos" {
		t.Errorf("deltaKeyFromParams(%+v) = %+v, want Source=cnes Intent=estabelecimentos", p, key)
	}
}
