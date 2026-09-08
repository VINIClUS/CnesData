package manifest

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const goldenSHA256 = "9c6005f90bbc5af3bcb3c5469474e44ea895e4e2e8ef27e014e0933e45af0e99"

func TestManifestoRawCorrespondeAoGoldenPython(t *testing.T) {
	want, err := os.ReadFile("../../../../docs/fixtures/data-plane/raw-manifest-v1.json")
	require.NoError(t, err)
	require.Len(t, want, 630)

	got, err := Build(goldenRequest())
	require.NoError(t, err)
	require.Equal(t, "fixture-cnes-nacional-v1", got.ManifestID)
	require.Equal(t, got.ManifestID, got.SnapshotID)
	require.Nil(t, got.BaseSnapshotID)
	require.Nil(t, got.PreviousManifestSHA256)
	require.Equal(t, uint32(1), got.Sequence)

	payload, err := CanonicalJSON(got)
	require.NoError(t, err)
	require.Equal(t, want, payload)

	digest, err := SHA256(got)
	require.NoError(t, err)
	require.Equal(t, goldenSHA256, digest)
}

func TestConstrucaoDeltaCarregaBaseSequenciaEHashDoServidor(t *testing.T) {
	serverHash := strings.Repeat("b", 64)
	request := goldenRequest()
	request.JobID = "job-delta-2"
	request.SnapshotMode = SnapshotModeDelta
	request.Previous = &PreviousHead{
		SnapshotID:     "job-full-1",
		Sequence:       7,
		ManifestSHA256: serverHash,
	}

	got, err := Build(request)

	require.NoError(t, err)
	require.Equal(t, "job-delta-2", got.ManifestID)
	require.Equal(t, "job-delta-2", got.SnapshotID)
	require.Equal(t, "job-full-1", *got.BaseSnapshotID)
	require.Equal(t, uint32(8), got.Sequence)
	require.Equal(t, serverHash, *got.PreviousManifestSHA256)
	require.Equal(t,
		"raw/354130/CNES_NACIONAL/2026-01/job-delta-2/data.parquet",
		got.ObjectKey,
	)
}

func TestConstrucaoRejeitaContratoRawInvalido(t *testing.T) {
	tests := map[string]func(*BuildRequest){
		"identificador vazio": func(r *BuildRequest) { r.JobID = "" },
		"fonte desconhecida":  func(r *BuildRequest) { r.SourceType = "OUTRA" },
		"subtipo vazio":       func(r *BuildRequest) { r.FileSubtype = "" },
		"competencia invalida": func(r *BuildRequest) {
			r.Competencia = "2026-13"
		},
		"agente vazio":      func(r *BuildRequest) { r.AgentID = "" },
		"versao vazia":      func(r *BuildRequest) { r.AgentVersion = "" },
		"schema vazio":      func(r *BuildRequest) { r.SchemaVersion = "" },
		"modo desconhecido": func(r *BuildRequest) { r.SnapshotMode = "OUTRO" },
		"hash invalido":     func(r *BuildRequest) { r.ObjectSHA256 = strings.Repeat("A", 64) },
		"linhas negativas":  func(r *BuildRequest) { r.RowCount = -1 },
		"tamanho vazio":     func(r *BuildRequest) { r.SizeBytes = 0 },
		"timestamp fora de UTC": func(r *BuildRequest) {
			r.CreatedAt = time.Date(2026, 2, 1, 0, 0, 0, 0, time.FixedZone("BRT", -3*60*60))
		},
	}

	for name, change := range tests {
		t.Run(name, func(t *testing.T) {
			request := goldenRequest()
			change(&request)

			_, err := Build(request)

			require.Error(t, err)
		})
	}
}

func TestDeltaExigeCabecaAnteriorValida(t *testing.T) {
	tests := map[string]*PreviousHead{
		"ausente":        nil,
		"snapshot vazio": {Sequence: 1, ManifestSHA256: strings.Repeat("a", 64)},
		"sequencia zero": {SnapshotID: "job-full-1", ManifestSHA256: strings.Repeat("a", 64)},
		"hash invalido":  {SnapshotID: "job-full-1", Sequence: 1, ManifestSHA256: "abc"},
		"sequencia maxima": {
			SnapshotID: "job-full-1", Sequence: ^uint32(0), ManifestSHA256: strings.Repeat("a", 64),
		},
	}

	for name, previous := range tests {
		t.Run(name, func(t *testing.T) {
			request := goldenRequest()
			request.SnapshotMode = SnapshotModeDelta
			request.Previous = previous

			_, err := Build(request)

			require.Error(t, err)
		})
	}
}

func TestManifestoRawRejeitaNomeDeObjetoDiferenteDeDataParquet(t *testing.T) {
	raw, err := Build(goldenRequest())
	require.NoError(t, err)
	raw.ObjectKey = strings.Replace(raw.ObjectKey, "data.parquet", "outro.parquet", 1)
	operations := map[string]func(Raw) error{
		"json canonico": func(raw Raw) error {
			_, err := CanonicalJSON(raw)
			return err
		},
		"sha256": func(raw Raw) error {
			_, err := SHA256(raw)
			return err
		},
	}

	for name, operation := range operations {
		t.Run(name, func(t *testing.T) {
			require.EqualError(t, operation(raw), "field=object_key_filename")
		})
	}
}

func goldenRequest() BuildRequest {
	return BuildRequest{
		JobID:         "fixture-cnes-nacional-v1",
		TenantID:      "354130",
		SourceType:    SourceTypeCNESNacional,
		FileSubtype:   "CNES_VINCULO",
		Competencia:   "2026-01",
		AgentID:       "system-datasus",
		AgentVersion:  "1.0.0",
		SchemaVersion: "cnes-profissional-v1",
		SnapshotMode:  SnapshotModeFull,
		ObjectSHA256:  "30a60a84af9b06c568fe6170ce72365ee33cb1a706f130cc5bc7e197aef348ec",
		RowCount:      5,
		SizeBytes:     5123,
		CreatedAt:     time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC),
	}
}

func TestJSONCanonicoPreservaMicrossegundosESeparadoresUnicodeDoPython(t *testing.T) {
	request := goldenRequest()
	request.CreatedAt = request.CreatedAt.Add(100000 * time.Microsecond)
	request.AgentVersion = "line\u2028paragraph\u2029 literal\\u2028"
	raw, err := Build(request)
	require.NoError(t, err)
	want, err := os.ReadFile("../../../../docs/fixtures/data-plane/raw-manifest-v1.json")
	require.NoError(t, err)
	expected := strings.Replace(string(want), `"agent_version":"1.0.0"`,
		"\"agent_version\":\"line\u2028paragraph\u2029 literal\\\\u2028\"", 1)
	expected = strings.Replace(expected, "00:00:00Z", "00:00:00.100000Z", 1)
	payload, err := CanonicalJSON(raw)
	require.NoError(t, err)
	require.Equal(t, expected, string(payload))
	hash, err := SHA256(raw)
	require.NoError(t, err)
	require.Equal(t, "8db9c1d06145a284d24c9cea25463666b5de83b47ed349a4b68f48f1509c9100", hash)
}

func TestManifestoRejeitaPrecisaoInferiorAMicrossegundo(t *testing.T) {
	request := goldenRequest()
	request.CreatedAt = request.CreatedAt.Add(time.Nanosecond)
	_, err := Build(request)
	require.Error(t, err)
	raw, err := Build(goldenRequest())
	require.NoError(t, err)
	raw.CreatedAt = raw.CreatedAt.Add(time.Nanosecond)
	_, err = CanonicalJSON(raw)
	require.Error(t, err)
}
