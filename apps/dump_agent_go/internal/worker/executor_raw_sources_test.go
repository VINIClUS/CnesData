package worker_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/cnesdata/dumpagent/internal/apiclient"
	"github.com/cnesdata/dumpagent/internal/delta"
	"github.com/cnesdata/dumpagent/internal/extractor"
	"github.com/cnesdata/dumpagent/internal/manifest"
	"github.com/cnesdata/dumpagent/internal/upload"
	"github.com/cnesdata/dumpagent/internal/worker"
	pq "github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"
)

var rawSourcePairs = map[manifest.SourceType][]string{
	manifest.SourceTypeSIHD:     {"SIHD_INTERNACAO", "SIHD_PROC_AIH"},
	manifest.SourceTypeBPAMag:   {"BPA_C", "BPA_I"},
	manifest.SourceTypeSIALocal: {"SIA_APA", "SIA_BPI", "SIA_BPIHST", "DIM_SIGTAP", "DIM_MUNICIPIO"},
}

func rawPairKey(source manifest.SourceType, subtype string) delta.SourceKey {
	return delta.SourceKey{Source: strings.ToLower(string(source)),
		Intent: strings.ToLower(subtype), Competencia: "202601"}
}

func rawScopeManifest(source manifest.SourceType, subtype string) manifest.Raw {
	return manifest.Raw{SourceType: source, FileSubtype: subtype, Competencia: "2026-01",
		SnapshotMode: manifest.SnapshotModeFull}
}

func TestEscopoRawAceitaMatrizDoEdgeECNES(t *testing.T) {
	cnes := delta.SourceKey{Source: "cnes", Intent: "profissionais", Competencia: "202601"}
	require.NoError(t, worker.ValidateRawScope(cnes,
		rawScopeManifest(manifest.SourceTypeCNESLocal, "CNES_VINCULO")))
	for source, subtypes := range rawSourcePairs {
		for _, subtype := range subtypes {
			require.NoError(t, worker.ValidateRawScope(rawPairKey(source, subtype),
				rawScopeManifest(source, subtype)), "%s/%s", source, subtype)
		}
	}
}

func TestEscopoRawRejeitaParesChavesEModosForaDoContrato(t *testing.T) {
	deltaRaw := rawScopeManifest(manifest.SourceTypeSIHD, "SIHD_INTERNACAO")
	deltaRaw.SnapshotMode = manifest.SnapshotModeDelta
	cases := map[string]struct {
		key  delta.SourceKey
		raw  manifest.Raw
		want string
	}{
		"par_trocado": {rawPairKey(manifest.SourceTypeSIHD, "BPA_C"),
			rawScopeManifest(manifest.SourceTypeSIHD, "BPA_C"), "raw_source=identity_invalid"},
		"chave_legada_sihd": {delta.SourceKey{Source: "sihd", Intent: "aih", Competencia: "202601"},
			rawScopeManifest(manifest.SourceTypeSIHD, "SIHD_INTERNACAO"),
			"raw_source=identity_invalid"},
		"chave_de_outro_subtipo": {rawPairKey(manifest.SourceTypeSIALocal, "SIA_BPI"),
			rawScopeManifest(manifest.SourceTypeSIALocal, "SIA_APA"), "raw_source=identity_invalid"},
		"cnes_nacional": {rawPairKey(manifest.SourceTypeCNESNacional, "CNES_VINCULO"),
			rawScopeManifest(manifest.SourceTypeCNESNacional, "CNES_VINCULO"),
			"raw_source=identity_invalid"},
		"competencia_divergente": {rawPairKey(manifest.SourceTypeBPAMag, "BPA_C"),
			manifest.Raw{SourceType: manifest.SourceTypeBPAMag, FileSubtype: "BPA_C",
				Competencia: "2026-02", SnapshotMode: manifest.SnapshotModeFull},
			"raw_competencia=identity_invalid"},
		"delta_fora_do_cnes": {rawPairKey(manifest.SourceTypeSIHD, "SIHD_INTERNACAO"), deltaRaw,
			"raw_snapshot_mode=unsupported"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			require.EqualError(t, worker.ValidateRawScope(tc.key, tc.raw), tc.want)
		})
	}
}

func rawSourceJobs(source manifest.SourceType, baseURL string) []*worker.Job {
	var jobs []*worker.Job
	for i, subtype := range rawSourcePairs[source] {
		id := fmt.Sprintf("%s-%d", strings.ToLower(string(source)), i)
		jobs = append(jobs, &worker.Job{ID: id, TenantID: "tenant", FencingToken: 7,
			UploadURL: baseURL + "/api/v1/edge/jobs/" + id + "/raw-object",
			Params:    extractor.ExtractionParams{Competencia: "202601"},
			RawRequest: &manifest.BuildRequest{SourceType: source, FileSubtype: subtype,
				Competencia: "2026-01", AgentID: "agent", AgentVersion: "1", SchemaVersion: "1",
				SnapshotMode: manifest.SnapshotModeFull,
				CreatedAt:    time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}})
	}
	return jobs
}

type edgeStub struct {
	mu        sync.Mutex
	objects   map[string][]byte
	manifests []manifest.Raw
}

func newEdgeStub(t *testing.T) (*edgeStub, *httptest.Server) {
	t.Helper()
	stub := &edgeStub{objects: map[string][]byte{}}
	srv := httptest.NewServer(http.HandlerFunc(stub.serve))
	t.Cleanup(srv.Close)
	return stub, srv
}

func (s *edgeStub) serve(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, "/raw-object") {
		body, _ := io.ReadAll(r.Body)
		s.objects[strings.Split(r.URL.Path, "/")[5]] = body
		return
	}
	var submission struct {
		JobID    string          `json:"job_id"`
		Manifest json.RawMessage `json:"manifest"`
	}
	var raw manifest.Raw
	if r.URL.Path != "/api/v1/edge/raw-manifests" ||
		json.NewDecoder(r.Body).Decode(&submission) != nil ||
		json.Unmarshal(submission.Manifest, &raw) != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	digest := sha256.Sum256(s.objects[submission.JobID])
	hash, err := manifest.SHA256(raw)
	if err != nil || hex.EncodeToString(digest[:]) != raw.ObjectSHA256 {
		w.WriteHeader(http.StatusUnprocessableEntity)
		return
	}
	s.manifests = append(s.manifests, raw)
	w.Header().Set("Content-Type", "application/json")
	_, _ = fmt.Fprintf(w, `{"accepted":true,"manifest_id":%q,"manifest_sha256":%q,`+
		`"full_resync_required":false,"reason":null}`, raw.ManifestID, hash)
}

func sihdRawDB(t *testing.T) *sql.DB {
	t.Helper()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	var internacao, procedimento []string
	for _, column := range extractor.SihdRawColumns("SIHD_INTERNACAO") {
		internacao = append(internacao, column.Name)
	}
	for _, column := range extractor.SihdRawColumns("SIHD_PROC_AIH") {
		procedimento = append(procedimento, column.Name)
	}
	row := sqlmock.NewRows(internacao).AddRow("3526100012345", "3541300001", int64(1),
		"2077000", "202601", "0303010037", "0303010037", "A09", nil, "20260103",
		"20260105", "01", "0", "1", "02", "F", "354130")
	mock.ExpectQuery("FROM TB_HAIH").WithArgs("202601").WillReturnRows(row)
	mock.ExpectQuery("FROM TB_HPA").WithArgs("202601").WillReturnRows(sqlmock.NewRows(procedimento))
	return db
}

func bpaRawDB(t *testing.T) *sql.DB {
	t.Helper()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	columns := []string{"PRD_UID", "PRD_CMP", "PRD_ORG", "PRD_FLH", "PRD_SEQ", "PRD_PA",
		"PRD_CBO", "PRD_CID", "PRD_IDADE", "PRD_DTATEN", "PRD_CNSMED", "PRD_QT_P"}
	for range 2 {
		mock.ExpectQuery("FROM S_PRD").WithArgs("202601", "BPI").WillReturnRows(
			sqlmock.NewRows(columns).AddRow("2077000", "202601", "BPA", "001", "01",
				"0301010064", "225125", "", "", "", "", 3.0))
		mock.ExpectQuery("FROM S_PRD").WithArgs("202601", "BPI").
			WillReturnRows(sqlmock.NewRows(columns))
	}
	return db
}

func siaRawDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	entries, err := os.ReadDir(siaFixturesDir())
	require.NoError(t, err)
	for _, entry := range entries {
		if entry.Name() == "S_BPIHST.DBF" {
			continue
		}
		data, err := os.ReadFile(filepath.Join(siaFixturesDir(), entry.Name()))
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(dir, entry.Name()), data, 0o600))
	}
	return dir
}

func parquetRows(t *testing.T, object []byte, gzipped bool) (int64, []string) {
	t.Helper()
	if gzipped {
		reader, err := gzip.NewReader(bytes.NewReader(object))
		require.NoError(t, err)
		object, err = io.ReadAll(reader)
		require.NoError(t, err)
	}
	file, err := pq.OpenFile(bytes.NewReader(object), int64(len(object)))
	require.NoError(t, err)
	var names []string
	for _, column := range file.Schema().Columns() {
		names = append(names, column[0])
	}
	return file.NumRows(), names
}

func TestFonteRawEmiteTodosOsManifestsPeloOutboxEDrainer(t *testing.T) {
	for source, subtypes := range rawSourcePairs {
		t.Run(string(source), func(t *testing.T) {
			stub, srv := newEdgeStub(t)
			exe := newRawExecutor(t)
			exe.RawUploader = upload.NewHTTP(srv.Client())
			exe.RawPayload = worker.NewRawPayloadExtractor(worker.RawSourcesConfig{
				SIHD: sihdRawDB(t), BPA: bpaRawDB(t), SIADir: siaRawDir(t)})
			ctx := context.Background()

			_, err := exe.RunRawSource(ctx, rawSourceJobs(source, srv.URL))
			require.NoError(t, err)
			adapter, err := apiclient.NewAdapter(apiclient.AdapterConfig{BaseURL: srv.URL,
				TenantID: "tenant", MachineID: "machine", HTTPClient: srv.Client()})
			require.NoError(t, err)
			drainer := worker.NewRawDrainer(adapter, exe.DeltaStore, nil)
			drainer.SpoolDirectory, drainer.Uploader = exe.RawSpoolDirectory, exe.RawUploader
			require.NoError(t, drainer.Drain(ctx, exe.RawOutbox))

			require.Len(t, stub.manifests, len(subtypes))
			for i, raw := range stub.manifests {
				require.Equal(t, source, raw.SourceType)
				require.Equal(t, subtypes[i], raw.FileSubtype)
				rows, columns := parquetRows(t, stub.objects[raw.ManifestID],
					source != manifest.SourceTypeSIHD)
				require.Equal(t, raw.RowCount, rows, raw.FileSubtype)
				if source == manifest.SourceTypeSIHD {
					require.Len(t, columns, len(extractor.SihdRawColumns(raw.FileSubtype)))
				}
				_, _, _, _, ok, err := exe.DeltaStore.ChainHead(rawPairKey(source, raw.FileSubtype))
				require.NoError(t, err)
				require.True(t, ok, raw.FileSubtype)
			}
			items, err := exe.RawOutbox.Peek(10)
			require.NoError(t, err)
			require.Empty(t, items)
		})
	}
}

func TestSlotsVaziosEDBFOpcionalAusenteViramParquetZeroRow(t *testing.T) {
	want := map[string]bool{"SIHD_PROC_AIH": true, "BPA_I": true, "SIA_BPIHST": true}
	for source := range rawSourcePairs {
		stub, srv := newEdgeStub(t)
		exe := newRawExecutor(t)
		exe.RawUploader = upload.NewHTTP(srv.Client())
		exe.RawPayload = worker.NewRawPayloadExtractor(worker.RawSourcesConfig{
			SIHD: sihdRawDB(t), BPA: bpaRawDB(t), SIADir: siaRawDir(t)})
		jobs := rawSourceJobs(source, srv.URL)
		_, err := exe.RunRawSource(context.Background(), jobs)
		require.NoError(t, err)
		for _, job := range jobs {
			subtype := job.RawRequest.FileSubtype
			require.Equal(t, want[subtype], job.RowCount == 0, subtype)
			rows, _ := parquetRows(t, stub.objects[job.ID], source != manifest.SourceTypeSIHD)
			require.Equal(t, int64(job.RowCount), rows, subtype)
			digest := sha256.Sum256(stub.objects[job.ID])
			require.Equal(t, hex.EncodeToString(digest[:]), job.Sha256, subtype)
		}
	}
}

func TestConjuntoDeJobsDaFonteIncompletoNaoEmiteNada(t *testing.T) {
	mixed := rawSourceJobs(manifest.SourceTypeSIHD, "http://edge.invalid")
	mixed[1].RawRequest.Competencia = "2026-02"
	other := rawSourceJobs(manifest.SourceTypeSIHD, "http://edge.invalid")
	other[1] = rawSourceJobs(manifest.SourceTypeBPAMag, "http://edge.invalid")[1]
	full := rawSourceJobs(manifest.SourceTypeBPAMag, "http://edge.invalid")
	cases := map[string][]*worker.Job{
		"vazio":             nil,
		"faltando_subtipo":  rawSourceJobs(manifest.SourceTypeSIALocal, "http://edge.invalid")[:4],
		"subtipo_duplicado": {full[0], full[0]},
		"competencia_mista": mixed,
		"fonte_mista":       other,
		"job_nulo":          {full[0], nil},
		"sem_raw_request":   {full[0], {ID: "legacy"}},
	}
	for name, jobs := range cases {
		t.Run(name, func(t *testing.T) {
			exe := newRawExecutor(t)
			_, err := exe.RunRawSource(context.Background(), jobs)
			require.EqualError(t, err, "raw_source_set=incomplete")
			items, err := exe.RawOutbox.Peek(10)
			require.NoError(t, err)
			require.Empty(t, items)
		})
	}
}

func TestFonteRawNaoCNESExigeExtratorDePayloadESnapshotFull(t *testing.T) {
	job := rawSourceJobs(manifest.SourceTypeSIHD, "http://edge.invalid")[0]
	exe := newRawExecutor(t)
	_, err := exe.RunRaw(context.Background(), job)
	require.EqualError(t, err, "raw_executor=unconfigured")

	exe.RawPayload = worker.NewRawPayloadExtractor(worker.RawSourcesConfig{})
	job.RawRequest.SnapshotMode = manifest.SnapshotModeDelta
	_, err = exe.RunRaw(context.Background(), job)
	require.EqualError(t, err, "raw_snapshot_mode=unsupported")
}

func TestExtratorDePayloadFalhaSemFonteConfiguradaOuComErroDeExtracao(t *testing.T) {
	extract := worker.NewRawPayloadExtractor(worker.RawSourcesConfig{SIADir: "missing"})
	for source := range rawSourcePairs {
		job := rawSourceJobs(source, "http://edge.invalid")[0]
		_, err := extract(context.Background(), *job)
		if source == manifest.SourceTypeSIALocal {
			require.ErrorContains(t, err, "sia_dir_missing")
			continue
		}
		require.EqualError(t, err, "raw_source=unconfigured source="+string(source))
	}
	job := rawSourceJobs(manifest.SourceTypeSIHD, "http://edge.invalid")[0]
	job.RawRequest.SourceType = manifest.SourceTypeCNESNacional
	_, err := extract(context.Background(), *job)
	require.EqualError(t, err, "raw_source=unsupported source=CNES_NACIONAL")
}

func siaPayloadRows[T any](t *testing.T, dir, subtype string) []T {
	t.Helper()
	job := rawSourceJobs(manifest.SourceTypeSIALocal, "http://edge.invalid")[0]
	job.RawRequest.FileSubtype = subtype
	extract := worker.NewRawPayloadExtractor(worker.RawSourcesConfig{SIADir: dir})
	payload, err := extract(context.Background(), *job)
	require.NoError(t, err)
	var buf bytes.Buffer
	require.NoError(t, payload.Write(&buf))
	reader, err := gzip.NewReader(&buf)
	require.NoError(t, err)
	plain, err := io.ReadAll(reader)
	require.NoError(t, err)
	rows := make([]T, payload.RowCount+1)
	n, _ := pq.NewGenericReader[T](bytes.NewReader(plain)).Read(rows)
	require.Equal(t, int(payload.RowCount), n)
	return rows[:n]
}

func TestFatosSIARawSaoFiltradosPelaCompetenciaDoManifest(t *testing.T) {
	bpihst := siaPayloadRows[extractor.SIABPIRow](t, siaFixturesDir(), "SIA_BPIHST")
	require.NotEmpty(t, bpihst)
	all, err := extractor.ExtractSIA(siaFixturesDir(), "202601", []string{"SIA_BPIHST"})
	require.NoError(t, err)
	require.Less(t, len(bpihst), len(all.BPIHST), "fixture mistura 202512 e 202601")
	for _, row := range bpihst {
		require.Equal(t, "202601", row.Competencia)
	}
	for _, row := range siaPayloadRows[extractor.SIABPIRow](t, siaFixturesDir(), "SIA_BPI") {
		require.Equal(t, "202601", row.Competencia)
	}
	for _, row := range siaPayloadRows[extractor.SIAAPARow](t, siaFixturesDir(), "SIA_APA") {
		require.Equal(t, "202601", row.Competencia)
	}
}
