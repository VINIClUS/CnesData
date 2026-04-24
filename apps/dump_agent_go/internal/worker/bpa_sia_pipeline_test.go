package worker_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cnesdata/dumpagent/internal/upload"
	"github.com/cnesdata/dumpagent/internal/worker"
)

type registerStub struct {
	calls   int32
	lastID  string
	lastMan []worker.ManifestEntry
	err     error
}

func (r *registerStub) fn() worker.RegisterFunc {
	return func(_ context.Context, jobID string, files []worker.ManifestEntry) error {
		atomic.AddInt32(&r.calls, 1)
		r.lastID = jobID
		r.lastMan = append([]worker.ManifestEntry(nil), files...)
		return r.err
	}
}

func TestRunSIAPipeline_UploadsNFilesAndRegistersOnce(t *testing.T) {
	var uploadCalls int32
	upServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&uploadCalls, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer upServer.Close()

	reg := &registerStub{}
	job := worker.ClaimedJob{
		JobID:       "11111111-1111-1111-1111-111111111111",
		SourceType:  "SIA_LOCAL",
		Competencia: "202601",
		Files: []worker.FileManifestRef{
			{FatoSubtype: "SIA_APA", MinioKey: "sia/2026-01/apa.parquet.gz", PresignedURL: upServer.URL + "/apa"},
			{FatoSubtype: "SIA_BPI", MinioKey: "sia/2026-01/bpi.parquet.gz", PresignedURL: upServer.URL + "/bpi"},
			{FatoSubtype: "DIM_MUNICIPIO", MinioKey: "sia/2026-01/cadmun.parquet.gz", PresignedURL: upServer.URL + "/cad"},
		},
	}
	cfg := worker.SIAPipelineConfig{
		SIADir:   siaFixturesDir(),
		Uploader: upload.NewHTTP(nil),
		Register: reg.fn(),
	}
	err := worker.RunSIAPipeline(context.Background(), cfg, job)
	require.NoError(t, err)
	require.Equal(t, int32(3), atomic.LoadInt32(&uploadCalls))
	require.Equal(t, int32(1), atomic.LoadInt32(&reg.calls))
	require.Len(t, reg.lastMan, 3)
	require.Equal(t, "SIA_APA", reg.lastMan[0].FatoSubtype)
	require.Greater(t, reg.lastMan[0].SizeBytes, int64(0))
	require.Len(t, reg.lastMan[0].Sha256, 64)
}

func TestRunSIAPipeline_PropagatesUploadError(t *testing.T) {
	upServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer upServer.Close()

	reg := &registerStub{}
	cfg := worker.SIAPipelineConfig{
		SIADir:   siaFixturesDir(),
		Uploader: upload.NewHTTP(nil),
		Register: reg.fn(),
	}
	job := worker.ClaimedJob{
		JobID:      "11111111-1111-1111-1111-111111111111",
		SourceType: "SIA_LOCAL",
		Files: []worker.FileManifestRef{
			{FatoSubtype: "SIA_APA", MinioKey: "x.parquet.gz", PresignedURL: upServer.URL + "/x"},
		},
	}
	err := worker.RunSIAPipeline(context.Background(), cfg, job)
	require.Error(t, err)
	require.Equal(t, int32(0), atomic.LoadInt32(&reg.calls))
}

func TestRunSIAPipeline_UnknownSubtype(t *testing.T) {
	reg := &registerStub{}
	cfg := worker.SIAPipelineConfig{
		SIADir:   siaFixturesDir(),
		Uploader: upload.NewHTTP(nil),
		Register: reg.fn(),
	}
	job := worker.ClaimedJob{
		JobID: "11111111-1111-1111-1111-111111111111",
		Files: []worker.FileManifestRef{
			{FatoSubtype: "BOGUS", MinioKey: "x.parquet.gz", PresignedURL: "http://localhost/x"},
		},
	}
	err := worker.RunSIAPipeline(context.Background(), cfg, job)
	require.Error(t, err)
}

func TestRunSIAPipeline_NilRegister(t *testing.T) {
	upServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer upServer.Close()
	cfg := worker.SIAPipelineConfig{
		SIADir:   siaFixturesDir(),
		Uploader: upload.NewHTTP(nil),
		Register: nil,
	}
	job := worker.ClaimedJob{
		JobID:      "11111111-1111-1111-1111-111111111111",
		SourceType: "SIA_LOCAL",
		Files: []worker.FileManifestRef{
			{FatoSubtype: "SIA_APA", MinioKey: "x.parquet.gz", PresignedURL: upServer.URL + "/x"},
		},
	}
	err := worker.RunSIAPipeline(context.Background(), cfg, job)
	require.Error(t, err)
}

func TestRunSIAPipeline_RegisterError(t *testing.T) {
	upServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer upServer.Close()

	reg := &registerStub{err: errors.New("http_500")}
	cfg := worker.SIAPipelineConfig{
		SIADir:   siaFixturesDir(),
		Uploader: upload.NewHTTP(nil),
		Register: reg.fn(),
	}
	job := worker.ClaimedJob{
		JobID:      "11111111-1111-1111-1111-111111111111",
		SourceType: "SIA_LOCAL",
		Files: []worker.FileManifestRef{
			{FatoSubtype: "SIA_APA", MinioKey: "x.parquet.gz", PresignedURL: upServer.URL + "/x"},
		},
	}
	err := worker.RunSIAPipeline(context.Background(), cfg, job)
	require.Error(t, err)
}

func TestDispatch_UnknownSourceType(t *testing.T) {
	err := worker.Dispatch(context.Background(), worker.ClaimedJob{SourceType: "UNKNOWN"}, worker.DispatchConfig{})
	require.Error(t, err)
}

func TestDispatch_SIALocalRoutes(t *testing.T) {
	upServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer upServer.Close()

	reg := &registerStub{}
	cfg := worker.DispatchConfig{
		SIA: worker.SIAPipelineConfig{
			SIADir:   siaFixturesDir(),
			Uploader: upload.NewHTTP(nil),
			Register: reg.fn(),
		},
	}
	job := worker.ClaimedJob{
		JobID:      "11111111-1111-1111-1111-111111111111",
		SourceType: "SIA_LOCAL",
		Files: []worker.FileManifestRef{
			{FatoSubtype: "SIA_APA", MinioKey: "x.parquet.gz", PresignedURL: upServer.URL + "/x"},
		},
	}
	require.NoError(t, worker.Dispatch(context.Background(), job, cfg))
	require.Equal(t, int32(1), atomic.LoadInt32(&reg.calls))
}

func TestDispatch_BPARoutesWithoutFBReturnsExtractError(t *testing.T) {
	// FB connection fail → extractor error propagates; confirms dispatch wires to RunBPAPipeline.
	cfg := worker.DispatchConfig{
		BPA: worker.BPAPipelineConfig{
			GDBPath: "C:/nonexistent/bpamag.gdb",
			FBHost:  "127.0.0.1", FBPort: 59999,
			FBUser: "SYSDBA", FBPassword: "x",
		},
	}
	job := worker.ClaimedJob{
		JobID:       "11111111-1111-1111-1111-111111111111",
		SourceType:  "BPA_MAG",
		Competencia: "202601",
		Files: []worker.FileManifestRef{
			{FatoSubtype: "BPA_C", MinioKey: "x.parquet.gz", PresignedURL: "http://127.0.0.1:1/x"},
		},
	}
	err := worker.Dispatch(context.Background(), job, cfg)
	require.Error(t, err)
}

func siaFixturesDir() string {
	return filepath.Join("..", "..", "test", "integration", "fixtures", "sia_synthetic")
}
