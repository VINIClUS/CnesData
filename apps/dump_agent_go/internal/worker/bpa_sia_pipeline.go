package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/cnesdata/dumpagent/internal/extractor"
	"github.com/cnesdata/dumpagent/internal/fbdriver"
	"github.com/cnesdata/dumpagent/internal/upload"
	"github.com/cnesdata/dumpagent/internal/writer"
)

// FileManifestRef slot esperado no ClaimedJob: fato_subtype + minio_key +
// presigned URL já mintada pelo central para upload PUT.
type FileManifestRef struct {
	FatoSubtype  string
	MinioKey     string
	PresignedURL string
}

// ClaimedJob representa extração já reivindicada pelo agent, com N slots
// para upload. source_type dirige o pipeline (BPA_MAG / SIA_LOCAL / etc).
type ClaimedJob struct {
	JobID       string
	SourceType  string
	Competencia string
	Files       []FileManifestRef
}

// ManifestEntry corresponde a 1 arquivo no POST /api/v1/jobs/register.
// Campos serializam exatamente conforme contrato apiclient.FileManifest.
type ManifestEntry struct {
	MinioKey    string
	FatoSubtype string
	SizeBytes   int64
	Sha256      string
}

// RegisterFunc chama POST /api/v1/jobs/register com N-file manifest.
// Implementação real vive em apiclient (ver adapter.RegisterBPASIAJob).
// Aceita stub em teste sem forçar import de apiclient no pipeline.
type RegisterFunc func(ctx context.Context, jobID string, files []ManifestEntry) error

// BPAPipelineConfig parâmetros runtime para BPA_MAG (FB 1.5 GDB local).
type BPAPipelineConfig struct {
	GDBPath      string
	FBHost       string
	FBPort       int
	FBUser       string
	FBPassword   string
	FBClientPath string
	Uploader     upload.Uploader
	Register     RegisterFunc
}

// SIAPipelineConfig parâmetros runtime para SIA_LOCAL (DBF dir).
type SIAPipelineConfig struct {
	SIADir   string
	Uploader upload.Uploader
	Register RegisterFunc
}

// RunBPAPipeline extrai BPA_MAG, faz PUT de cada FileManifestRef e
// registra manifest no central via /jobs/register.
func RunBPAPipeline(ctx context.Context, cfg BPAPipelineConfig, job ClaimedJob) error {
	db, err := openBPADB(cfg)
	if err != nil {
		return err
	}
	defer db.Close()

	result, err := extractor.ExtractBPA(ctx, db, job.Competencia)
	if err != nil {
		return fmt.Errorf("bpa_extract: %w", err)
	}

	manifests, err := uploadBPAFiles(ctx, cfg.Uploader, job.Files, result)
	if err != nil {
		return err
	}
	return callRegister(ctx, cfg.Register, job.JobID, manifests)
}

// RunSIAPipeline extrai SIA_LOCAL, faz PUT de cada FileManifestRef e
// registra manifest no central via /jobs/register.
func RunSIAPipeline(ctx context.Context, cfg SIAPipelineConfig, job ClaimedJob) error {
	result, err := extractor.ExtractSIA(cfg.SIADir)
	if err != nil {
		return fmt.Errorf("sia_extract: %w", err)
	}
	manifests, err := uploadSIAFiles(ctx, cfg.Uploader, job.Files, result)
	if err != nil {
		return err
	}
	return callRegister(ctx, cfg.Register, job.JobID, manifests)
}

func openBPADB(cfg BPAPipelineConfig) (*sql.DB, error) {
	dsn := fbdriver.BuildDSN(fbdriver.ConnConfig{
		Host: cfg.FBHost, Port: cfg.FBPort, Path: cfg.GDBPath,
		User: cfg.FBUser, Password: cfg.FBPassword,
	})
	db, err := sql.Open("firebirdsql", dsn)
	if err != nil {
		return nil, fmt.Errorf("fb_open: %w", err)
	}
	db.SetMaxOpenConns(1)
	return db, nil
}

func uploadBPAFiles(
	ctx context.Context, up upload.Uploader, files []FileManifestRef,
	result *extractor.BPAResult,
) ([]ManifestEntry, error) {
	out := make([]ManifestEntry, 0, len(files))
	for _, f := range files {
		payload, err := serializeBPA(f.FatoSubtype, result)
		if err != nil {
			return nil, err
		}
		m, err := uploadAndManifest(ctx, up, f, payload)
		if err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, nil
}

func uploadSIAFiles(
	ctx context.Context, up upload.Uploader, files []FileManifestRef,
	result *extractor.SIAResult,
) ([]ManifestEntry, error) {
	out := make([]ManifestEntry, 0, len(files))
	for _, f := range files {
		payload, err := serializeSIA(f.FatoSubtype, result)
		if err != nil {
			return nil, err
		}
		m, err := uploadAndManifest(ctx, up, f, payload)
		if err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, nil
}

func serializeBPA(subtype string, r *extractor.BPAResult) ([]byte, error) {
	switch subtype {
	case "BPA_C":
		return writer.WriteBPACParquetGzip(r.BPA_C)
	case "BPA_I":
		return writer.WriteBPAIParquetGzip(r.BPA_I)
	}
	return nil, fmt.Errorf("unknown_bpa_subtype=%s", subtype)
}

func serializeSIA(subtype string, r *extractor.SIAResult) ([]byte, error) {
	switch subtype {
	case "SIA_APA":
		return writer.WriteSIAAPAParquetGzip(r.APA)
	case "SIA_BPI":
		return writer.WriteSIABPIParquetGzip(r.BPI)
	case "SIA_BPIHST":
		return writer.WriteSIABPIParquetGzip(r.BPIHST)
	case "DIM_SIGTAP":
		return writer.WriteCDNParquetGzip(r.CDN)
	case "DIM_MUNICIPIO":
		return writer.WriteCADMUNParquetGzip(r.CADMUN)
	}
	return nil, fmt.Errorf("unknown_sia_subtype=%s", subtype)
}

func uploadAndManifest(
	ctx context.Context, up upload.Uploader, f FileManifestRef, payload []byte,
) (ManifestEntry, error) {
	if up == nil {
		return ManifestEntry{}, errors.New("uploader_nil")
	}
	if _, err := up.Put(ctx, f.PresignedURL, bytes.NewReader(payload), "application/octet-stream"); err != nil {
		return ManifestEntry{}, fmt.Errorf("put_%s: %w", f.FatoSubtype, err)
	}
	sum := sha256.Sum256(payload)
	return ManifestEntry{
		MinioKey:    f.MinioKey,
		FatoSubtype: f.FatoSubtype,
		SizeBytes:   int64(len(payload)),
		Sha256:      hex.EncodeToString(sum[:]),
	}, nil
}

func callRegister(
	ctx context.Context, rf RegisterFunc, jobID string, files []ManifestEntry,
) error {
	if rf == nil {
		return errors.New("register_func_nil")
	}
	return rf(ctx, jobID, files)
}
