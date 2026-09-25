package worker

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/cnesdata/dumpagent/internal/delta"
	"github.com/cnesdata/dumpagent/internal/extractor"
	"github.com/cnesdata/dumpagent/internal/manifest"
	"github.com/cnesdata/dumpagent/internal/writer"
)

// RawPayload é o snapshot FULL serializado de um subtipo raw fora do CNES.
type RawPayload struct {
	RowCount int64
	Write    func(io.Writer) error
}

// RawSourcesConfig reúne as origens locais lidas pelo caminho raw de SIHD, BPA e SIA.
type RawSourcesConfig struct {
	SIHD   extractor.SihdQueryer
	BPA    *sql.DB
	SIADir string
}

// NewRawPayloadExtractor despacha a extração raw pelo par (source_type, file_subtype).
//
// Returns: função para JobExecutor.RawPayload.
// Raises (na chamada): raw_source=unconfigured, raw_source=unsupported, erros do extrator.
func NewRawPayloadExtractor(cfg RawSourcesConfig) func(context.Context, Job) (RawPayload, error) {
	return func(ctx context.Context, job Job) (RawPayload, error) {
		request := job.RawRequest
		switch request.SourceType {
		case manifest.SourceTypeSIHD:
			if cfg.SIHD == nil {
				return RawPayload{}, unconfiguredRawSource(request.SourceType)
			}
			return sihdRawPayload(ctx, cfg.SIHD, request)
		case manifest.SourceTypeBPAMag:
			if cfg.BPA == nil {
				return RawPayload{}, unconfiguredRawSource(request.SourceType)
			}
			return bpaRawPayload(ctx, cfg.BPA, request)
		case manifest.SourceTypeSIALocal:
			return siaRawPayload(cfg.SIADir, request)
		}
		return RawPayload{}, fmt.Errorf("raw_source=unsupported source=%s", request.SourceType)
	}
}

func unconfiguredRawSource(source manifest.SourceType) error {
	return fmt.Errorf("raw_source=unconfigured source=%s", source)
}

func sihdRawPayload(
	ctx context.Context, db extractor.SihdQueryer, request *manifest.BuildRequest,
) (RawPayload, error) {
	rows, err := extractor.ExtractSihdRaw(ctx, db, request.Competencia, request.FileSubtype)
	if err != nil {
		return RawPayload{}, err
	}
	var columns []writer.RawColumn
	for _, column := range extractor.SihdRawColumns(request.FileSubtype) {
		kind := writer.RawText
		if column.Kind == extractor.SihdInteger {
			kind = writer.RawInt64
		}
		columns = append(columns, writer.RawColumn{Name: column.Name, Kind: kind})
	}
	return RawPayload{RowCount: int64(len(rows)), Write: func(dst io.Writer) error {
		return writer.WriteRawTableParquet(dst, columns, rows)
	}}, nil
}

func bpaRawPayload(
	ctx context.Context, db *sql.DB, request *manifest.BuildRequest,
) (RawPayload, error) {
	result, err := extractor.ExtractBPA(ctx, db, compactCompetencia(request.Competencia))
	if err != nil {
		return RawPayload{}, fmt.Errorf("bpa_extract: %w", err)
	}
	rows := len(result.BPA_C)
	if request.FileSubtype == "BPA_I" {
		rows = len(result.BPA_I)
	}
	payload, err := serializeBPA(request.FileSubtype, result)
	return bytesRawPayload(rows, payload, err)
}

func siaRawPayload(dir string, request *manifest.BuildRequest) (RawPayload, error) {
	result, err := extractor.ExtractSIA(dir, request.Competencia, []string{request.FileSubtype})
	if err != nil {
		return RawPayload{}, fmt.Errorf("sia_extract: %w", err)
	}
	rows := map[string]int{
		"SIA_APA": len(result.APA), "SIA_BPI": len(result.BPI),
		"SIA_BPIHST": len(result.BPIHST), "DIM_SIGTAP": len(result.SIGTAP),
		"DIM_MUNICIPIO": len(result.CADMUN),
	}[request.FileSubtype]
	payload, err := serializeSIA(request.FileSubtype, result)
	return bytesRawPayload(rows, payload, err)
}

// BPA/SIA reusam o Parquet gzip do caminho legado, já lido pelos processors SRC-011/012.
func bytesRawPayload(rows int, payload []byte, err error) (RawPayload, error) {
	if err != nil {
		return RawPayload{}, err
	}
	return RawPayload{RowCount: int64(rows), Write: func(dst io.Writer) error {
		_, err := dst.Write(payload)
		return err
	}}, nil
}

func compactCompetencia(competencia string) string {
	return strings.ReplaceAll(competencia, "-", "")
}

// RunRawSource emite, pelo outbox durável, um manifest por subtipo exigido da fonte.
//
// Args: jobs com RawRequest cobrindo exatamente manifest.RawSubtypes da fonte e competência.
// Returns: bytes enviados somados.
// Raises: raw_source_set=incomplete, erros de RunRaw com raw_source_subtype.
func (e *JobExecutor) RunRawSource(ctx context.Context, jobs []*Job) (int64, error) {
	if err := validateRawSourceSet(jobs); err != nil {
		return 0, err
	}
	var total int64
	for _, job := range jobs {
		size, err := e.RunRaw(ctx, job)
		if err != nil {
			return total, fmt.Errorf("raw_source_subtype=%s: %w", job.RawRequest.FileSubtype, err)
		}
		total += size
	}
	return total, nil
}

func validateRawSourceSet(jobs []*Job) error {
	incomplete := errors.New("raw_source_set=incomplete")
	if len(jobs) == 0 || jobs[0] == nil || jobs[0].RawRequest == nil {
		return incomplete
	}
	first := jobs[0].RawRequest
	subtypes := make([]string, 0, len(jobs))
	for _, job := range jobs {
		if job == nil || job.RawRequest == nil || job.RawRequest.SourceType != first.SourceType ||
			job.RawRequest.Competencia != first.Competencia {
			return incomplete
		}
		subtypes = append(subtypes, job.RawRequest.FileSubtype)
	}
	want := manifest.RawSubtypes(first.SourceType)
	slices.Sort(subtypes)
	slices.Sort(want)
	if !slices.Equal(subtypes, want) {
		return incomplete
	}
	return nil
}

// CNES mantém a chave histórica cnes/profissionais; as demais fontes usam o par em minúsculas,
// disjunto das chaves do caminho delta legado (ex.: sihd/aih).
func rawPairKey(sourceType manifest.SourceType, fileSubtype string) (source, intent string) {
	if sourceType == manifest.SourceTypeCNESLocal {
		return "cnes", "profissionais"
	}
	return strings.ToLower(string(sourceType)), strings.ToLower(fileSubtype)
}

func rawSourceKey(job *Job) delta.SourceKey {
	request := job.RawRequest
	if request.SourceType == manifest.SourceTypeCNESLocal {
		return deltaKeyFromParams(job.Params)
	}
	source, intent := rawPairKey(request.SourceType, request.FileSubtype)
	return delta.SourceKey{Source: source, Intent: intent, Competencia: job.Params.Competencia}
}

func validateRawScope(key delta.SourceKey, raw manifest.Raw) error {
	source, intent := rawPairKey(raw.SourceType, raw.FileSubtype)
	if !manifest.ValidRawPair(raw.SourceType, raw.FileSubtype) ||
		key.Source != source || key.Intent != intent {
		return errors.New("raw_source=identity_invalid")
	}
	if raw.SourceType != manifest.SourceTypeCNESLocal &&
		raw.SnapshotMode != manifest.SnapshotModeFull {
		return errors.New("raw_snapshot_mode=unsupported")
	}
	layout := "200601"
	if len(key.Competencia) == 7 {
		layout = "2006-01"
	}
	month, err := time.Parse(layout, key.Competencia)
	if err != nil || month.Format("2006-01") != raw.Competencia {
		return errors.New("raw_competencia=identity_invalid")
	}
	return nil
}

func (e *JobExecutor) rawExtractorConfigured(job *Job) bool {
	if job.RawRequest.SourceType == manifest.SourceTypeCNESLocal {
		return e.RawExtract != nil
	}
	return e.RawPayload != nil
}

func (e *JobExecutor) prepareRawPayload(
	ctx context.Context, job *Job, cycle rawCycle,
) (rawCycle, error) {
	payload, err := e.RawPayload(ctx, *job)
	if err != nil {
		return cycle, err
	}
	cycle.payload = &payload
	cycle.hashes = map[string][32]byte{}
	cycle.request.RowCount = payload.RowCount
	return cycle, nil
}
