// Package worker orquestra job execution.
package worker

import (
	"context"
	"database/sql"
	"io"

	"github.com/cnesdata/dumpagent/internal/extractor"
	"github.com/cnesdata/dumpagent/internal/obs"
	"github.com/cnesdata/dumpagent/internal/writer"
	"golang.org/x/sync/errgroup"
)

// PipelineFn executa extract+write para 1 intent, drenando para dst.
type PipelineFn func(ctx context.Context, conn *sql.Conn,
	params extractor.ExtractionParams, dst io.Writer) error

type extractFn[T any] func(
	ctx context.Context, conn *sql.Conn,
	params extractor.ExtractionParams, out chan<- T,
) error

// runPipeline constrói PipelineFn para T específico, encapsulando o row type.
func runPipeline[T any](ext extractFn[T]) PipelineFn {
	return func(ctx context.Context, conn *sql.Conn,
		params extractor.ExtractionParams, dst io.Writer) error {
		w := writer.NewParquetGzip[T](dst)
		rowCh := make(chan T, 5000)

		eg, egCtx := errgroup.WithContext(ctx)

		eg.Go(func() error {
			defer close(rowCh)
			return obs.SafeRun(func() error {
				return ext(egCtx, conn, params, rowCh)
			}, "extract:"+params.Intent)
		})

		eg.Go(func() error {
			defer func() { _ = w.Close() }()
			return obs.SafeRun(func() error {
				for row := range rowCh {
					if err := w.Write(row); err != nil {
						return err
					}
				}
				return nil
			}, "write:"+params.Intent)
		})

		return eg.Wait()
	}
}

var intentPipelines = map[string]PipelineFn{
	extractor.IntentCnesProfissionais:    runPipeline[extractor.CnesProfissionalRow](extractor.ExtractCnesProfissionais),
	extractor.IntentCnesEstabelecimentos: runPipeline[extractor.CnesEstabelecimentoRow](extractor.ExtractCnesEstabelecimentos),
	extractor.IntentCnesEquipes:          runPipeline[extractor.CnesEquipeRow](extractor.ExtractCnesEquipes),
	extractor.IntentSihdProducao:         runPipeline[extractor.SihdProducaoRow](extractor.ExtractSihdProducao),
}

// PipelineFor lookup por intent.
func PipelineFor(intent string) (PipelineFn, bool) {
	fn, ok := intentPipelines[intent]
	return fn, ok
}
