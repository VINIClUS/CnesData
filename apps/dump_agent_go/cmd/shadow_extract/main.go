// Standalone shadow extractor — bypass central_api, output Parquet direct.
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	_ "github.com/nakagami/firebirdsql"

	"github.com/cnesdata/dumpagent/internal/extractor"
	"github.com/cnesdata/dumpagent/internal/fbdriver"
	"github.com/cnesdata/dumpagent/internal/writer"
)

func main() {
	host := flag.String("host", "localhost", "FB host")
	port := flag.Int("port", 3052, "FB port")
	path := flag.String("db", "/firebird/data/shadow.fdb", "FB db path")
	user := flag.String("user", "SYSDBA", "FB user")
	pass := flag.String("pass", "masterkey", "FB pass")
	codMun := flag.String("cod-mun", "354130", "cod_municipio_gestor")
	output := flag.String("output", "/tmp/go_shadow/", "output dir")
	flag.Parse()

	if err := run(*host, *port, *path, *user, *pass, *codMun, *output); err != nil {
		fmt.Fprintf(os.Stderr, "shadow_extract_error: %v\n", err)
		os.Exit(1)
	}
}

func run(host string, port int, path, user, pass, codMun, output string) error {
	dsn := fbdriver.BuildDSN(fbdriver.ConnConfig{
		Host: host, Port: port, Path: path,
		User: user, Password: pass, Charset: "WIN1252",
	})
	db, err := sql.Open("firebirdsql", dsn)
	if err != nil {
		return fmt.Errorf("sql.Open: %w", err)
	}
	defer db.Close()

	conn, err := db.Conn(context.Background())
	if err != nil {
		return fmt.Errorf("db.Conn: %w", err)
	}
	defer conn.Close()

	if err := os.MkdirAll(output, 0o755); err != nil {
		return err
	}

	params := extractor.ExtractionParams{
		Intent:     extractor.IntentCnesProfissionais,
		CodMunGest: codMun,
	}
	ch := make(chan extractor.CnesProfissionalRow, 1024)
	var wg sync.WaitGroup
	var writeErr error

	wg.Add(1)
	go func() {
		defer wg.Done()
		outPath := filepath.Join(output, "cnes_profissionais.parquet.gz")
		f, e := os.Create(outPath)
		if e != nil {
			writeErr = e
			for range ch {
			}
			return
		}
		defer f.Close()
		w := writer.NewParquetGzip[extractor.CnesProfissionalRow](f)
		for row := range ch {
			if e := w.Write(row); e != nil {
				writeErr = e
			}
		}
		if e := w.Close(); e != nil && writeErr == nil {
			writeErr = e
		}
	}()

	if err := extractor.ExtractCnesProfissionais(context.Background(), conn, params, ch); err != nil {
		close(ch)
		wg.Wait()
		return fmt.Errorf("ExtractCnesProfissionais: %w", err)
	}
	close(ch)
	wg.Wait()
	if writeErr != nil {
		return fmt.Errorf("write: %w", writeErr)
	}
	return nil
}
