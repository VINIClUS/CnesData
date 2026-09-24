//go:build integration && windows

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"

	_ "github.com/nakagami/firebirdsql"

	"github.com/cnesdata/dumpagent/internal/extractor"
)

func TestBPA_SyntheticGDB(t *testing.T) {
	host := os.Getenv("FB_HOST")
	port := os.Getenv("FB_PORT")
	path := os.Getenv("BPA_GDB_PATH")

	if host == "" || port == "" || path == "" {
		t.Skip("FB_HOST / FB_PORT / BPA_GDB_PATH not set")
	}

	dsn := fmt.Sprintf("SYSDBA:masterkey@%s:%s/%s?charset=WIN1252",
		host, port, path)
	db, err := sql.Open("firebirdsql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		t.Fatalf("ping: %v", err)
	}

	result, err := extractor.ExtractBPA(context.Background(), db, "202601")
	if err != nil {
		t.Fatalf("extract: %v", err)
	}

	if len(result.BPA_C) != 2 || len(result.BPA_I) != 2 {
		t.Fatalf("counts C=%d I=%d want 2/2", len(result.BPA_C), len(result.BPA_I))
	}
	for _, row := range result.BPA_C {
		if row.Org != "BPA" || row.Quantidade == nil {
			t.Errorf("bpa_c row=%+v", row)
		}
	}
	if result.BPA_I[0].DtAtendimento != "20260115" {
		t.Errorf("dt_atendimento=%q want 20260115", result.BPA_I[0].DtAtendimento)
	}
	if result.BPA_I[1].Quantidade != nil || result.BPA_I[1].CnsProfissional != "" {
		t.Errorf("null row not preserved: %+v", result.BPA_I[1])
	}
}
