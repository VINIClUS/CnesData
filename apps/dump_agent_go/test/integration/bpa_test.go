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

	if len(result.BPA_C) == 0 {
		t.Error("BPA_C empty")
	}
	if len(result.BPA_I) == 0 {
		t.Error("BPA_I empty")
	}
}
