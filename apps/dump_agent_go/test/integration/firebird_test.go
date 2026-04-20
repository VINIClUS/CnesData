//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"os"
	"testing"

	_ "github.com/nakagami/firebirdsql"

	"github.com/cnesdata/dumpagent/internal/extractor"
	"github.com/cnesdata/dumpagent/internal/fbdriver"
	"github.com/stretchr/testify/require"
)

func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := fbdriver.BuildDSN(fbdriver.ConnConfig{
		Host:     envOrDefault("FB_HOST", "localhost"),
		Port:     3050,
		Path:     envOrDefault("FB_PATH", "C:/tmp/CNES_test.gdb"),
		User:     "SYSDBA",
		Password: "masterkey",
		Charset:  "WIN1252",
	})
	db, err := sql.Open("firebirdsql", dsn)
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	return db
}

func envOrDefault(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func TestIntegration_FirebirdConnect(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()
	require.NoError(t, db.Ping())
}

func TestIntegration_ExtractEstabelecimentos(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer conn.Close()

	ch := make(chan extractor.CnesEstabelecimentoRow, 10)
	go func() {
		defer close(ch)
		err := extractor.ExtractCnesEstabelecimentos(context.Background(), conn,
			extractor.ExtractionParams{CodMunGest: "354130"}, ch)
		require.NoError(t, err)
	}()

	var rows []extractor.CnesEstabelecimentoRow
	for r := range ch {
		rows = append(rows, r)
	}
	require.Len(t, rows, 2)
	names := []string{rows[0].NomeFanta, rows[1].NomeFanta}
	require.Contains(t, names, "UBS Central")
}

func TestIntegration_WIN1252_AcentoRoundtrip(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer conn.Close()

	ch := make(chan extractor.CnesProfissionalRow, 10)
	go func() {
		defer close(ch)
		err := extractor.ExtractCnesProfissionais(context.Background(), conn,
			extractor.ExtractionParams{CodMunGest: "354130"}, ch)
		require.NoError(t, err)
	}()

	var names []string
	for r := range ch {
		names = append(names, r.NomeProf)
	}
	require.Contains(t, names, "Maria Atenção", "WIN1252 acento preservado")
}

func TestIntegration_501BugReproductor(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	// Documenta comportamento do 1-query LEFT JOIN a LFCES060.
	// Spec §3 G3 diz: pode retornar NULLs silenciosos OU erro -501.
	// Workaround 3-query é preservado regardless.
	rows, err := db.Query(`
		SELECT prof.CPF_PROF, eq.SEQ_EQUIPE
		FROM LFCES021 vinc
		INNER JOIN LFCES018 prof ON prof.PROF_ID = vinc.PROF_ID
		LEFT JOIN LFCES060 eq ON eq.COD_MUN = '354130'
	`)
	if err != nil {
		t.Logf("LEFT JOIN LFCES060 retornou erro esperado: %v", err)
		return
	}
	defer rows.Close()

	var nullCount, totalCount int
	for rows.Next() {
		var cpf, seq sql.NullString
		require.NoError(t, rows.Scan(&cpf, &seq))
		totalCount++
		if !seq.Valid {
			nullCount++
		}
	}
	t.Logf("LEFT JOIN returned: total=%d null=%d", totalCount, nullCount)
	// Documenta — não assert hard. Comportamento varia entre FB build/driver.
}
