package discover

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWriteYAML_TopUncommentedAlternatesAsComments(t *testing.T) {
	results := []SourceResult{
		{
			Source: SourceCNES,
			Top:    Candidate{Path: `C:\A\CNES.GDB`, Score: 80, Strategy: StrategyRegistry},
			Alternates: []Candidate{
				{Path: `D:\B\CNES.GDB`, Score: 60, Strategy: StrategyFSTemplate},
			},
		},
		{Source: SourceSIHD},
		{Source: SourceBPA},
		{
			Source: SourceSIA,
			Top:    Candidate{Path: `C:\SIA`, Score: 70, Strategy: StrategyFSTemplate},
		},
	}
	var buf bytes.Buffer
	require.NoError(t, WriteYAML(&buf, results))
	out := buf.String()
	require.Contains(t, out, "cnes:")
	require.Contains(t, out, `database_path: C:\A\CNES.GDB`)
	require.Contains(t, out, "# candidate strategy=fs_template")
	require.Contains(t, out, `D:\B\CNES.GDB`)
	require.Contains(t, out, "(no candidates found")
	require.Contains(t, out, "sia:")
	require.Contains(t, out, `dbf_dir: C:\SIA`)
}

func TestWriteYAML_AtomicRename(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "config.yaml")
	results := []SourceResult{
		{Source: SourceCNES,
			Top: Candidate{Path: `C:\X.GDB`, Score: 80, Strategy: StrategyRegistry}},
		{Source: SourceSIHD}, {Source: SourceBPA}, {Source: SourceSIA},
	}
	require.NoError(t, WriteYAMLAtomic(out, results))
	data, err := os.ReadFile(out)
	require.NoError(t, err)
	require.Contains(t, string(data), `C:\X.GDB`)
	tmpEntries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, e := range tmpEntries {
		require.NotContains(t, e.Name(), ".tmp",
			"atomic rename should leave no .tmp behind")
	}
}

func TestLoadYAML_HappyPath(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "config.yaml")
	yaml := `
cnes:
  host: localhost
  port: 3050
  database_path: C:\X.GDB
  user: SYSDBA
  charset: WIN1252
sihd:
  host: localhost
  port: 3050
  database_path: ""
  user: SYSDBA
  charset: WIN1252
bpa:
  host: localhost
  port: 3050
  database_path: C:\BPAMAG.GDB
  user: SYSDBA
  charset: WIN1252
sia:
  dbf_dir: C:\SIA
`
	require.NoError(t, os.WriteFile(out, []byte(yaml), 0o644))
	cfg, err := LoadYAML(out)
	require.NoError(t, err)
	require.Equal(t, `C:\X.GDB`, cfg.CNES.DatabasePath)
	require.Equal(t, 3050, cfg.CNES.Port)
	require.Equal(t, `C:\SIA`, cfg.SIA.DBFDir)
}

func TestLoadYAML_NotFoundReturnsErrNoYAML(t *testing.T) {
	_, err := LoadYAML(filepath.Join(t.TempDir(), "missing.yaml"))
	require.True(t, errors.Is(err, ErrNoYAML))
}

func TestLoadYAML_TypeMismatchSurfacesError(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "config.yaml")
	yaml := "cnes:\n  port: \"not an int\"\n"
	require.NoError(t, os.WriteFile(out, []byte(yaml), 0o644))
	_, err := LoadYAML(out)
	require.Error(t, err)
	require.False(t, errors.Is(err, ErrNoYAML))
}

func TestLoadYAML_UnknownTopLevelKeyRejected(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "config.yaml")
	yaml := "extra_key: foo\ncnes:\n  host: localhost\n"
	require.NoError(t, os.WriteFile(out, []byte(yaml), 0o644))
	_, err := LoadYAML(out)
	require.Error(t, err)
}

func TestWriteYAML_FBEmptyTopShowsHint(t *testing.T) {
	results := []SourceResult{
		{Source: SourceCNES},
		{Source: SourceSIHD},
		{Source: SourceBPA},
		{Source: SourceSIA},
	}
	var buf bytes.Buffer
	require.NoError(t, WriteYAML(&buf, results))
	out := buf.String()
	require.Contains(t, out, "no candidates found")
	require.Contains(t, out, "CNES_DB_PATH")
	require.Contains(t, out, "SIA_DIR")
}

func TestWriteYAML_AlternatesSorted(t *testing.T) {
	results := []SourceResult{
		{
			Source: SourceCNES,
			Top:    Candidate{Path: `C:\A.GDB`, Score: 80, Strategy: StrategyRegistry},
			Alternates: []Candidate{
				{Path: `D:\Z.GDB`, Score: 40, Strategy: StrategyFSWalk},
				{Path: `D:\A.GDB`, Score: 60, Strategy: StrategyFSTemplate},
			},
		},
		{Source: SourceSIHD},
		{Source: SourceBPA},
		{Source: SourceSIA},
	}
	var buf bytes.Buffer
	require.NoError(t, WriteYAML(&buf, results))
	out := buf.String()
	require.Contains(t, out, `D:\A.GDB`)
	require.Contains(t, out, `D:\Z.GDB`)
}
