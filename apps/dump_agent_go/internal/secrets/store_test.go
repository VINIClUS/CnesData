package secrets

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSourceName_Validate(t *testing.T) {
	for _, tc := range []struct {
		name string
		ok   bool
	}{
		{"cnes", true},
		{"sihd", true},
		{"bpa", true},
		{"sia", false},
		{"", false},
		{"../etc", false},
	} {
		err := ValidateSource(tc.name)
		if tc.ok {
			require.NoError(t, err, "expected ok for %q", tc.name)
		} else {
			require.Error(t, err, "expected reject for %q", tc.name)
		}
	}
}

func TestStore_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	s := NewStore(dir)
	require.NoError(t, s.Save("cnes", "p@ssw0rd!"))
	got, err := s.Load("cnes")
	require.NoError(t, err)
	require.Equal(t, "p@ssw0rd!", got)
}

func TestStore_LoadMissingErrNotSet(t *testing.T) {
	s := NewStore(t.TempDir())
	_, err := s.Load("cnes")
	require.True(t, errors.Is(err, ErrNotSet))
}

func TestStore_OverwriteAllowed(t *testing.T) {
	dir := t.TempDir()
	s := NewStore(dir)
	require.NoError(t, s.Save("cnes", "first"))
	require.NoError(t, s.Save("cnes", "second"))
	got, _ := s.Load("cnes")
	require.Equal(t, "second", got)
}

func TestStore_FilePerSource(t *testing.T) {
	dir := t.TempDir()
	s := NewStore(dir)
	require.NoError(t, s.Save("cnes", "c"))
	require.NoError(t, s.Save("bpa", "b"))
	cnes, _ := s.Load("cnes")
	bpa, _ := s.Load("bpa")
	require.Equal(t, "c", cnes)
	require.Equal(t, "b", bpa)

	require.FileExists(t, filepath.Join(dir, "cnes.dpapi"))
	require.FileExists(t, filepath.Join(dir, "bpa.dpapi"))
}

func TestStore_SaveRejectsEmptyPassword(t *testing.T) {
	s := NewStore(t.TempDir())
	err := s.Save("cnes", "")
	require.Error(t, err)
}

func TestStore_SaveRejectsInvalidSource(t *testing.T) {
	s := NewStore(t.TempDir())
	err := s.Save("sia", "xyz")
	require.Error(t, err)
	err = s.Save("../etc", "xyz")
	require.Error(t, err)
}

func TestStore_LoadRejectsInvalidSource(t *testing.T) {
	s := NewStore(t.TempDir())
	_, err := s.Load("sia")
	require.Error(t, err)
}

func TestStore_SaveFailsWhenDirIsAFile(t *testing.T) {
	parent := t.TempDir()
	blocker := filepath.Join(parent, "blocked")
	require.NoError(t, os.WriteFile(blocker, []byte("x"), 0o644))
	s := NewStore(blocker)
	err := s.Save("cnes", "p")
	require.Error(t, err, "MkdirAll over a regular file must fail")
}

func TestStore_LoadFailsWhenPathIsADirectory(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(dir, "cnes.dpapi"), 0o755))
	s := NewStore(dir)
	_, err := s.Load("cnes")
	require.Error(t, err)
	require.False(t, errors.Is(err, ErrNotSet))
}

func TestStore_SaveFailsWhenTmpPathIsADirectory(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(dir, "cnes.dpapi.tmp"), 0o755))
	s := NewStore(dir)
	err := s.Save("cnes", "p")
	require.Error(t, err, "WriteFile must fail when tmp is a directory")
}
