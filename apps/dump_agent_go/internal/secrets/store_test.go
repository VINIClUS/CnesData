package secrets

import (
	"errors"
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
