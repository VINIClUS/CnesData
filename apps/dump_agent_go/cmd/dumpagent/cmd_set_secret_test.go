package main

import (
	"path/filepath"
	"testing"

	"github.com/cnesdata/dumpagent/internal/secrets"
	"github.com/stretchr/testify/require"
)

func TestRunSetSecret_RoundTrip(t *testing.T) {
	dir := t.TempDir()

	prevDir := setSecretDirFn
	defer func() { setSecretDirFn = prevDir }()
	setSecretDirFn = func() (string, error) { return dir, nil }

	prevPrompt := setSecretPromptFn
	defer func() { setSecretPromptFn = prevPrompt }()
	setSecretPromptFn = func() (string, error) { return "MySecret123", nil }

	code := runSetSecret([]string{"cnes"})
	require.Equal(t, 0, code)

	got, err := secrets.NewStore(dir).Load("cnes")
	require.NoError(t, err)
	require.Equal(t, "MySecret123", got)
}

func TestRunSetSecret_RejectSIA(t *testing.T) {
	code := runSetSecret([]string{"sia"})
	require.Equal(t, 2, code)
}

func TestRunSetSecret_NoArgs(t *testing.T) {
	code := runSetSecret(nil)
	require.Equal(t, 2, code)
}

func TestRunSetSecret_EmptyPasswordReject(t *testing.T) {
	dir := t.TempDir()
	prevDir := setSecretDirFn
	defer func() { setSecretDirFn = prevDir }()
	setSecretDirFn = func() (string, error) { return dir, nil }

	prevPrompt := setSecretPromptFn
	defer func() { setSecretPromptFn = prevPrompt }()
	setSecretPromptFn = func() (string, error) { return "", nil }

	code := runSetSecret([]string{"cnes"})
	require.Equal(t, 2, code)
}

func TestRunSetSecret_TTYRejected(t *testing.T) {
	dir := t.TempDir()
	prevDir := setSecretDirFn
	defer func() { setSecretDirFn = prevDir }()
	setSecretDirFn = func() (string, error) { return dir, nil }

	prevPrompt := setSecretPromptFn
	defer func() { setSecretPromptFn = prevPrompt }()
	setSecretPromptFn = func() (string, error) {
		return "", errSetSecretNotTTY
	}

	code := runSetSecret([]string{"cnes"})
	require.Equal(t, 2, code)

	require.NoFileExists(t, filepath.Join(dir, "cnes.dpapi"))
}
