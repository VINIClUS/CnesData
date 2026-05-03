package main

import (
	"errors"
	"testing"

	"github.com/cnesdata/dumpagent/internal/secrets"
	"github.com/stretchr/testify/require"
)

func TestResolvePassword_EnvWins(t *testing.T) {
	envFn := func(_ string) string { return "envpw" }
	loadFn := func(_ string) (string, error) { return "dpapipw", nil }
	pw, src, err := ResolvePassword("cnes", "SYSDBA", envFn, loadFn)
	require.NoError(t, err)
	require.Equal(t, "envpw", pw)
	require.Equal(t, PasswordSourceEnv, src)
}

func TestResolvePassword_DPAPIWhenNoEnv(t *testing.T) {
	envFn := func(_ string) string { return "" }
	loadFn := func(_ string) (string, error) { return "dpapipw", nil }
	pw, src, err := ResolvePassword("cnes", "SYSDBA", envFn, loadFn)
	require.NoError(t, err)
	require.Equal(t, "dpapipw", pw)
	require.Equal(t, PasswordSourceSecrets, src)
}

func TestResolvePassword_MasterkeyForSYSDBA(t *testing.T) {
	envFn := func(_ string) string { return "" }
	loadFn := func(_ string) (string, error) { return "", secrets.ErrNotSet }
	pw, src, err := ResolvePassword("cnes", "SYSDBA", envFn, loadFn)
	require.NoError(t, err)
	require.Equal(t, "masterkey", pw)
	require.Equal(t, PasswordSourceMasterkey, src)
}

func TestResolvePassword_NoFallbackForNonSYSDBA(t *testing.T) {
	envFn := func(_ string) string { return "" }
	loadFn := func(_ string) (string, error) { return "", secrets.ErrNotSet }
	pw, src, err := ResolvePassword("cnes", "OTHERUSER", envFn, loadFn)
	require.Error(t, err)
	require.Equal(t, "", pw)
	require.Equal(t, PasswordSourceUnconfigured, src)
}

func TestResolvePassword_DPAPIErrorBubbles(t *testing.T) {
	envFn := func(_ string) string { return "" }
	loadFn := func(_ string) (string, error) {
		return "", errors.New("dpapi_unwrap_failed")
	}
	pw, src, err := ResolvePassword("cnes", "SYSDBA", envFn, loadFn)
	require.Error(t, err)
	require.NotEqual(t, "masterkey", pw)
	require.Equal(t, PasswordSourceError, src)
}

func TestPasswordSource_String(t *testing.T) {
	require.Equal(t, "env", PasswordSourceEnv.String())
	require.Equal(t, "secrets-store", PasswordSourceSecrets.String())
	require.Equal(t, "masterkey-default", PasswordSourceMasterkey.String())
	require.Equal(t, "unconfigured", PasswordSourceUnconfigured.String())
}
