package main

import (
	"errors"
	"fmt"

	"github.com/cnesdata/dumpagent/internal/secrets"
)

// PasswordSource identifies which layer supplied the resolved password.
type PasswordSource int

const (
	PasswordSourceUnconfigured PasswordSource = iota
	PasswordSourceEnv
	PasswordSourceSecrets
	PasswordSourceMasterkey
	PasswordSourceError
)

// String returns the audit-log token for a password source.
func (p PasswordSource) String() string {
	switch p {
	case PasswordSourceEnv:
		return "env"
	case PasswordSourceSecrets:
		return "secrets-store"
	case PasswordSourceMasterkey:
		return "masterkey-default"
	case PasswordSourceError:
		return "error"
	default:
		return "unconfigured"
	}
}

// ResolvePassword applies env > DPAPI > masterkey (SYSDBA only).
// Returns ("", PasswordSourceUnconfigured, err) if all fall through and
// user is not SYSDBA. DPAPI unwrap failure (other than ErrNotSet) is
// fatal and returns PasswordSourceError.
func ResolvePassword(
	source, user string,
	envFn func(string) string,
	loadFn func(string) (string, error),
) (string, PasswordSource, error) {
	envKey := upperASCII(source) + "_DB_PASSWORD"
	if v := envFn(envKey); v != "" {
		return v, PasswordSourceEnv, nil
	}
	pw, err := loadFn(source)
	if err == nil && pw != "" {
		return pw, PasswordSourceSecrets, nil
	}
	if err != nil && !errors.Is(err, secrets.ErrNotSet) {
		return "", PasswordSourceError,
			fmt.Errorf("dpapi_unwrap source=%s: %w", source, err)
	}
	if user == "SYSDBA" {
		return "masterkey", PasswordSourceMasterkey, nil
	}
	return "", PasswordSourceUnconfigured,
		fmt.Errorf("password_unconfigured source=%s user=%s", source, user)
}
