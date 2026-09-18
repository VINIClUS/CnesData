package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/term"

	"github.com/cnesdata/dumpagent/internal/platform"
	"github.com/cnesdata/dumpagent/internal/secrets"
)

// errSetSecretNotTTY signals stdin is not a terminal — refuse to prompt
// for a password (capture risk in scripts).
var errSetSecretNotTTY = errors.New("not_a_tty")

// setSecretDirFn returns the directory secret files live under. Test seam.
var setSecretDirFn = defaultSetSecretDir

// setSecretPromptFn reads a password with no echo. Test seam.
var setSecretPromptFn = defaultSetSecretPrompt

func defaultSetSecretDir() (string, error) {
	app, err := platform.AppDataDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(app, "secrets"), nil
}

func defaultSetSecretPrompt() (string, error) {
	if !term.IsTerminal(int(os.Stdin.Fd())) {
		return "", errSetSecretNotTTY
	}
	fmt.Fprint(os.Stderr, "Password (no echo): ")
	pw, err := term.ReadPassword(int(os.Stdin.Fd()))
	fmt.Fprintln(os.Stderr)
	if err != nil {
		return "", err
	}
	return string(pw), nil
}

func cmdSetSecret(args []string) int {
	return runSetSecret(args)
}

func runSetSecret(args []string) int {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "usage: dumpagent set-secret <cnes|sihd|bpa>")
		return 2
	}
	source := args[0]
	if err := secrets.ValidateSource(source); err != nil {
		fmt.Fprintf(os.Stderr, "invalid_source: %s\n", err.Error())
		return 2
	}
	dir, err := setSecretDirFn()
	if err != nil {
		fmt.Fprintf(os.Stderr, "secrets_dir_init: %s\n", err.Error())
		return 1
	}
	pw, err := setSecretPromptFn()
	if err != nil {
		fmt.Fprintf(os.Stderr, "prompt_failed: %s\n", err.Error())
		return 2
	}
	if pw == "" {
		fmt.Fprintln(os.Stderr, "password_empty")
		return 2
	}
	if err := secrets.NewStore(dir).Save(source, pw); err != nil {
		fmt.Fprintf(os.Stderr, "save_failed: %s\n", err.Error())
		return 1
	}
	fmt.Printf("secret saved: %s\n", source)
	return 0
}
