package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/cnesdata/dumpagent/internal/audit"
	"github.com/cnesdata/dumpagent/internal/platform"
	"github.com/cnesdata/dumpagent/internal/secrets"
)

var errAuditKeyMissing = errors.New("audit_key_missing")

// auditKeyLoaderFn is a test seam to inject HMAC key resolution.
var auditKeyLoaderFn = defaultAuditKeyLoader

func defaultAuditKeyLoader() ([]byte, error) {
	app, err := platform.AppDataDir()
	if err != nil {
		return nil, errAuditKeyMissing
	}
	store := secrets.NewStore(filepath.Join(app, "secrets"))
	return audit.LoadOrCreate(store)
}

func cmdAudit(args []string) int {
	if len(args) == 0 || args[0] != "verify" {
		fmt.Fprintln(os.Stderr,
			"usage: dumpagent audit verify <path>")
		return 2
	}
	return runAuditVerify(args[1:])
}

func runAuditVerify(args []string) int {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr,
			"usage: dumpagent audit verify <path>")
		return 2
	}
	key, err := auditKeyLoaderFn()
	if err != nil {
		fmt.Fprintf(os.Stderr, "audit_key_load: %s\n", err.Error())
		return 2
	}
	valid, errs, err := audit.VerifyFile(args[0], key)
	if err != nil {
		fmt.Fprintf(os.Stderr, "verify_file: %s\n", err.Error())
		return 1
	}
	fmt.Printf("valid=%d invalid=%d path=%s\n",
		len(valid), len(errs), args[0])
	if len(errs) > 0 {
		for _, e := range errs {
			fmt.Fprintf(os.Stderr, "line=%d reason=%s\n",
				e.LineNum, e.Reason)
		}
		return 1
	}
	return 0
}
