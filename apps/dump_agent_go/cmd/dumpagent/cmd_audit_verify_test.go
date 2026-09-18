package main

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRunAuditVerify_NoArgsExit2(t *testing.T) {
	code := runAuditVerify(nil)
	require.Equal(t, 2, code)
}

func TestRunAuditVerify_KeyMissingExit2(t *testing.T) {
	prev := auditKeyLoaderFn
	defer func() { auditKeyLoaderFn = prev }()
	auditKeyLoaderFn = func() ([]byte, error) {
		return nil, errAuditKeyMissing
	}
	code := runAuditVerify([]string{"someplace.jsonl"})
	require.Equal(t, 2, code)
}

func TestRunAuditVerify_FileMissingExit1(t *testing.T) {
	prev := auditKeyLoaderFn
	defer func() { auditKeyLoaderFn = prev }()
	auditKeyLoaderFn = func() ([]byte, error) {
		return []byte("0123456789abcdef0123456789abcdef"), nil
	}
	code := runAuditVerify([]string{
		filepath.Join(t.TempDir(), "missing.jsonl"),
	})
	require.Equal(t, 1, code)
}

func TestCmdAudit_RejectsUnknownAction(t *testing.T) {
	code := cmdAudit([]string{"nope"})
	require.Equal(t, 2, code)
}

func TestCmdAudit_NoArgsRejected(t *testing.T) {
	code := cmdAudit(nil)
	require.Equal(t, 2, code)
}
