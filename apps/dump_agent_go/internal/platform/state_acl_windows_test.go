//go:build windows

package platform

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/windows"
)

func TestRestrictStatePath_RemoveUsuariosHerdados(t *testing.T) {
	dir := t.TempDir()
	if err := RestrictStatePath(dir); err != nil {
		t.Fatalf("RestrictStatePath: %v", err)
	}
	sd, err := windows.GetNamedSecurityInfo(
		dir,
		windows.SE_FILE_OBJECT,
		windows.DACL_SECURITY_INFORMATION,
	)
	if err != nil {
		t.Fatalf("GetNamedSecurityInfo: %v", err)
	}
	got := sd.String()
	if !strings.Contains(got, "D:P") || !strings.Contains(got, ";;;SY") ||
		!strings.Contains(got, ";;;BA") || strings.Contains(got, ";;;AU") {
		t.Fatalf("DACL = %q, want protected SYSTEM/Administrators ACL", got)
	}
}

func TestRestrictStateTree_ProtegeFilhoExistente(t *testing.T) {
	root := t.TempDir()
	child := filepath.Join(root, "queue", "outbox.db")
	if err := os.MkdirAll(filepath.Dir(child), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	file, err := os.Create(child)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := RestrictStateTree(root); err != nil {
		t.Fatalf("RestrictStateTree: %v", err)
	}
	sd, err := windows.GetNamedSecurityInfo(
		child,
		windows.SE_FILE_OBJECT,
		windows.DACL_SECURITY_INFORMATION,
	)
	if err != nil {
		t.Fatalf("GetNamedSecurityInfo: %v", err)
	}
	got := sd.String()
	if !strings.Contains(got, "D:P") || !strings.Contains(got, ";;;SY") ||
		!strings.Contains(got, ";;;BA") || strings.Contains(got, ";;;AU") {
		t.Fatalf("child DACL = %q, want protected SYSTEM/Administrators ACL", got)
	}
}
