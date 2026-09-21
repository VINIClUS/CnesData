//go:build windows

package platform

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"golang.org/x/sys/windows"
)

const privateStateSDDL = "D:P(A;OICI;FA;;;SY)(A;OICI;FA;;;BA)"

// RestrictStatePath applies a protected SYSTEM/Administrators-only DACL.
func RestrictStatePath(path string) error {
	sd, err := windows.SecurityDescriptorFromString(privateStateSDDL)
	if err != nil {
		return fmt.Errorf("parse_state_acl=%w", err)
	}
	dacl, _, err := sd.DACL()
	if err != nil {
		return fmt.Errorf("read_state_acl=%w", err)
	}
	if err := windows.SetNamedSecurityInfo(
		path,
		windows.SE_FILE_OBJECT,
		windows.DACL_SECURITY_INFORMATION|windows.PROTECTED_DACL_SECURITY_INFORMATION,
		nil,
		nil,
		dacl,
		nil,
	); err != nil {
		return fmt.Errorf("set_state_acl=%w", err)
	}
	return nil
}

// RestrictStateTree repairs the DACL on a state root and existing descendants.
func RestrictStateTree(root string) error {
	return filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return nil
		}
		return RestrictStatePath(path)
	})
}
