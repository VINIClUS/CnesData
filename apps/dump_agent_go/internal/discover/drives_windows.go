//go:build windows

package discover

import "golang.org/x/sys/windows"

// DefaultDrives returns mapped drive letters (A: through Z:), skipping
// CD-ROM and remote (network) drives to avoid blocking on offline UNC.
func DefaultDrives() []string {
	mask, err := windows.GetLogicalDrives()
	if err != nil {
		return nil
	}
	out := make([]string, 0, 8)
	for i := uint32(0); i < 26; i++ {
		if mask&(1<<i) == 0 {
			continue
		}
		letter := string(rune('A'+i)) + ":"
		dt := getDriveType(letter + `\`)
		if dt == driveCDRom || dt == driveRemote {
			continue
		}
		out = append(out, letter)
	}
	return out
}

const (
	driveCDRom  = 5
	driveRemote = 4
)

func getDriveType(rootPath string) uint32 {
	utf16, _ := windows.UTF16PtrFromString(rootPath)
	return windows.GetDriveType(utf16)
}
