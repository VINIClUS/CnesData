//go:build !windows

package discover

// DefaultDrives returns nothing on non-Windows; discovery emits 4 empty
// SourceResults — caller (cmd_discover) prints "no candidates found".
func DefaultDrives() []string {
	return nil
}
