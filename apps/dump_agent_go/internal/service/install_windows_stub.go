//go:build windows

package service

import (
	"fmt"
	"os"
)

// Install placeholder — Plan B Task 4 implementa com svc/mgr.
func Install(_ []string) int {
	fmt.Fprintln(os.Stderr, "install subcommand not yet implemented (Plan B Task 4)")
	return 2
}

// Uninstall placeholder — Plan B Task 4 implementa.
func Uninstall() int {
	fmt.Fprintln(os.Stderr, "uninstall subcommand not yet implemented (Plan B Task 4)")
	return 2
}
