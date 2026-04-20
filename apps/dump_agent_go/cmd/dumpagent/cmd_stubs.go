package main

import (
	"fmt"
	"os"
)

func detectAutoMode() int {
	return cmdRun(nil)
}

func cmdService(_ []string) int {
	fmt.Fprintln(os.Stderr, "service subcommand not yet implemented (Plan B Task 3)")
	return 2
}

func cmdInstall(_ []string) int {
	fmt.Fprintln(os.Stderr, "install subcommand not yet implemented (Plan B Task 4)")
	return 2
}

func cmdUninstall() int {
	fmt.Fprintln(os.Stderr, "uninstall subcommand not yet implemented (Plan B Task 4)")
	return 2
}
