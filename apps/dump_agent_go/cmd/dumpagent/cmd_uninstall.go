package main

import "github.com/cnesdata/dumpagent/internal/service"

func cmdUninstall() int {
	return service.Uninstall()
}
