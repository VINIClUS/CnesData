package main

import "github.com/cnesdata/dumpagent/internal/service"

func cmdInstall(args []string) int {
	return service.Install(args)
}
