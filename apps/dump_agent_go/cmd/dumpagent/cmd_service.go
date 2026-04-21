package main

import "github.com/cnesdata/dumpagent/internal/service"

func cmdService(_ []string) int {
	return service.RunAsService(Version)
}
