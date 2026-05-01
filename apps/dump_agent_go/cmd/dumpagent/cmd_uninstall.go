package main

import (
	"fmt"

	"github.com/cnesdata/dumpagent/internal/service"
)

func cmdUninstall() int {
	if err := service.RemoveEventSource(service.EventSourceName); err != nil {
		fmt.Printf("warn: eventlog source removal failed: %v\n", err)
	}
	return service.Uninstall()
}
