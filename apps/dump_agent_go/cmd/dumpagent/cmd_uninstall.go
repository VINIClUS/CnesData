package main

import (
	"fmt"
	"os"

	"github.com/cnesdata/dumpagent/internal/service"
)

func cmdUninstall() int {
	if err := service.RemoveEventSource(service.EventSourceName); err != nil {
		fmt.Fprintf(os.Stderr, "warn: eventlog source removal failed: %v\n", err)
	}
	return service.Uninstall()
}
