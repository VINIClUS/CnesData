package main

import (
	"fmt"
	"os"

	"github.com/cnesdata/dumpagent/internal/service"
)

func cmdInstall(args []string) int {
	rc := service.Install(args)
	if rc != 0 {
		return rc
	}
	if err := service.RegisterEventSource(service.EventSourceName); err != nil {
		fmt.Fprintf(os.Stderr, "warn: eventlog source registration failed: %v\n", err)
		fmt.Fprintln(os.Stderr, "warn: continuing without eventlog sink (file logs unaffected)")
	} else {
		fmt.Printf("eventlog source registered: %s\n", service.EventSourceName)
	}
	return 0
}
