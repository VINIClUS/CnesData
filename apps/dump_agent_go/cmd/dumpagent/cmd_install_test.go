package main

import (
	"testing"

	"github.com/cnesdata/dumpagent/internal/service"
)

func TestCmdInstall_EventSourceConstantsMatch(t *testing.T) {
	if service.EventSourceName != "DumpAgent" {
		t.Fatalf("source name drift: got %q want DumpAgent", service.EventSourceName)
	}
}
