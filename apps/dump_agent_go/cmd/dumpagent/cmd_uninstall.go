package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/cnesdata/dumpagent/internal/platform"
	"github.com/cnesdata/dumpagent/internal/service"
)

// cmdUninstall stops (if running), removes the service from the SCM, and
// deregisters its eventlog source, in that order — see
// internal/service/lifecycle.go. State (certs, secrets, queue, delta,
// audit log, machine_id) is preserved by default, matching
// docs/runbooks/dumpagent-rollback.md, which reads the state root *after*
// this same uninstall step (H3 in docs/edge-agent-audit-2026-09-20.md — a
// default purge would silently break that documented rollback procedure).
// --purge opts in to removing the state root too.
func cmdUninstall(args []string) int {
	fs := flag.NewFlagSet("uninstall", flag.ExitOnError)
	purge := fs.Bool("purge", false, "also remove agent state (certs, secrets, queue, logs)")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	rc := service.Uninstall()
	if rc != 0 {
		return rc
	}
	if !*purge {
		return 0
	}
	return purgeState()
}

func purgeState() int {
	dir, err := platform.AppDataDir()
	if err != nil {
		fmt.Fprintf(os.Stderr, "purge: resolve_app_data_dir: %v\n", err)
		return 1
	}
	if err := os.RemoveAll(dir); err != nil {
		fmt.Fprintf(os.Stderr, "purge: remove_state_dir: %v\n", err)
		return 1
	}
	fmt.Printf("purged state dir=%s\n", dir)
	return 0
}
