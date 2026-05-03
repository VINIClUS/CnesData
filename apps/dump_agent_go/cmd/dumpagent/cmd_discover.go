package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/cnesdata/dumpagent/internal/discover"
	"github.com/cnesdata/dumpagent/internal/platform"
)

// discoverRunFn is the test seam for discover.Run.
var discoverRunFn = discover.Run

type discoverFlags struct {
	out    string
	tenant string
	force  bool
	dryRun bool
}

func cmdDiscover(args []string) int {
	return runDiscover(args)
}

func parseDiscoverFlags(args []string) (discoverFlags, error) {
	fs := flag.NewFlagSet("discover", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	out := fs.String("out", "",
		"output YAML path (default: %PROGRAMDATA%\\dumpagent\\config.yaml)")
	tenant := fs.String("tenant", "",
		"tenant id (audit only; not written to YAML)")
	force := fs.Bool("force", false, "overwrite existing config.yaml")
	dryRun := fs.Bool("dry-run", false, "print summary; do not write file")
	if err := fs.Parse(args); err != nil {
		return discoverFlags{}, err
	}
	return discoverFlags{
		out:    *out,
		tenant: *tenant,
		force:  *force,
		dryRun: *dryRun,
	}, nil
}

func resolveDiscoverOut(flagOut string) (string, error) {
	if flagOut != "" {
		return flagOut, nil
	}
	dir, err := platform.AppDataDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "config.yaml"), nil
}

func runDiscover(args []string) int {
	flags, err := parseDiscoverFlags(args)
	if err != nil {
		return 2
	}
	out, err := resolveDiscoverOut(flags.out)
	if err != nil {
		fmt.Fprintf(os.Stderr, "discover_out_resolve: %s\n", err.Error())
		return 1
	}
	if !flags.force {
		if _, err := os.Stat(out); err == nil {
			fmt.Fprintf(os.Stderr,
				"file_exists: %s (use --force to overwrite)\n", out)
			return 2
		}
	}
	results := discoverRunFn(context.Background(), buildRunConfig())
	any := anyTopFound(results)
	if !flags.dryRun {
		if err := discover.WriteYAMLAtomic(out, results); err != nil {
			fmt.Fprintf(os.Stderr, "yaml_write_failed: %s\n", err.Error())
			return 1
		}
	}
	printDiscoverSummary(os.Stdout, out, results)
	if !any {
		return 3
	}
	return 0
}

func buildRunConfig() discover.RunConfig {
	return discover.RunConfig{
		FS:          discover.OSFS{},
		Drives:      discover.DefaultDrives,
		RegistryFn:  discover.RegistryHits,
		PerProbeTTL: 10 * time.Second,
	}
}

func anyTopFound(results []discover.SourceResult) bool {
	for _, r := range results {
		if r.Top.Path != "" {
			return true
		}
	}
	return false
}

func printDiscoverSummary(w *os.File, out string, results []discover.SourceResult) {
	for _, r := range results {
		if r.Top.Path == "" {
			fmt.Fprintf(w, "%s: 0 candidates\n", r.Source.String())
			continue
		}
		fmt.Fprintf(w, "%s: top=%s score=%d strategy=%s alternates=%d\n",
			r.Source.String(), r.Top.Path, r.Top.Score,
			r.Top.Strategy.String(), len(r.Alternates))
	}
	fmt.Fprintf(w, "Wrote: %s\n", out)
}
