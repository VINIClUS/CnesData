package main

import (
	"context"
	"io"
	"os"

	"github.com/cnesdata/dumpagent/internal/auth"
	"github.com/cnesdata/dumpagent/internal/diagnose"
	"github.com/cnesdata/dumpagent/internal/platform"
)

// ctxLike lets the test seam accept any context value.
type ctxLike = context.Context

// diagnoseRunFn is the seam tests can override.
var diagnoseRunFn = func(ctx ctxLike, cfg diagnose.Config) []diagnose.Check {
	return diagnose.Run(ctx, cfg)
}

func cmdDiagnose(args []string) int {
	return runDiagnose(args, os.Stdout)
}

func runDiagnose(args []string, w io.Writer) int {
	cfg := parseDiagnoseFlags(args)
	resolveDiagnoseConfig(&cfg)
	checks := diagnoseRunFn(context.Background(), cfg)
	if cfg.JSON {
		_ = diagnose.JSON(w, checks)
	} else {
		_ = diagnose.Text(w, checks)
	}
	if diagnose.AnyFail(checks) {
		return 1
	}
	return 0
}

func parseDiagnoseFlags(args []string) diagnose.Config {
	cfg := diagnose.Config{}
	for _, a := range args {
		switch a {
		case "--probe":
			cfg.Probe = true
		case "--json":
			cfg.JSON = true
		}
	}
	return cfg
}

// resolveDiagnoseConfig fills AuthDir/AppData/BaseURL/FBDsn/MinIOEP from defaults + env.
// Errors are tolerated — checks themselves report missing dirs as FAIL.
func resolveDiagnoseConfig(cfg *diagnose.Config) {
	if cfg.AuthDir == "" {
		if dir, err := auth.AuthDir(); err == nil {
			cfg.AuthDir = dir
		}
	}
	if cfg.AppData == "" {
		if appData, err := platform.AppDataDir(); err == nil {
			cfg.AppData = appData
		}
	}
	if cfg.BaseURL == "" {
		cfg.BaseURL = os.Getenv("CENTRAL_API_URL")
	}
	if cfg.FBDsn == "" {
		cfg.FBDsn = os.Getenv("FB_DSN")
	}
	if cfg.MinIOEP == "" {
		cfg.MinIOEP = os.Getenv("MINIO_ENDPOINT")
	}
}
