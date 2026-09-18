package main

import (
	"os"

	"github.com/cnesdata/dumpagent/internal/apiclient"
	"github.com/cnesdata/dumpagent/internal/upload"
	"github.com/cnesdata/dumpagent/internal/worker"
)

// buildDispatchConfig constrói worker.DispatchConfig para BPA_MAG/SIA_LOCAL
// a partir dos flags da CLI + adapter (para RegisterFunc).
// O dispatcher BPA/SIA é acionado em fluxo distinto do JobExecutor clássico
// (CNES/SIHD seguem por intentPipelines).
func buildDispatchConfig(flags RunFlags, adapter *apiclient.Adapter) worker.DispatchConfig {
	var register worker.RegisterFunc
	if adapter != nil {
		register = adapter.RegisterBPASIAJob
	}
	return worker.DispatchConfig{
		BPA: worker.BPAPipelineConfig{
			GDBPath:      flags.BPAGDBPath,
			FBHost:       envOr("DB_HOST", "localhost"),
			FBPort:       fbPort(),
			FBUser:       envOr("DB_USER", "SYSDBA"),
			FBPassword:   os.Getenv("DB_PASSWORD"),
			FBClientPath: flags.FBClientPath,
			Uploader:     upload.NewHTTP(nil),
			Register:     register,
		},
		SIA: worker.SIAPipelineConfig{
			SIADir:   flags.SIADir,
			Uploader: upload.NewHTTP(nil),
			Register: register,
		},
	}
}
