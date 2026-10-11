package main

import (
	"log/slog"
	"os"
	"strings"
)

const rawModeEnv = "AGENT_RAW_MODE"

// rawModeDefault keeps raw for unset, empty and unrecognized values: only an explicit
// "false" (any case) opts out to the legacy /api/v1/jobs protocol, which the server
// retires in MIG-012 and MIG-014 removes.
func rawModeDefault() bool {
	return !strings.EqualFold(os.Getenv(rawModeEnv), "false")
}

func logRunMode(logger *slog.Logger, flags RunFlags) {
	protocol := "legacy"
	if flags.Raw {
		protocol = "raw"
	}
	logger.Info("run_mode", "protocol", protocol, "agent_raw_mode", os.Getenv(rawModeEnv))
}
