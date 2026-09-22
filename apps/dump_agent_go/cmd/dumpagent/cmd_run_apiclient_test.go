package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A5: landing.extractions.agent_version registrava sempre "dev" porque
// buildAPIClient nunca repassava main.Version (setado via -X no build) para
// apiclient.NewAdapter.
func TestBuildAPIClient_AgentVersionUsaVersionDoBuild(t *testing.T) {
	t.Setenv("TENANT_ID", "354130")
	t.Setenv("AGENT_VERSION", "")

	origVersion := Version
	Version = "v9.9.9-test"
	defer func() { Version = origVersion }()

	adapter, err := buildAPIClient("machine-1", nil)
	require.NoError(t, err)
	require.Equal(t, "v9.9.9-test", adapter.AgentVersion)
}

func TestBuildAPIClient_EnvAgentVersionTemPrecedenciaSobreVersionDoBuild(t *testing.T) {
	t.Setenv("TENANT_ID", "354130")
	t.Setenv("AGENT_VERSION", "env-override")

	origVersion := Version
	Version = "v9.9.9-test"
	defer func() { Version = origVersion }()

	adapter, err := buildAPIClient("machine-1", nil)
	require.NoError(t, err)
	require.Equal(t, "env-override", adapter.AgentVersion)
}
