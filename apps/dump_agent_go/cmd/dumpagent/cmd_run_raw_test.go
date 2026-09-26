package main

import (
	"testing"

	"github.com/cnesdata/dumpagent/internal/rawclient"
	"github.com/stretchr/testify/require"
)

func TestClaimRawMontaJobComIdentidadeDoServidor(t *testing.T) {
	client := rawclient.New("https://api.example", nil, "", "")
	claim := rawclient.Claim{
		JobID: "job-1", AgentID: "registered-agent", SourceType: "SIHD",
		FileSubtype: "SIHD_INTERNACAO", Competencia: "2026-09",
		SnapshotMode: "FULL", FencingToken: 2,
		RawUploadPath: "/api/v1/edge/jobs/job-1/raw-object",
	}

	job, err := rawJobFromClaim(client, "354130", claim)

	require.NoError(t, err)
	require.Equal(t, "registered-agent", job.RawRequest.AgentID)
	require.Equal(t, "202609", job.Params.Competencia)
	require.Equal(t, "https://api.example/api/v1/edge/jobs/job-1/raw-object", job.UploadURL)
}
