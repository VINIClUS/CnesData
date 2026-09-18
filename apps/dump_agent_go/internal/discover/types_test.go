package discover

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSourceID_String(t *testing.T) {
	require.Equal(t, "cnes", SourceCNES.String())
	require.Equal(t, "sihd", SourceSIHD.String())
	require.Equal(t, "bpa", SourceBPA.String())
	require.Equal(t, "sia", SourceSIA.String())
}

func TestStrategy_String(t *testing.T) {
	require.Equal(t, "registry", StrategyRegistry.String())
	require.Equal(t, "fs_template", StrategyFSTemplate.String())
	require.Equal(t, "fs_walk", StrategyFSWalk.String())
}

func TestCandidate_SortByScoreDesc_TieBreakLex(t *testing.T) {
	cs := []Candidate{
		{Path: "C:\\B\\X.GDB", Score: 60, Strategy: StrategyFSTemplate},
		{Path: "C:\\A\\X.GDB", Score: 80, Strategy: StrategyRegistry},
		{Path: "C:\\A\\Y.GDB", Score: 60, Strategy: StrategyFSTemplate},
		{Path: "C:\\C\\X.GDB", Score: 80, Strategy: StrategyRegistry},
	}
	sort.Sort(ByScoreThenPath(cs))
	require.Equal(t, "C:\\A\\X.GDB", cs[0].Path)
	require.Equal(t, "C:\\C\\X.GDB", cs[1].Path)
	require.Equal(t, "C:\\A\\Y.GDB", cs[2].Path)
	require.Equal(t, "C:\\B\\X.GDB", cs[3].Path)
}

func TestConfig_ZeroValueIsValidEmpty(t *testing.T) {
	var c Config
	require.Empty(t, c.CNES.DatabasePath)
	require.Empty(t, c.SIA.DBFDir)
}
