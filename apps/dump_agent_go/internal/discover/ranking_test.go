package discover

import (
	"math/rand/v2"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestScore_RegistryDirectHit(t *testing.T) {
	got := Score(ScoreInput{
		Strategy:   StrategyRegistry,
		FileExists: true,
		FileSize:   1024,
		IsSIADir:   false,
		Source:     SourceCNES,
	})
	require.Equal(t, 80, got)
}

func TestScore_RegistryHitFileMissing(t *testing.T) {
	got := Score(ScoreInput{
		Strategy:   StrategyRegistry,
		FileExists: false,
	})
	require.Equal(t, 30, got)
}

func TestScore_FSTemplateNonEmpty(t *testing.T) {
	got := Score(ScoreInput{
		Strategy:   StrategyFSTemplate,
		FileExists: true,
		FileSize:   100,
		Source:     SourceCNES,
	})
	require.Equal(t, 60, got)
}

func TestScore_FSTemplateEmpty(t *testing.T) {
	got := Score(ScoreInput{
		Strategy:   StrategyFSTemplate,
		FileExists: true,
		FileSize:   0,
		Source:     SourceCNES,
	})
	require.Equal(t, 35, got)
}

func TestScore_FSWalk(t *testing.T) {
	got := Score(ScoreInput{
		Strategy:   StrategyFSWalk,
		FileExists: true,
		FileSize:   100,
		Source:     SourceCNES,
	})
	require.Equal(t, 40, got)
}

func TestScore_SIABonus(t *testing.T) {
	got := Score(ScoreInput{
		Strategy:         StrategyFSTemplate,
		FileExists:       true,
		FileSize:         100,
		IsSIADir:         true,
		SIAExpectedFound: 3,
		Source:           SourceSIA,
	})
	require.Equal(t, 70, got, "60 base + 10 bonus")
}

func TestScore_BPAFatBonus(t *testing.T) {
	got := Score(ScoreInput{
		Strategy:   StrategyRegistry,
		FileExists: true,
		FileSize:   100 * 1024 * 1024,
		Source:     SourceBPA,
	})
	require.Equal(t, 85, got, "80 base + 5 fat-file bonus")
}

func TestTop_DeterministicTieBreak(t *testing.T) {
	for i := 0; i < 100; i++ {
		cs := []Candidate{
			{Path: "C:\\Z.GDB", Score: 80, Strategy: StrategyRegistry},
			{Path: "C:\\A.GDB", Score: 80, Strategy: StrategyRegistry},
			{Path: "C:\\M.GDB", Score: 80, Strategy: StrategyRegistry},
		}
		rng := rand.New(rand.NewPCG(uint64(i), 0))
		rng.Shuffle(len(cs), func(a, b int) { cs[a], cs[b] = cs[b], cs[a] })
		top := Top(cs)
		require.Equal(t, "C:\\A.GDB", top.Path,
			"shuffle iteration %d picked %s", i, top.Path)
	}
}

func TestTop_EmptyReturnsZero(t *testing.T) {
	require.Equal(t, Candidate{}, Top(nil))
	require.Equal(t, Candidate{}, Top([]Candidate{}))
}

func TestSortAndTop_BehaviorIdentical(t *testing.T) {
	cs := []Candidate{
		{Path: "C:\\B.GDB", Score: 60, Strategy: StrategyFSTemplate},
		{Path: "C:\\A.GDB", Score: 80, Strategy: StrategyRegistry},
	}
	sorted := append([]Candidate(nil), cs...)
	sort.Sort(ByScoreThenPath(sorted))
	require.Equal(t, sorted[0], Top(cs))
}

func TestScore_UnknownStrategyReturnsZero(t *testing.T) {
	got := Score(ScoreInput{Strategy: StrategyUnknown, FileExists: true})
	require.Equal(t, 0, got)
}

func TestScore_BPABonusBoundary(t *testing.T) {
	at := Score(ScoreInput{
		Strategy: StrategyRegistry, FileExists: true,
		FileSize: 50 * 1024 * 1024, Source: SourceBPA,
	})
	require.Equal(t, 80, at, "exactly 50MB does not earn bonus (strict >)")
	over := Score(ScoreInput{
		Strategy: StrategyRegistry, FileExists: true,
		FileSize: 50*1024*1024 + 1, Source: SourceBPA,
	})
	require.Equal(t, 85, over, "50MB+1 byte earns +5 fat-file bonus")
}

func TestScore_SIABonusRequires3Expected(t *testing.T) {
	below := Score(ScoreInput{
		Strategy:         StrategyFSTemplate,
		FileExists:       true,
		FileSize:         100,
		IsSIADir:         true,
		SIAExpectedFound: 2,
		Source:           SourceSIA,
	})
	require.Equal(t, 60, below, "2 expected DBFs does not unlock bonus")
	at := Score(ScoreInput{
		Strategy:         StrategyFSTemplate,
		FileExists:       true,
		FileSize:         100,
		IsSIADir:         true,
		SIAExpectedFound: 3,
		Source:           SourceSIA,
	})
	require.Equal(t, 70, at, "3 expected DBFs unlocks +10 bonus")
}
