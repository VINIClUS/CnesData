package discover

import "sort"

const (
	scoreRegistryHit        = 80
	scoreFSTemplateNonEmpty = 60
	scoreFSWalkHit          = 40
	scoreFSTemplateEmpty    = 35
	scoreRegistryNoFile     = 30
	bonusSIAComplete        = 10
	bonusBPAFatFile         = 5
	siaCompleteThreshold    = 3
	bpaFatFileBytes         = 50 * 1024 * 1024
)

// ScoreInput groups the inputs that drive Score for a single Candidate.
type ScoreInput struct {
	Strategy         Strategy
	FileExists       bool
	FileSize         int64
	IsSIADir         bool
	SIAExpectedFound int
	Source           SourceID
}

// Score returns the total ranking score for a Candidate (base + bonus).
func Score(in ScoreInput) int {
	base := baseScore(in)
	if base == 0 {
		return 0
	}
	return base + bonusScore(in)
}

// Top returns the highest-ranked Candidate (descending score, lex path tie-break).
func Top(cs []Candidate) Candidate {
	if len(cs) == 0 {
		return Candidate{}
	}
	cp := append([]Candidate(nil), cs...)
	sort.Sort(ByScoreThenPath(cp))
	return cp[0]
}

func baseScore(in ScoreInput) int {
	switch in.Strategy {
	case StrategyRegistry:
		if in.FileExists {
			return scoreRegistryHit
		}
		return scoreRegistryNoFile
	case StrategyFSTemplate:
		if !in.FileExists {
			return 0
		}
		if in.FileSize == 0 && !in.IsSIADir {
			return scoreFSTemplateEmpty
		}
		return scoreFSTemplateNonEmpty
	case StrategyFSWalk:
		if in.FileExists {
			return scoreFSWalkHit
		}
		return 0
	default:
		return 0
	}
}

func bonusScore(in ScoreInput) int {
	bonus := 0
	if in.Source == SourceSIA && in.IsSIADir && in.SIAExpectedFound >= siaCompleteThreshold {
		bonus += bonusSIAComplete
	}
	if in.Source == SourceBPA && in.FileSize > bpaFatFileBytes {
		bonus += bonusBPAFatFile
	}
	return bonus
}
