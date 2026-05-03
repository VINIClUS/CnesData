package discover

import "sort"

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
			return 80
		}
		return 30
	case StrategyFSTemplate:
		if !in.FileExists {
			return 0
		}
		if in.FileSize == 0 && !in.IsSIADir {
			return 35
		}
		return 60
	case StrategyFSWalk:
		if in.FileExists {
			return 40
		}
		return 0
	default:
		return 0
	}
}

func bonusScore(in ScoreInput) int {
	bonus := 0
	if in.Source == SourceSIA && in.IsSIADir && in.SIAExpectedFound >= 3 {
		bonus += 10
	}
	if in.Source == SourceBPA && in.FileSize > 50*1024*1024 {
		bonus += 5
	}
	return bonus
}
