package discover

import "context"

// probeSIA discovers the SIA DBF directory.
func probeSIA(ctx context.Context, d probeDeps) SourceResult {
	p := ProfileFor(SourceSIA)
	cs := append([]Candidate(nil),
		FilesystemHits(ctx, p, d.FS, d.Drives)...)
	cs = append(cs, registryCandidatesForSIA(p, d.FS, d.Reg)...)
	return resultFromCandidates(SourceSIA, cs)
}

func registryCandidatesForSIA(p Profile, fs FS, regFn registryFn) []Candidate {
	hits := regFn(p)
	out := make([]Candidate, 0, len(hits))
	for _, h := range hits {
		info, err := fs.Stat(h.Path)
		exists := err == nil && info.Exists && info.IsDir
		score := Score(ScoreInput{
			Strategy:         StrategyRegistry,
			FileExists:       exists,
			IsSIADir:         exists,
			SIAExpectedFound: siaCompleteThreshold,
			Source:           SourceSIA,
		})
		if score == 0 {
			continue
		}
		out = append(out, Candidate{
			Path: h.Path, Score: score, Strategy: StrategyRegistry,
		})
	}
	return out
}
