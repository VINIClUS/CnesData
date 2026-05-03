package discover

import (
	"context"
	"sort"
)

// registryFn is the seam to inject the registry probe into tests.
type registryFn func(Profile) []RegistryHit

// probeDeps groups the per-probe collaborators so probe entry-points stay
// within the 4-parameter hard limit.
type probeDeps struct {
	FS     FS
	Drives DriveEnumerator
	Reg    registryFn
}

// probeFB aggregates registry hits + filesystem hits into a SourceResult
// for one of cnes/sihd/bpa. Caller supplies the profile.
func probeFB(ctx context.Context, p Profile, d probeDeps) SourceResult {
	cs := append([]Candidate(nil),
		FilesystemHits(ctx, p, d.FS, d.Drives)...)
	cs = append(cs, registryCandidatesForFB(p, d.FS, d.Reg)...)
	return resultFromCandidates(p.Source, cs)
}

func registryCandidatesForFB(p Profile, fs FS, regFn registryFn) []Candidate {
	hits := regFn(p)
	out := make([]Candidate, 0, len(hits))
	for _, h := range hits {
		c := candidateForRegistryFB(p, fs, h.Path)
		if c.Score > 0 {
			out = append(out, c)
		}
	}
	return out
}

func candidateForRegistryFB(p Profile, fs FS, path string) Candidate {
	info, err := fs.Stat(path)
	exists := err == nil && info.Exists && !info.IsDir
	score := Score(ScoreInput{
		Strategy:   StrategyRegistry,
		FileExists: exists,
		FileSize:   info.Size,
		Source:     p.Source,
	})
	if score == 0 {
		return Candidate{}
	}
	return Candidate{Path: path, Score: score, Strategy: StrategyRegistry}
}

func resultFromCandidates(src SourceID, cs []Candidate) SourceResult {
	if len(cs) == 0 {
		return SourceResult{Source: src}
	}
	top := Top(cs)
	alt := make([]Candidate, 0, len(cs))
	removed := false
	for _, c := range cs {
		if !removed && c == top {
			removed = true
			continue
		}
		alt = append(alt, c)
	}
	sortAlternates(alt)
	return SourceResult{Source: src, Top: top, Alternates: alt}
}

func sortAlternates(cs []Candidate) {
	sort.Sort(ByScoreThenPath(cs))
}
