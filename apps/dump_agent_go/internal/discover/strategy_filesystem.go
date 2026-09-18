package discover

import (
	"context"
	"errors"
	"strings"
)

// ErrNotDir is returned by FS.ReadDir when the target path is not a directory.
var ErrNotDir = errors.New("not_dir")

// FileInfo is the minimal stat shape used by discover strategies.
type FileInfo struct {
	Exists bool
	Size   int64
	IsDir  bool
}

// FS abstracts filesystem access so strategies can be tested with fakes.
type FS interface {
	Stat(path string) (FileInfo, error)
	ReadDir(path string) ([]string, error)
}

// DriveEnumerator returns the list of drive prefixes (e.g. "C:") to scan.
type DriveEnumerator func() []string

// FilesystemHits applies a profile's FS templates over each drive.
func FilesystemHits(
	ctx context.Context, p Profile, fs FS, drives DriveEnumerator,
) []Candidate {
	if ctx.Err() != nil {
		return nil
	}
	out := make([]Candidate, 0)
	for _, drive := range drives() {
		if ctx.Err() != nil {
			return out
		}
		for _, tmpl := range p.FSTemplates {
			path := strings.ReplaceAll(tmpl, "<DRIVE>", drive)
			if c, ok := scoreFSPath(p, path, fs); ok {
				out = append(out, c)
			}
		}
	}
	return out
}

func scoreFSPath(p Profile, path string, fs FS) (Candidate, bool) {
	if p.Source == SourceSIA {
		return scoreSIADir(p, path, fs)
	}
	return scoreFBFile(p, path, fs)
}

func scoreFBFile(p Profile, path string, fs FS) (Candidate, bool) {
	info, err := fs.Stat(path)
	if err != nil || !info.Exists || info.IsDir {
		return Candidate{}, false
	}
	score := Score(ScoreInput{
		Strategy:   StrategyFSTemplate,
		FileExists: true,
		FileSize:   info.Size,
		Source:     p.Source,
	})
	if score == 0 {
		return Candidate{}, false
	}
	return Candidate{Path: path, Score: score, Strategy: StrategyFSTemplate}, true
}

func scoreSIADir(p Profile, path string, fs FS) (Candidate, bool) {
	info, err := fs.Stat(path)
	if err != nil || !info.Exists || !info.IsDir {
		return Candidate{}, false
	}
	found := countSIAExpectedDBFs(p, path, fs)
	if found < siaCompleteThreshold {
		return Candidate{}, false
	}
	score := Score(ScoreInput{
		Strategy:         StrategyFSTemplate,
		FileExists:       true,
		FileSize:         1,
		IsSIADir:         true,
		SIAExpectedFound: found,
		Source:           SourceSIA,
	})
	return Candidate{Path: path, Score: score, Strategy: StrategyFSTemplate}, true
}

func countSIAExpectedDBFs(p Profile, dir string, fs FS) int {
	files, err := fs.ReadDir(dir)
	if err != nil {
		return 0
	}
	found := 0
	for _, name := range files {
		upper := strings.ToUpper(name)
		for _, prefix := range p.SIAExpectedDBFs {
			if strings.HasPrefix(upper, prefix) {
				found++
				break
			}
		}
	}
	return found
}
