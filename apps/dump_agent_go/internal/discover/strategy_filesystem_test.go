package discover

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

type fakeFS struct {
	files map[string]int64 // path → size
	dirs  map[string]bool
}

func (f *fakeFS) Stat(p string) (FileInfo, error) {
	if size, ok := f.files[p]; ok {
		return FileInfo{Exists: true, Size: size, IsDir: false}, nil
	}
	if f.dirs[p] {
		return FileInfo{Exists: true, IsDir: true}, nil
	}
	return FileInfo{Exists: false}, nil
}

func (f *fakeFS) ReadDir(p string) ([]string, error) {
	if !f.dirs[p] {
		return nil, ErrNotDir
	}
	var out []string
	for fp := range f.files {
		if len(fp) > len(p)+1 && fp[:len(p)+1] == p+`\` {
			out = append(out, fp[len(p)+1:])
		}
	}
	return out, nil
}

func TestFSStrategy_TemplateHitFB(t *testing.T) {
	fs := &fakeFS{
		files: map[string]int64{`C:\Datasus\CNES\CNES.GDB`: 1024},
	}
	drives := func() []string { return []string{`C:`} }
	cands := FilesystemHits(context.Background(), ProfileFor(SourceCNES), fs, drives)
	require.Len(t, cands, 1)
	require.Equal(t, `C:\Datasus\CNES\CNES.GDB`, cands[0].Path)
	require.Equal(t, StrategyFSTemplate, cands[0].Strategy)
	require.Equal(t, 60, cands[0].Score)
}

func TestFSStrategy_MultiDriveMultiTemplate(t *testing.T) {
	fs := &fakeFS{
		files: map[string]int64{
			`C:\CNES\CNES.GDB`:         500,
			`D:\Datasus\CNES\CNES.GDB`: 2048,
		},
	}
	drives := func() []string { return []string{`C:`, `D:`} }
	cands := FilesystemHits(context.Background(), ProfileFor(SourceCNES), fs, drives)
	require.Len(t, cands, 2)
	paths := []string{cands[0].Path, cands[1].Path}
	require.Contains(t, paths, `C:\CNES\CNES.GDB`)
	require.Contains(t, paths, `D:\Datasus\CNES\CNES.GDB`)
}

func TestFSStrategy_NoHits(t *testing.T) {
	fs := &fakeFS{}
	drives := func() []string { return []string{`C:`} }
	cands := FilesystemHits(context.Background(), ProfileFor(SourceCNES), fs, drives)
	require.Empty(t, cands)
}

func TestFSStrategy_SIADirWithExpectedDBFs(t *testing.T) {
	fs := &fakeFS{
		dirs: map[string]bool{`C:\Datasus\SIA`: true},
		files: map[string]int64{
			`C:\Datasus\SIA\S_APA0125.DBF`: 100,
			`C:\Datasus\SIA\S_BPI0125.DBF`: 100,
			`C:\Datasus\SIA\CADMUN.DBF`:    50,
		},
	}
	drives := func() []string { return []string{`C:`} }
	cands := FilesystemHits(context.Background(), ProfileFor(SourceSIA), fs, drives)
	require.Len(t, cands, 1)
	require.Equal(t, `C:\Datasus\SIA`, cands[0].Path)
	require.Equal(t, 70, cands[0].Score, "60 base + 10 SIA bonus")
}

func TestFSStrategy_SIADirMissingExpectedDBFs(t *testing.T) {
	fs := &fakeFS{
		dirs:  map[string]bool{`C:\Datasus\SIA`: true},
		files: map[string]int64{`C:\Datasus\SIA\unrelated.DBF`: 10},
	}
	drives := func() []string { return []string{`C:`} }
	cands := FilesystemHits(context.Background(), ProfileFor(SourceSIA), fs, drives)
	require.Empty(t, cands, "SIA dir without expected DBFs should not score")
}

func TestFSStrategy_CtxCancelled(t *testing.T) {
	fs := &fakeFS{files: map[string]int64{`C:\CNES\CNES.GDB`: 1}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cands := FilesystemHits(ctx, ProfileFor(SourceCNES), fs,
		func() []string { return []string{`C:`} })
	require.Empty(t, cands)
}
