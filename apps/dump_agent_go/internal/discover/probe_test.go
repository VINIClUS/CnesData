package discover

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProbeFB_AggregatesRegistryAndFS(t *testing.T) {
	fs := &fakeFS{
		files: map[string]int64{
			`C:\Datasus\CNES\CNES.GDB`: 5000,
			`D:\CNES\CNES.GDB`:         1000,
		},
	}
	drives := func() []string { return []string{`C:`, `D:`} }
	stubRegistry := func(p Profile) []RegistryHit {
		return []RegistryHit{{Path: `C:\Datasus\CNES\CNES.GDB`}}
	}
	deps := probeDeps{FS: fs, Drives: drives, Reg: stubRegistry}
	res := probeFB(context.Background(), ProfileFor(SourceCNES), deps)
	require.Equal(t, SourceCNES, res.Source)
	require.Equal(t, `C:\Datasus\CNES\CNES.GDB`, res.Top.Path)
	require.Equal(t, 80, res.Top.Score, "registry hit + file exists")
	require.Len(t, res.Alternates, 2,
		"includes fs_template hit at C: and D: (registry path collides with one)")
}

func TestProbeFB_RegistryEmpty(t *testing.T) {
	fs := &fakeFS{
		files: map[string]int64{`C:\CNES\CNES.GDB`: 100},
	}
	drives := func() []string { return []string{`C:`} }
	stubRegistry := func(p Profile) []RegistryHit { return nil }
	deps := probeDeps{FS: fs, Drives: drives, Reg: stubRegistry}
	res := probeFB(context.Background(), ProfileFor(SourceCNES), deps)
	require.Equal(t, `C:\CNES\CNES.GDB`, res.Top.Path)
	require.Equal(t, 60, res.Top.Score)
}

func TestProbeFB_NoCandidates(t *testing.T) {
	fs := &fakeFS{}
	drives := func() []string { return []string{`C:`} }
	stubRegistry := func(p Profile) []RegistryHit { return nil }
	deps := probeDeps{FS: fs, Drives: drives, Reg: stubRegistry}
	res := probeFB(context.Background(), ProfileFor(SourceSIHD), deps)
	require.Equal(t, Candidate{}, res.Top)
	require.Empty(t, res.Alternates)
}

func TestProbeSIA_DirWithExpectedDBFs(t *testing.T) {
	fs := &fakeFS{
		dirs: map[string]bool{`C:\Datasus\SIA`: true},
		files: map[string]int64{
			`C:\Datasus\SIA\S_APA0125.DBF`:    100,
			`C:\Datasus\SIA\S_BPI0125.DBF`:    100,
			`C:\Datasus\SIA\S_BPIHST0125.DBF`: 100,
			`C:\Datasus\SIA\CADMUN.DBF`:       100,
		},
	}
	drives := func() []string { return []string{`C:`} }
	stubRegistry := func(p Profile) []RegistryHit { return nil }
	deps := probeDeps{FS: fs, Drives: drives, Reg: stubRegistry}
	res := probeSIA(context.Background(), deps)
	require.Equal(t, SourceSIA, res.Source)
	require.Equal(t, `C:\Datasus\SIA`, res.Top.Path)
	require.Equal(t, 70, res.Top.Score)
}
