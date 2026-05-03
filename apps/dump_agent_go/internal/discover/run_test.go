package discover

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRun_Returns4Sources(t *testing.T) {
	fs := &fakeFS{
		files: map[string]int64{
			`C:\Datasus\CNES\CNES.GDB`:     1000,
			`C:\Datasus\BPAMAG\BPAMAG.GDB`: 60 * 1024 * 1024,
		},
		dirs: map[string]bool{`C:\Datasus\SIA`: true},
	}
	fs.files[`C:\Datasus\SIA\S_APA01.DBF`] = 1
	fs.files[`C:\Datasus\SIA\S_BPI01.DBF`] = 1
	fs.files[`C:\Datasus\SIA\CADMUN.DBF`] = 1
	cfg := RunConfig{
		FS:          fs,
		Drives:      func() []string { return []string{`C:`} },
		RegistryFn:  func(_ Profile) []RegistryHit { return nil },
		PerProbeTTL: 5 * time.Second,
	}
	results := Run(context.Background(), cfg)
	require.Len(t, results, 4)
	bySrc := map[SourceID]SourceResult{}
	for _, r := range results {
		bySrc[r.Source] = r
	}
	require.Equal(t, `C:\Datasus\CNES\CNES.GDB`, bySrc[SourceCNES].Top.Path)
	require.Equal(t, Candidate{}, bySrc[SourceSIHD].Top, "no SIHD on this fakefs")
	require.Equal(t, `C:\Datasus\BPAMAG\BPAMAG.GDB`, bySrc[SourceBPA].Top.Path)
	require.Equal(t, 65, bySrc[SourceBPA].Top.Score, "60 fs_template + 5 fat-bonus")
	require.Equal(t, `C:\Datasus\SIA`, bySrc[SourceSIA].Top.Path)
}

func TestRun_PerProbeTimeoutDoesNotKillOthers(t *testing.T) {
	fs := &slowFS{
		inner:  &fakeFS{files: map[string]int64{`C:\CNES\CNES.GDB`: 100}},
		delay:  200 * time.Millisecond,
		filter: SourceSIHD,
	}
	cfg := RunConfig{
		FS:          fs,
		Drives:      func() []string { return []string{`C:`} },
		RegistryFn:  func(_ Profile) []RegistryHit { return nil },
		PerProbeTTL: 50 * time.Millisecond,
	}
	results := Run(context.Background(), cfg)
	require.Len(t, results, 4)
	bySrc := map[SourceID]SourceResult{}
	for _, r := range results {
		bySrc[r.Source] = r
	}
	require.Equal(t, `C:\CNES\CNES.GDB`, bySrc[SourceCNES].Top.Path,
		"CNES probe completes even though SIHD probe is slow")
}

func TestRun_CtxCancelPropagates(t *testing.T) {
	fs := &fakeFS{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cfg := RunConfig{
		FS:          fs,
		Drives:      func() []string { return []string{`C:`} },
		RegistryFn:  func(_ Profile) []RegistryHit { return nil },
		PerProbeTTL: time.Second,
	}
	results := Run(ctx, cfg)
	require.Len(t, results, 4, "always emits 4 results, possibly empty")
	for _, r := range results {
		require.Equal(t, Candidate{}, r.Top)
	}
}

// slowFS injects per-source delay (matched by filename heuristic) to test
// per-probe timeout.
type slowFS struct {
	inner  *fakeFS
	delay  time.Duration
	filter SourceID
}

func (f *slowFS) Stat(p string) (FileInfo, error) {
	if matchesSource(f.filter, p) {
		time.Sleep(f.delay)
	}
	return f.inner.Stat(p)
}

func (f *slowFS) ReadDir(p string) ([]string, error) {
	if matchesSource(f.filter, p) {
		time.Sleep(f.delay)
	}
	return f.inner.ReadDir(p)
}

func matchesSource(s SourceID, path string) bool {
	switch s {
	case SourceSIHD:
		return strings.Contains(path, "SIHD")
	case SourceCNES:
		return strings.Contains(path, "CNES")
	case SourceBPA:
		return strings.Contains(path, "BPA")
	case SourceSIA:
		return strings.Contains(path, "SIA")
	default:
		return false
	}
}
