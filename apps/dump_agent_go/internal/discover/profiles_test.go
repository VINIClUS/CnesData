package discover

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProfileFor_AllSourcesDefined(t *testing.T) {
	for _, src := range []SourceID{SourceCNES, SourceSIHD, SourceBPA, SourceSIA} {
		p := ProfileFor(src)
		require.Equal(t, src, p.Source, "source mismatch for %s", src)
		require.NotEmpty(t, p.RegistryVendorKey, "vendor key empty for %s", src)
		require.NotEmpty(t, p.UninstallDisplayMatch, "uninstall match empty for %s", src)
		require.NotEmpty(t, p.FSTemplates, "fs templates empty for %s", src)
	}
}

func TestProfileFor_FBSourcesShareGDBExtension(t *testing.T) {
	for _, src := range []SourceID{SourceCNES, SourceSIHD, SourceBPA} {
		p := ProfileFor(src)
		require.Equal(t, "GDB", strings.ToUpper(strings.TrimPrefix(p.FileExtension, ".")),
			"FB sources expect .GDB extension")
	}
}

func TestProfileFor_CNESFilename(t *testing.T) {
	p := ProfileFor(SourceCNES)
	require.Equal(t, "CNES.GDB", p.PrimaryFilename)
}

func TestProfileFor_BPAFilename(t *testing.T) {
	p := ProfileFor(SourceBPA)
	require.Equal(t, "BPAMAG.GDB", p.PrimaryFilename)
}

func TestProfileFor_SIAExpectedDBFs(t *testing.T) {
	p := ProfileFor(SourceSIA)
	require.GreaterOrEqual(t, len(p.SIAExpectedDBFs), 4)
}

func TestProfileFor_Unknown(t *testing.T) {
	p := ProfileFor(SourceUnknown)
	require.Equal(t, SourceUnknown, p.Source)
}
