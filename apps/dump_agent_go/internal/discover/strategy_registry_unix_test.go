//go:build !windows

package discover

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRegistryHits_LinuxStubReturnsNil(t *testing.T) {
	got := RegistryHits(ProfileFor(SourceCNES))
	require.Nil(t, got)
}
