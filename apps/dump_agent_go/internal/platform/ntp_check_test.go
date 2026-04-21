package platform_test

import (
	"testing"
	"time"

	"github.com/cnesdata/dumpagent/internal/platform"
	"github.com/stretchr/testify/require"
)

func TestClassifySkew(t *testing.T) {
	cases := []struct {
		skew     time.Duration
		expected platform.SkewLevel
	}{
		{30 * time.Second, platform.SkewOK},
		{2 * time.Minute, platform.SkewWarn},
		{10 * time.Minute, platform.SkewError},
		{2 * time.Hour, platform.SkewFatal},
		{-30 * time.Second, platform.SkewOK},
		{-90 * time.Minute, platform.SkewFatal},
	}
	for _, c := range cases {
		t.Run(c.skew.String(), func(t *testing.T) {
			require.Equal(t, c.expected, platform.ClassifySkew(c.skew))
		})
	}
}
