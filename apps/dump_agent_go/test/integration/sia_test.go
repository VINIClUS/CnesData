//go:build integration

package integration_test

import (
	"path/filepath"
	"testing"

	"github.com/cnesdata/dumpagent/internal/extractor"
)

func TestSIA_SyntheticFixtures(t *testing.T) {
	dir := filepath.Join("fixtures", "sia_synthetic")
	result, err := extractor.ExtractSIA(dir, "202601",
		[]string{"SIA_APA", "SIA_BPI", "SIA_BPIHST", "DIM_SIGTAP", "DIM_MUNICIPIO"})
	if err != nil {
		t.Fatalf("extract: %v", err)
	}

	cases := []struct {
		name string
		got  int
		want int
	}{
		{"APA", len(result.APA), 4},
		{"BPI", len(result.BPI), 8},
		{"BPIHST", len(result.BPIHST), 12},
		{"SIGTAP", len(result.SIGTAP), 4},
		{"CADMUN", len(result.CADMUN), 2},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("%s count=%d want=%d", c.name, c.got, c.want)
		}
	}
}
