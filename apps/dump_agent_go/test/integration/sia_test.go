//go:build integration

package integration_test

import (
	"path/filepath"
	"testing"

	"github.com/cnesdata/dumpagent/internal/extractor"
)

func TestSIA_SyntheticFixtures(t *testing.T) {
	dir := filepath.Join("fixtures", "sia_synthetic")
	result, err := extractor.ExtractSIA(dir)
	if err != nil {
		t.Fatalf("extract: %v", err)
	}

	cases := []struct {
		name string
		got  int
		min  int
	}{
		{"APA", len(result.APA), 3},
		{"BPI", len(result.BPI), 5},
		{"BPIHST", len(result.BPIHST), 8},
		{"CDN", len(result.CDN), 3},
		{"CADMUN", len(result.CADMUN), 2},
	}
	for _, c := range cases {
		if c.got < c.min {
			t.Errorf("%s count=%d want>=%d", c.name, c.got, c.min)
		}
	}
}
