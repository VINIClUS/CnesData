//go:build go1.18

package extractor_test

import (
	"testing"
	"unicode/utf8"

	"github.com/cnesdata/dumpagent/internal/extractor"
)

func FuzzSanitizeString(f *testing.F) {
	seeds := []string{
		"Atenção Básica",
		"São Paulo",
		"Hospital João XXIII",
		"",
		"\xc7\xe3o",
		"caf\x82 com leite",
	}
	for _, s := range seeds {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, input string) {
		out, dirty := extractor.SanitizeString(input)
		if !utf8.ValidString(out) {
			t.Fatalf("sanitize produced invalid UTF-8: %q -> %q", input, out)
		}
		if dirty < 0 {
			t.Fatalf("negative dirty count: %q -> %d", input, dirty)
		}
		out2, dirty2 := extractor.SanitizeString(out)
		if out2 != out {
			t.Fatalf("not idempotent: %q -> %q -> %q", input, out, out2)
		}
		if dirty2 != 0 {
			t.Fatalf("idempotent pass reported dirty=%d: %q -> %q", dirty2, input, out)
		}
	})
}
