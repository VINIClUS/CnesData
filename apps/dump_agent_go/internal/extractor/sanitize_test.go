package extractor_test

import (
	"testing"

	"github.com/cnesdata/dumpagent/internal/extractor"
	"github.com/stretchr/testify/require"
)

func TestSanitize_ValidUTF8Unchanged(t *testing.T) {
	clean, dirty := extractor.SanitizeString("Atenção Básica")
	require.Equal(t, "Atenção Básica", clean)
	require.Equal(t, 0, dirty)
}

func TestSanitize_ReplacesInvalidBytes(t *testing.T) {
	raw := "Aten\xE7\xE3o"
	clean, dirty := extractor.SanitizeString(raw)
	require.Equal(t, "Aten??o", clean)
	require.Equal(t, 2, dirty)
}

func TestSanitize_EmptyString(t *testing.T) {
	clean, dirty := extractor.SanitizeString("")
	require.Equal(t, "", clean)
	require.Equal(t, 0, dirty)
}

func TestSanitize_AllInvalid(t *testing.T) {
	clean, dirty := extractor.SanitizeString("\xFF\xFE\xFD")
	require.Equal(t, "???", clean)
	require.Equal(t, 3, dirty)
}

func TestSanitize_MixedValidInvalid(t *testing.T) {
	clean, dirty := extractor.SanitizeString("Nome\xFFSobrenome")
	require.Equal(t, "Nome?Sobrenome", clean)
	require.Equal(t, 1, dirty)
}
