package extractor

import (
	"strconv"
	"strings"
)

// CompetenciaInt converte Competencia "YYYYMM" para int.
// Retorna 0 se vazio ou inválido.
func (p ExtractionParams) CompetenciaInt() int {
	yyyymm, _ := strconv.Atoi(p.Competencia)
	return yyyymm
}

// SourceType deriva fonte_sistema a partir do prefixo do Intent.
// "cnes_*" → CNES_LOCAL; "sihd_*" → SIHD; outros → "".
func (p ExtractionParams) SourceType() string {
	switch {
	case strings.HasPrefix(p.Intent, "cnes_"):
		return "CNES_LOCAL"
	case strings.HasPrefix(p.Intent, "sihd_"):
		return "SIHD"
	}
	return ""
}
