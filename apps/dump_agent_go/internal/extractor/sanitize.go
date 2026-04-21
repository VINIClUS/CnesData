package extractor

import (
	"strings"
	"unicode/utf8"
)

// SanitizeString substitui bytes UTF-8 inválidos por '?'. Retorna string
// limpa + contador de bytes substituídos. Ver §10.1.1 da spec.
func SanitizeString(raw string) (clean string, dirty int) {
	if utf8.ValidString(raw) {
		return raw, 0
	}
	b := []byte(raw)
	var sb strings.Builder
	sb.Grow(len(b))
	for i := 0; i < len(b); {
		r, size := utf8.DecodeRune(b[i:])
		if r == utf8.RuneError && size == 1 {
			sb.WriteByte('?')
			dirty++
			i++
			continue
		}
		sb.WriteRune(r)
		i += size
	}
	return sb.String(), dirty
}
