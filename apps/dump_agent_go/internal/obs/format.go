package obs

import (
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"unicode/utf8"
)

const maxValueLen = 8000

// FormatCompact renders a slog.Record as a single-line key=value string.
// Order: level, msg, then attrs sorted alphabetically. Group attrs flatten
// to dotted keys (db.host). Special chars escaped via Go quoted form.
// Values longer than maxValueLen are truncated with "...truncated" suffix.
func FormatCompact(r slog.Record) string {
	var b strings.Builder
	b.WriteString("level=")
	b.WriteString(levelString(r.Level))
	b.WriteString(" msg=")
	b.WriteString(formatValue(r.Message))

	pairs := make([]string, 0, r.NumAttrs())
	r.Attrs(func(a slog.Attr) bool {
		pairs = appendAttr(pairs, "", a)
		return true
	})
	sort.Strings(pairs)
	for _, p := range pairs {
		b.WriteByte(' ')
		b.WriteString(p)
	}
	return b.String()
}

func levelString(l slog.Level) string {
	switch {
	case l >= slog.LevelError:
		return "error"
	case l >= slog.LevelWarn:
		return "warn"
	case l >= slog.LevelInfo:
		return "info"
	default:
		return "debug"
	}
}

func appendAttr(out []string, prefix string, a slog.Attr) []string {
	key := a.Key
	if prefix != "" {
		key = prefix + "." + key
	}
	if a.Value.Kind() == slog.KindGroup {
		for _, sub := range a.Value.Group() {
			out = appendAttr(out, key, sub)
		}
		return out
	}
	return append(out, key+"="+formatValue(a.Value.String()))
}

func formatValue(s string) string {
	if len(s) > maxValueLen {
		s = truncateAtRuneBoundary(s, maxValueLen) + "...truncated"
	}
	if needsQuote(s) {
		return fmt.Sprintf("%q", s)
	}
	return s
}

func truncateAtRuneBoundary(s string, n int) string {
	if n >= len(s) {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}

func needsQuote(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c < 0x20 || c == 0x7F || c == ' ' || c == '"' || c == '\\' {
			return true
		}
	}
	return false
}
