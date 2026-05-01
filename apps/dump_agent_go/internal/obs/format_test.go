package obs

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func makeRecord(level slog.Level, msg string, attrs ...slog.Attr) slog.Record {
	r := slog.NewRecord(time.Time{}, level, msg, 0)
	r.AddAttrs(attrs...)
	return r
}

func TestFormatCompact_StableKeyOrder(t *testing.T) {
	r := makeRecord(slog.LevelWarn, "upload_failed",
		slog.String("zeta", "1"),
		slog.String("alpha", "2"),
		slog.Int("middle", 3),
	)
	got := FormatCompact(r)
	want := `level=warn msg=upload_failed alpha=2 middle=3 zeta=1`
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestFormatCompact_EscapesSpecialChars(t *testing.T) {
	r := makeRecord(slog.LevelError, "err",
		slog.String("reason", "line1\nline2 \"quoted\"\ttab"),
	)
	got := FormatCompact(r)
	want := `level=error msg=err reason="line1\nline2 \"quoted\"\ttab"`
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestFormatCompact_GroupFlatten(t *testing.T) {
	r := makeRecord(slog.LevelInfo, "boot",
		slog.Group("db", slog.String("host", "h"), slog.Int("port", 5432)),
	)
	got := FormatCompact(r)
	want := `level=info msg=boot db.host=h db.port=5432`
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestFormatCompact_TruncatesAt8KB(t *testing.T) {
	big := strings.Repeat("x", 9000)
	r := makeRecord(slog.LevelWarn, "big", slog.String("blob", big))
	got := FormatCompact(r)
	if len(got) > 8200 {
		t.Fatalf("expected <=8200 chars, got %d", len(got))
	}
	if !strings.Contains(got, "...truncated") {
		t.Fatalf("expected truncation marker, got %q", got[:80])
	}
}

var _ = context.Background // silence unused import in stripped scenarios
