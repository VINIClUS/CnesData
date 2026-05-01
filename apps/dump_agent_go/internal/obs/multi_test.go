package obs

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"
)

type recordingHandler struct {
	enabled  bool
	records  []slog.Record
	attrs    []slog.Attr
	groups   []string
	failWith error
}

func (h *recordingHandler) Enabled(_ context.Context, _ slog.Level) bool { return h.enabled }

func (h *recordingHandler) Handle(_ context.Context, r slog.Record) error {
	h.records = append(h.records, r)
	return h.failWith
}

func (h *recordingHandler) WithAttrs(a []slog.Attr) slog.Handler {
	cp := *h
	cp.attrs = append(cp.attrs[:0:0], h.attrs...)
	cp.attrs = append(cp.attrs, a...)
	return &cp
}

func (h *recordingHandler) WithGroup(name string) slog.Handler {
	cp := *h
	cp.groups = append(cp.groups[:0:0], h.groups...)
	cp.groups = append(cp.groups, name)
	return &cp
}

func TestMultiHandler_FansOutToAll(t *testing.T) {
	a := &recordingHandler{enabled: true}
	b := &recordingHandler{enabled: true}
	c := &recordingHandler{enabled: true}
	mh := NewMultiHandler(a, b, c)

	rec := slog.NewRecord(time.Time{}, slog.LevelWarn, "x", 0)
	if err := mh.Handle(context.Background(), rec); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	for i, h := range []*recordingHandler{a, b, c} {
		if got := len(h.records); got != 1 {
			t.Errorf("child[%d] received %d records, want 1", i, got)
		}
	}
}

func TestMultiHandler_OneChildErrorDoesNotDropOthers(t *testing.T) {
	boom := errors.New("boom")
	a := &recordingHandler{enabled: true, failWith: boom}
	b := &recordingHandler{enabled: true}
	mh := NewMultiHandler(a, b)

	rec := slog.NewRecord(time.Time{}, slog.LevelError, "x", 0)
	err := mh.Handle(context.Background(), rec)
	if err == nil || !errors.Is(err, boom) {
		t.Fatalf("want errors.Is boom, got %v", err)
	}
	if len(b.records) != 1 {
		t.Fatalf("sibling child must still receive record, got %d", len(b.records))
	}
}

func TestMultiHandler_EnabledTrueIfAnyChild(t *testing.T) {
	a := &recordingHandler{enabled: false}
	b := &recordingHandler{enabled: true}
	mh := NewMultiHandler(a, b)
	if !mh.Enabled(context.Background(), slog.LevelWarn) {
		t.Fatal("expected Enabled=true when any child enabled")
	}
}

func TestMultiHandler_EnabledFalseWhenNoChild(t *testing.T) {
	a := &recordingHandler{enabled: false}
	b := &recordingHandler{enabled: false}
	mh := NewMultiHandler(a, b)
	if mh.Enabled(context.Background(), slog.LevelWarn) {
		t.Fatal("expected Enabled=false when no child enabled")
	}
}

func TestMultiHandler_WithAttrsPropagates(t *testing.T) {
	a := &recordingHandler{enabled: true}
	b := &recordingHandler{enabled: true}
	mh := NewMultiHandler(a, b)
	derived := mh.WithAttrs([]slog.Attr{slog.String("k", "v")})

	mh2, ok := derived.(*MultiHandler)
	if !ok {
		t.Fatalf("expected *MultiHandler, got %T", derived)
	}
	for i, child := range mh2.children {
		rh := child.(*recordingHandler)
		if len(rh.attrs) != 1 || rh.attrs[0].Key != "k" {
			t.Errorf("child[%d]: WithAttrs not propagated, got %+v", i, rh.attrs)
		}
	}
}

func TestMultiHandler_WithGroupPropagates(t *testing.T) {
	a := &recordingHandler{enabled: true}
	mh := NewMultiHandler(a)
	derived := mh.WithGroup("g1")
	mh2, _ := derived.(*MultiHandler)
	rh := mh2.children[0].(*recordingHandler)
	if len(rh.groups) != 1 || rh.groups[0] != "g1" {
		t.Fatalf("WithGroup not propagated: %+v", rh.groups)
	}
}
