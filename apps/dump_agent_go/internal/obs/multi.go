package obs

import (
	"context"
	"errors"
	"log/slog"
)

// MultiHandler fans slog.Handler operations to N children.
// Errors from children are collected via errors.Join.
type MultiHandler struct {
	children []slog.Handler
}

// NewMultiHandler returns a Handler that dispatches to each child.
func NewMultiHandler(children ...slog.Handler) *MultiHandler {
	return &MultiHandler{children: children}
}

func (m *MultiHandler) Enabled(ctx context.Context, l slog.Level) bool {
	for _, c := range m.children {
		if c.Enabled(ctx, l) {
			return true
		}
	}
	return false
}

func (m *MultiHandler) Handle(ctx context.Context, r slog.Record) error {
	var errs []error
	for _, c := range m.children {
		if !c.Enabled(ctx, r.Level) {
			continue
		}
		if err := c.Handle(ctx, r.Clone()); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (m *MultiHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	derived := make([]slog.Handler, len(m.children))
	for i, c := range m.children {
		derived[i] = c.WithAttrs(attrs)
	}
	return &MultiHandler{children: derived}
}

func (m *MultiHandler) WithGroup(name string) slog.Handler {
	derived := make([]slog.Handler, len(m.children))
	for i, c := range m.children {
		derived[i] = c.WithGroup(name)
	}
	return &MultiHandler{children: derived}
}
