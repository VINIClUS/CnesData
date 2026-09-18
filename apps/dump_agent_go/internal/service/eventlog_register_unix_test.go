//go:build !windows

package service

import "testing"

func TestRegisterEventSource_UnixNoOp(t *testing.T) {
	if err := RegisterEventSource("DumpAgent"); err != nil {
		t.Fatalf("unix stub must return nil, got %v", err)
	}
}

func TestRemoveEventSource_UnixNoOp(t *testing.T) {
	if err := RemoveEventSource("DumpAgent"); err != nil {
		t.Fatalf("unix stub must return nil, got %v", err)
	}
}
