//go:build windows

package service

import (
	"errors"
	"strings"
	"testing"
)

type fakeRegistryWriter struct {
	installed  []string
	removed    []string
	installErr error
	removeErr  error
}

func (f *fakeRegistryWriter) Install(name string, _ uint32) error {
	f.installed = append(f.installed, name)
	return f.installErr
}
func (f *fakeRegistryWriter) Remove(name string) error {
	f.removed = append(f.removed, name)
	return f.removeErr
}

func TestRegisterEventSource_HappyPath(t *testing.T) {
	fake := &fakeRegistryWriter{}
	prev := registryWriterFactory
	registryWriterFactory = func() registryWriter { return fake }
	t.Cleanup(func() { registryWriterFactory = prev })

	if err := RegisterEventSource("DumpAgent"); err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(fake.installed) != 1 || fake.installed[0] != "DumpAgent" {
		t.Fatalf("Install not called for DumpAgent: %+v", fake.installed)
	}
}

func TestRegisterEventSource_AlreadyExistsIsIdempotent(t *testing.T) {
	fake := &fakeRegistryWriter{
		installErr: errors.New("registry key already exists"),
	}
	prev := registryWriterFactory
	registryWriterFactory = func() registryWriter { return fake }
	t.Cleanup(func() { registryWriterFactory = prev })

	if err := RegisterEventSource("DumpAgent"); err != nil {
		t.Fatalf("idempotent register must return nil, got %v", err)
	}
}

func TestRegisterEventSource_OtherErrorPropagates(t *testing.T) {
	fake := &fakeRegistryWriter{
		installErr: errors.New("access denied"),
	}
	prev := registryWriterFactory
	registryWriterFactory = func() registryWriter { return fake }
	t.Cleanup(func() { registryWriterFactory = prev })

	err := RegisterEventSource("DumpAgent")
	if err == nil || !strings.Contains(err.Error(), "access denied") {
		t.Fatalf("expected access denied propagation, got %v", err)
	}
}

func TestRemoveEventSource_NotFoundIsOK(t *testing.T) {
	fake := &fakeRegistryWriter{
		removeErr: errors.New("the system cannot find the file specified"),
	}
	prev := registryWriterFactory
	registryWriterFactory = func() registryWriter { return fake }
	t.Cleanup(func() { registryWriterFactory = prev })

	if err := RemoveEventSource("DumpAgent"); err != nil {
		t.Fatalf("not-found should be nil, got %v", err)
	}
}

func TestRemoveEventSource_PropagatesOtherErrors(t *testing.T) {
	fake := &fakeRegistryWriter{
		removeErr: errors.New("access denied"),
	}
	prev := registryWriterFactory
	registryWriterFactory = func() registryWriter { return fake }
	t.Cleanup(func() { registryWriterFactory = prev })

	err := RemoveEventSource("DumpAgent")
	if err == nil || !strings.Contains(err.Error(), "access denied") {
		t.Fatalf("expected access denied, got %v", err)
	}
}
