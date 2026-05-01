//go:build windows

package service

import (
	"errors"
	"strings"
	"syscall"

	"golang.org/x/sys/windows/svc/eventlog"
)

type registryWriter interface {
	Install(name string, eventsSupported uint32) error
	Remove(name string) error
}

type defaultRegistryWriter struct{}

func (defaultRegistryWriter) Install(name string, types uint32) error {
	return eventlog.InstallAsEventCreate(name, types)
}

func (defaultRegistryWriter) Remove(name string) error {
	return eventlog.Remove(name)
}

// Compile-time assertion: signature drift on registryWriter fails at build.
var _ registryWriter = defaultRegistryWriter{}

// registryWriterFactory is overridable in tests.
var registryWriterFactory = func() registryWriter { return defaultRegistryWriter{} }

const eventTypesSupported = eventlog.Info | eventlog.Warning | eventlog.Error

// RegisterEventSource creates the named source under the Application log.
// Idempotent: a "registry key already exists" error is treated as success.
// Requires Admin (HKLM write); call from elevated install path only.
//
// Note: upstream eventlog.InstallAsEventCreate constructs the "already exists"
// error via errors.New() with a fixed English string (see
// golang.org/x/sys/windows/svc/eventlog.InstallAsEventCreate). Substring
// match is locale-independent for the same reason.
func RegisterEventSource(name string) error {
	rw := registryWriterFactory()
	err := rw.Install(name, eventTypesSupported)
	if err == nil {
		return nil
	}
	if strings.Contains(err.Error(), "registry key already exists") {
		return nil
	}
	return err
}

// RemoveEventSource deletes the named source. Not-found = success (idempotent).
//
// Uses errors.Is(err, syscall.ERROR_FILE_NOT_FOUND) for locale-independence:
// upstream eventlog.Remove propagates the raw syscall.Errno from
// registry.DeleteKey, so the comparison works on any Windows locale
// (English, Portuguese, etc.). Production target is pt-BR Windows.
func RemoveEventSource(name string) error {
	rw := registryWriterFactory()
	err := rw.Remove(name)
	if err == nil || errors.Is(err, syscall.ERROR_FILE_NOT_FOUND) {
		return nil
	}
	return err
}
