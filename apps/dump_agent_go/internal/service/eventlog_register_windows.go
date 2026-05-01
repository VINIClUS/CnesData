//go:build windows

package service

import (
	"strings"

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

// registryWriterFactory is overridable in tests.
var registryWriterFactory = func() registryWriter { return defaultRegistryWriter{} }

const eventTypesSupported = eventlog.Info | eventlog.Warning | eventlog.Error

// RegisterEventSource creates the named source under the Application log.
// Idempotent: a "registry key already exists" error is treated as success.
// Requires Admin (HKLM write); call from elevated install path only.
func RegisterEventSource(name string) error {
	rw := registryWriterFactory()
	err := rw.Install(name, eventTypesSupported)
	if err == nil {
		return nil
	}
	if strings.Contains(err.Error(), "already exists") {
		return nil
	}
	return err
}

// RemoveEventSource deletes the named source. Not-found = success.
func RemoveEventSource(name string) error {
	rw := registryWriterFactory()
	err := rw.Remove(name)
	if err == nil {
		return nil
	}
	low := strings.ToLower(err.Error())
	if strings.Contains(low, "cannot find") || strings.Contains(low, "not exist") {
		return nil
	}
	return err
}
