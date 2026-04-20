//go:build !windows

package main

func detectAutoMode() int {
	return cmdRun(nil)
}
