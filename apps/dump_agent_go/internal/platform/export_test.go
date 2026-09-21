package platform

// Exports for black-box tests in platform_test package.
// File name suffix `_test.go` ensures these symbols are only visible
// during `go test` builds — they are not in the production API.

var Export = struct {
	WindowsAppDataDir func(string) string
}{
	WindowsAppDataDir: windowsAppDataDir,
}
