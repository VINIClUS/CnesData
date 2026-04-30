// Package auth — ca_pin: embedded root CA used as TLS trust anchor.
//
// `var` (not `const`) so tests can override via SetCAPinPEM in
// export_test.go. Production binaries overlay this file with the
// real root CA before `go build` (see apps/dump_agent_go/CLAUDE.md).
package auth

import _ "embed"

//go:embed root_ca.pem
var CAPinPEM []byte
