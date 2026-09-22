// Package auth — ca_pin: optional server-CA trust anchor for mTLS clients.
//
// Nil by default: transport.NewMTLSClient and cmd_register's bootstrap
// client fall back to the platform trust store (central_api's cert is
// Let's Encrypt, already trusted by any standard OS/Go cert pool).
// `--ca-pin` overrides this at runtime for a private CA (dev/staging).
// `var` (not `const`) so tests can override via SetCAPinPEM in
// export_test.go.
package auth

var CAPinPEM []byte
