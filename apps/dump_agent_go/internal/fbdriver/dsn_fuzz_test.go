//go:build go1.18

package fbdriver_test

import (
	"testing"

	"github.com/cnesdata/dumpagent/internal/fbdriver"
)

func FuzzBuildDSN(f *testing.F) {
	f.Add("localhost", 3050, "/path/db.fdb", "SYSDBA", "masterkey", "WIN1252")
	f.Add("", 0, "", "", "", "")
	f.Add("h\x00", -1, "p\x00", "u", "w", "c")
	f.Add("127.0.0.1", 65535, "C:/CNES.GDB", "sys", "pw", "")
	f.Add("h", -2147483648, "p", "u", "w", "UTF8")
	f.Fuzz(func(t *testing.T, host string, port int, path, user, pass, charset string) {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("panic on inputs: host=%q port=%d path=%q user=%q charset=%q: %v",
					host, port, path, user, charset, r)
			}
		}()
		_ = fbdriver.BuildDSN(fbdriver.ConnConfig{
			Host:     host,
			Port:     port,
			Path:     path,
			User:     user,
			Password: pass,
			Charset:  charset,
		})
	})
}
