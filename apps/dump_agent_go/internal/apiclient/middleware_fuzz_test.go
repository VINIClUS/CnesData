//go:build go1.18

package apiclient_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cnesdata/dumpagent/internal/apiclient"
)

func FuzzWithTenantID(f *testing.F) {
	f.Add("354130")
	f.Add("")
	f.Add("\x00\x01")
	f.Add("tenant with spaces")
	f.Add("\r\ninjected: header")
	f.Add("\U0001f3e5")
	f.Fuzz(func(t *testing.T, tid string) {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("panic on tid=%q: %v", tid, r)
			}
		}()
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		editor := apiclient.WithTenantID(tid)
		_ = editor(context.Background(), req)
	})
}

func FuzzWithMachineID(f *testing.F) {
	f.Add("abc12345")
	f.Add("")
	f.Add("\x00\x01")
	f.Add("machine with spaces")
	f.Add("\r\ninjected: header")
	f.Add("\U0001f3e5")
	f.Fuzz(func(t *testing.T, mid string) {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("panic on mid=%q: %v", mid, r)
			}
		}()
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		editor := apiclient.WithMachineID(mid)
		_ = editor(context.Background(), req)
	})
}
