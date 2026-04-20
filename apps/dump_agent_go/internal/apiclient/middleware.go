package apiclient

import (
	"context"
	"net/http"
)

// WithTenantID injeta header X-Tenant-Id.
func WithTenantID(tid string) RequestEditorFn {
	return func(_ context.Context, req *http.Request) error {
		req.Header.Set("X-Tenant-Id", tid)
		return nil
	}
}

// WithMachineID injeta header X-Machine-Id.
func WithMachineID(mid string) RequestEditorFn {
	return func(_ context.Context, req *http.Request) error {
		req.Header.Set("X-Machine-Id", mid)
		return nil
	}
}
