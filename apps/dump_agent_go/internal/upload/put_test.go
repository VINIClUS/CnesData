package upload_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/cnesdata/dumpagent/internal/upload"
	"github.com/stretchr/testify/require"
)

func TestPut_Streams(t *testing.T) {
	var body []byte
	var method string
	var contentType string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method = r.Method
		contentType = r.Header.Get("Content-Type")
		body, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	u := upload.NewHTTP(http.DefaultClient)
	n, err := u.Put(context.Background(), srv.URL, strings.NewReader("hello world"), "application/octet-stream")
	require.NoError(t, err)
	require.Equal(t, int64(11), n)
	require.Equal(t, http.MethodPut, method)
	require.Equal(t, "application/octet-stream", contentType)
	require.Equal(t, "hello world", string(body))
}

func TestPut_Returns_HTTPError_On_Non_2xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte("RequestTimeTooSkewed"))
	}))
	defer srv.Close()

	u := upload.NewHTTP(http.DefaultClient)
	_, err := u.Put(context.Background(), srv.URL, strings.NewReader("x"), "application/octet-stream")
	require.Error(t, err)
	require.Contains(t, err.Error(), "403")
	require.Contains(t, err.Error(), "RequestTimeTooSkewed")
}
