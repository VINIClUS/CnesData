// Package upload HTTP PUT streaming para presigned URLs.
package upload

import (
	"context"
	"io"
	"net/http"

	"github.com/cnesdata/dumpagent/internal/obs"
)

// Uploader contrato para upload streaming.
type Uploader interface {
	Put(ctx context.Context, url string, body io.Reader, contentType string) (int64, error)
}

// HTTP implementação padrão sobre http.Client.
type HTTP struct {
	client *http.Client
}

// NewHTTP constrói Uploader com client custom ou default.
func NewHTTP(client *http.Client) *HTTP {
	if client == nil {
		client = http.DefaultClient
	}
	return &HTTP{client: client}
}

// Put streaming PUT. body é consumido por io.Pipe reader ou qualquer
// io.Reader; ContentLength = -1 habilita transfer-encoding chunked.
func (h *HTTP) Put(ctx context.Context, url string, body io.Reader, contentType string) (int64, error) {
	cr := &countingReader{r: body}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, cr)
	if err != nil {
		return 0, err
	}
	req.ContentLength = -1
	req.Header.Set("Content-Type", contentType)

	resp, err := h.client.Do(req)
	if err != nil {
		return cr.n, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return cr.n, &obs.HTTPError{StatusCode: resp.StatusCode, Body: string(respBody)}
	}
	return cr.n, nil
}

type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}
