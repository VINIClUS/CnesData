// Package upload HTTP PUT streaming para presigned URLs.
package upload

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"github.com/cnesdata/dumpagent/internal/integrity"
	"github.com/cnesdata/dumpagent/internal/manifest"
	"github.com/cnesdata/dumpagent/internal/obs"
)

// RawSpool referencia um Parquet sincronizado em disco.
type RawSpool struct {
	Name      string
	SizeBytes int64
	SHA256    string
}

// RawSpoolUpload vincula arquivo, destino e manifesto imutável.
type RawSpoolUpload struct {
	Directory    string
	Name         string
	URL          string
	FencingToken uint64
	Manifest     manifest.Raw
}

// PrepareRawSpool grava por streaming e publica o arquivo completo sincronizado.
func PrepareRawSpool(ctx context.Context, directory string,
	write func(io.Writer) error,
) (result RawSpool, err error) {
	if directory == "" {
		return result, errors.New("raw_spool=unconfigured")
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return result, err
	}
	file, err := os.CreateTemp(directory, "raw-*.parquet")
	if err != nil {
		return result, err
	}
	defer func() {
		_ = file.Close()
		if err != nil {
			_ = os.Remove(file.Name())
		}
	}()
	result.SizeBytes, result.SHA256, err = writeRawSpool(ctx, file, write)
	if err != nil {
		return result, err
	}
	if err := file.Sync(); err != nil {
		return result, err
	}
	if err := file.Close(); err != nil {
		return result, err
	}
	if err := syncSpoolDirectory(directory); err != nil {
		return result, err
	}
	result.Name = filepath.Base(file.Name())
	return result, nil
}

func writeRawSpool(ctx context.Context, file *os.File,
	write func(io.Writer) error,
) (int64, string, error) {
	reader, pipe := io.Pipe()
	stop := context.AfterFunc(ctx, func() { _ = reader.CloseWithError(ctx.Err()) })
	defer stop()
	producer := obs.SafeGo(func() error {
		defer pipe.Close()
		err := write(pipe)
		_ = pipe.CloseWithError(err)
		return err
	}, "raw_spool")
	teed, digest := integrity.SHA256TeeReader(reader)
	size, err := io.Copy(file, teed)
	_ = reader.CloseWithError(err)
	return size, digest.SumHex(), errors.Join(err, <-producer, ctx.Err())
}

// PutRawSpool valida tamanho/hash antes do PUT e verifica o corpo consumido.
func PutRawSpool(ctx context.Context, uploader RawUploader,
	request RawSpoolUpload,
) (int64, error) {
	if uploader == nil {
		return 0, errors.New("raw_uploader=unconfigured")
	}
	path, err := rawSpoolPath(request.Directory, request.Name)
	if err != nil {
		return 0, err
	}
	file, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer file.Close()
	if err := verifyRawSpool(file, request.Manifest); err != nil {
		return 0, err
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}
	teed, hash := integrity.SHA256TeeReader(file)
	size, err := uploader.PutRaw(ctx, RawPutRequest{URL: request.URL, Body: teed,
		FencingToken: request.FencingToken, ObjectKey: request.Manifest.ObjectKey})
	if err != nil {
		return 0, err
	}
	if size != request.Manifest.SizeBytes || hash.SumHex() != request.Manifest.ObjectSHA256 {
		return 0, errors.New("raw_spool=upload_integrity_mismatch")
	}
	return size, nil
}

func verifyRawSpool(file *os.File, raw manifest.Raw) error {
	teed, hash := integrity.SHA256TeeReader(file)
	size, err := io.Copy(io.Discard, teed)
	if err != nil {
		return err
	}
	if size != raw.SizeBytes || hash.SumHex() != raw.ObjectSHA256 {
		return errors.New("raw_spool=integrity_mismatch")
	}
	return nil
}

// RemoveRawSpool remove somente o arquivo indicado.
func RemoveRawSpool(directory, name string) error {
	path, err := rawSpoolPath(directory, name)
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return syncSpoolDirectory(directory)
}

func rawSpoolPath(directory, name string) (string, error) {
	if directory == "" || filepath.Base(name) != name || !strings.HasSuffix(name, ".parquet") {
		return "", errors.New("raw_spool=invalid_path")
	}
	return filepath.Join(directory, name), nil
}

func syncSpoolDirectory(directory string) error {
	return syncSpoolDirectoryForOS(directory, runtime.GOOS, syncDirectory)
}

func syncSpoolDirectoryForOS(directory, goos string, sync func(string) error) error {
	if goos == "windows" {
		return nil
	}
	return sync(directory)
}

func syncDirectory(directory string) error {
	file, err := os.Open(directory)
	if err != nil {
		return err
	}
	defer file.Close()
	return file.Sync()
}

// Uploader contrato para upload streaming.
type Uploader interface {
	Put(ctx context.Context, url string, body io.Reader, contentType string) (int64, error)
}

// RawPutRequest define o destino e a identidade do upload raw.
type RawPutRequest struct {
	URL          string
	Body         io.Reader
	FencingToken uint64
	ObjectKey    string
}

// RawUploader envia Parquet raw com identidade de fencing.
type RawUploader interface {
	PutRaw(context.Context, RawPutRequest) (int64, error)
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
func (h *HTTP) Put(
	ctx context.Context, url string, body io.Reader, contentType string,
) (int64, error) {
	cr := &countingReader{r: body}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, cr)
	if err != nil {
		return 0, err
	}
	req.ContentLength = -1
	req.Header.Set("Content-Type", contentType)
	return h.send(req, cr)
}

// PutRaw envia o corpo por streaming com os cabeçalhos do contrato raw.
func (h *HTTP) PutRaw(ctx context.Context, input RawPutRequest) (int64, error) {
	cr := &countingReader{r: input.Body}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, input.URL, cr)
	if err != nil {
		return 0, err
	}
	req.ContentLength = -1
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-Fencing-Token", strconv.FormatUint(input.FencingToken, 10))
	req.Header.Set("X-Object-Key", input.ObjectKey)
	return h.send(req, cr)
}

func (h *HTTP) send(req *http.Request, cr *countingReader) (int64, error) {
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
