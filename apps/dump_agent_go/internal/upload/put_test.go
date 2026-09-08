package upload

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cnesdata/dumpagent/internal/manifest"
	"github.com/stretchr/testify/require"
)

func TestPutRawEnviaCabecalhosAntesDeConsumirCorpo(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	started := make(chan struct{})
	requests := make(chan *http.Request, 1)
	contents := make(chan string, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests <- r
		close(started)
		body, _ := io.ReadAll(r.Body)
		contents <- string(body)
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()
	var uploader RawUploader = NewHTTP(srv.Client())
	body := &bodyAfterHeaders{ctx: ctx, started: started, r: strings.NewReader("PAR1payloadPAR1")}
	n, err := uploader.PutRaw(ctx, RawPutRequest{
		URL: srv.URL, Body: body, FencingToken: 18446744073709551615,
		ObjectKey: "raw/354130/CNES/2026-01/snapshot/data.parquet",
	})
	require.NoError(t, err)
	require.Equal(t, int64(15), n)
	req := <-requests
	require.Equal(t, http.MethodPut, req.Method)
	require.Equal(t, int64(-1), req.ContentLength)
	require.Equal(t, "application/octet-stream", req.Header.Get("Content-Type"))
	require.Equal(t, "18446744073709551615", req.Header.Get("X-Fencing-Token"))
	require.Equal(t, "raw/354130/CNES/2026-01/snapshot/data.parquet", req.Header.Get("X-Object-Key"))
	require.Equal(t, "PAR1payloadPAR1", <-contents)
}

func TestSpoolDuravelPreservaBytesEValidaAntesDePut(t *testing.T) {
	directory := t.TempDir()
	spool, err := PrepareRawSpool(context.Background(), directory, func(dst io.Writer) error {
		_, err := io.WriteString(dst, "PAR1payloadPAR1")
		return err
	})
	require.NoError(t, err)
	stored, err := os.ReadFile(filepath.Join(directory, spool.Name))
	require.NoError(t, err)
	require.Equal(t, "PAR1payloadPAR1", string(stored))
	calls := 0
	uploader := spoolUploader(func(_ context.Context, request RawPutRequest) (int64, error) {
		calls++
		return io.Copy(io.Discard, request.Body)
	})
	request := RawSpoolUpload{Directory: directory, Name: spool.Name, URL: "upload",
		FencingToken: 1, Manifest: manifest.Raw{SizeBytes: spool.SizeBytes, ObjectSHA256: spool.SHA256}}
	_, err = UploadRawSpool(context.Background(), uploader, request)
	require.NoError(t, err)
	require.Equal(t, 1, calls)
	request.Manifest.SizeBytes++
	_, err = UploadRawSpool(context.Background(), uploader, request)
	require.Error(t, err)
	request.Manifest.SizeBytes--
	require.NoError(t, os.WriteFile(filepath.Join(directory, spool.Name),
		[]byte("PAR1changedPAR1"), 0o600))
	_, err = UploadRawSpool(context.Background(), uploader, request)
	require.Error(t, err)
	require.Equal(t, 1, calls)
	require.NoError(t, RemoveRawSpool(directory, spool.Name))
	require.NoError(t, RemoveRawSpool(directory, spool.Name))
	_, err = UploadRawSpool(context.Background(), uploader, request)
	require.Error(t, err)
}

func TestSpoolEntregaReferenciaSomenteAposEscritaCompleta(t *testing.T) {
	directory := t.TempDir()
	started, release := make(chan struct{}), make(chan struct{})
	t.Cleanup(func() { close(release) })
	type result struct {
		spool RawSpool
		err   error
	}
	prepared := make(chan result, 1)
	go func() {
		spool, err := PrepareRawSpool(context.Background(), directory, func(dst io.Writer) error {
			close(started)
			<-release
			_, err := io.WriteString(dst, "complete")
			return err
		})
		prepared <- result{spool, err}
	}()
	<-started
	select {
	case <-prepared:
		t.Fatal("referencia_publicada_antes_de_completar=true")
	default:
	}
	release <- struct{}{}
	got := <-prepared
	require.NoError(t, got.err)
	files, err := os.ReadDir(directory)
	require.NoError(t, err)
	require.Len(t, files, 1)
	require.Equal(t, got.spool.Name, files[0].Name())
	require.Equal(t, ".parquet", filepath.Ext(got.spool.Name))
	stored, err := os.ReadFile(filepath.Join(directory, got.spool.Name))
	require.NoError(t, err)
	require.Equal(t, "complete", string(stored))
}

func TestSpoolNaoSobrescreveDestinoDisputadoDurantePublicacao(t *testing.T) {
	directory := t.TempDir()
	var destination string
	var claimed bool
	spool, prepareErr := PrepareRawSpool(context.Background(), directory, func(dst io.Writer) error {
		files, err := os.ReadDir(directory)
		if err != nil {
			return err
		}
		name := strings.TrimSuffix(files[0].Name(), filepath.Ext(files[0].Name())) + ".parquet"
		destination = filepath.Join(directory, name)
		other, err := os.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err == nil {
			claimed = true
			_, err = io.WriteString(other, "existing")
			err = errors.Join(err, other.Close())
		}
		if err != nil && !errors.Is(err, os.ErrExist) {
			return err
		}
		_, err = io.WriteString(dst, "new")
		return err
	})
	stored, err := os.ReadFile(destination)
	require.NoError(t, err)
	if claimed {
		require.Equal(t, "existing", string(stored), "destino_preexistente_sobrescrito=true")
		require.True(t, prepareErr != nil || destination != filepath.Join(directory, spool.Name))
	} else {
		require.NoError(t, prepareErr)
		require.Equal(t, "new", string(stored))
	}
}

func TestSincronizacaoDeDiretorioNaoUsaOperacaoInvalidaNoWindows(t *testing.T) {
	for _, goos := range []string{"windows", "linux", "darwin"} {
		t.Run(goos, func(t *testing.T) {
			failure := errors.New("directory_sync=failed")
			err := syncSpoolDirectoryForOS("spool", goos, func(path string) error {
				require.Equal(t, "spool", path)
				return failure
			})
			if goos == "windows" {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, failure)
			}
		})
	}
}

type spoolUploader func(context.Context, RawPutRequest) (int64, error)

func (f spoolUploader) PutRaw(ctx context.Context, request RawPutRequest) (int64, error) {
	return f(ctx, request)
}

func TestSpoolFalhoOuCanceladoNaoDeixaArquivoParcial(t *testing.T) {
	directory := t.TempDir()
	spool, err := PrepareRawSpool(context.Background(), directory, func(io.Writer) error {
		return errors.New("write=failed")
	})
	require.Error(t, err)
	require.Empty(t, spool.Name)
	files, err := os.ReadDir(directory)
	require.NoError(t, err)
	require.Empty(t, files)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	spool, err = PrepareRawSpool(ctx, directory, func(dst io.Writer) error {
		_, err := io.WriteString(dst, "cancelled")
		return err
	})
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, spool.Name)
	files, err = os.ReadDir(directory)
	require.NoError(t, err)
	require.Empty(t, files)
	require.Error(t, RemoveRawSpool(directory, "../outside.parquet"))
}

type bodyAfterHeaders struct {
	ctx     context.Context
	started <-chan struct{}
	r       io.Reader
}

func (b *bodyAfterHeaders) Read(p []byte) (int, error) {
	select {
	case <-b.started:
		return b.r.Read(p)
	case <-b.ctx.Done():
		return 0, b.ctx.Err()
	}
}

func TestPutRawPropagaFalhaHTTP(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("unavailable"))
	}))
	defer srv.Close()
	_, err := NewHTTP(srv.Client()).PutRaw(context.Background(), RawPutRequest{
		URL: srv.URL, Body: strings.NewReader("x"), FencingToken: 9, ObjectKey: "raw/data.parquet",
	})
	require.ErrorContains(t, err, "503")
	require.ErrorContains(t, err, "unavailable")
}

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

	u := NewHTTP(http.DefaultClient)
	n, err := u.Put(context.Background(), srv.URL,
		strings.NewReader("hello world"), "application/octet-stream")
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

	u := NewHTTP(http.DefaultClient)
	_, err := u.Put(context.Background(), srv.URL, strings.NewReader("x"), "application/octet-stream")
	require.Error(t, err)
	require.Contains(t, err.Error(), "403")
	require.Contains(t, err.Error(), "RequestTimeTooSkewed")
}
