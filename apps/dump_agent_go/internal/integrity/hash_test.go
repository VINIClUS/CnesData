package integrity

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSHA256TeeReader_PassesBytesUnchanged(t *testing.T) {
	src := bytes.NewReader([]byte("hello world"))
	teed, _ := SHA256TeeReader(src)
	got, err := io.ReadAll(teed)
	require.NoError(t, err)
	require.Equal(t, "hello world", string(got))
}

func TestSHA256TeeReader_AccumulatesHash(t *testing.T) {
	src := bytes.NewReader([]byte("hello world"))
	teed, h := SHA256TeeReader(src)
	_, err := io.ReadAll(teed)
	require.NoError(t, err)
	expected := "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
	require.Equal(t, expected, h.SumHex())
}

func TestSHA256TeeReader_DeterministicAcrossRuns(t *testing.T) {
	first := ""
	for i := 0; i < 5; i++ {
		teed, h := SHA256TeeReader(bytes.NewReader([]byte("repeat-input")))
		_, _ = io.ReadAll(teed)
		got := h.SumHex()
		if i == 0 {
			first = got
		}
		require.Equal(t, first, got, "iter %d", i)
	}
}

func TestSHA256TeeReader_LargeStream1MB(t *testing.T) {
	buf := make([]byte, 1<<20)
	_, err := rand.Read(buf)
	require.NoError(t, err)
	teed, h := SHA256TeeReader(bytes.NewReader(buf))
	got, err := io.ReadAll(teed)
	require.NoError(t, err)
	require.Equal(t, len(buf), len(got))
	sum := h.Sum()
	require.Len(t, hex.EncodeToString(sum[:]), 64)
}

func TestHasher_SumIs32Bytes(t *testing.T) {
	teed, h := SHA256TeeReader(bytes.NewReader([]byte("x")))
	_, _ = io.ReadAll(teed)
	sum := h.Sum()
	require.Len(t, sum[:], 32)
}
