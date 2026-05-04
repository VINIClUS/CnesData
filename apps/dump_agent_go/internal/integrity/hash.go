// Package integrity provides streaming SHA-256 over Parquet bytes during
// upload (single-pass tee) for tamper-evident verification on the
// receiving side.
package integrity

import (
	"crypto/sha256"
	"encoding/hex"
	"hash"
	"io"
)

// Hasher accumulates a SHA-256 digest as bytes flow through.
type Hasher struct {
	h hash.Hash
}

// Sum returns the accumulated digest as a fixed [32]byte.
func (h *Hasher) Sum() [32]byte {
	var out [32]byte
	copy(out[:], h.h.Sum(nil))
	return out
}

// SumHex returns the digest as a 64-char lowercase hex string.
func (h *Hasher) SumHex() string {
	return hex.EncodeToString(h.h.Sum(nil))
}

// SHA256TeeReader wraps src with an io.TeeReader that funnels bytes
// into a SHA-256 hasher as the caller reads. Caller reads the returned
// reader normally; once io.EOF is reached, Hasher.Sum/SumHex returns
// the digest over the bytes streamed.
func SHA256TeeReader(src io.Reader) (io.Reader, *Hasher) {
	h := &Hasher{h: sha256.New()}
	return io.TeeReader(src, h.h), h
}
