package audit

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/cnesdata/dumpagent/internal/secrets"
)

const auditKeySource = "audit_hmac"

// LoadOrCreate returns the HMAC key bytes from the secrets store.
// On first call (key absent), generates 32 random bytes via crypto/rand,
// persists hex-encoded via secrets.Save (DPAPI-wrapped on Windows,
// 0600 plaintext on Linux dev), and returns the raw key.
// Subsequent calls return the cached value.
func LoadOrCreate(store *secrets.Store) ([]byte, error) {
	hexKey, err := store.Load(auditKeySource)
	if err == nil && hexKey != "" {
		return decodeKey(hexKey)
	}
	if err != nil && !errors.Is(err, secrets.ErrNotSet) {
		return nil, fmt.Errorf("audit_hmac_load: %w", err)
	}
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return nil, fmt.Errorf("audit_hmac_gen: %w", err)
	}
	encoded := hex.EncodeToString(raw)
	if err := store.Save(auditKeySource, encoded); err != nil {
		return nil, fmt.Errorf("audit_hmac_save: %w", err)
	}
	return raw, nil
}

func decodeKey(hexKey string) ([]byte, error) {
	raw, err := hex.DecodeString(hexKey)
	if err != nil {
		return nil, fmt.Errorf("audit_hmac_decode: %w", err)
	}
	if len(raw) != 32 {
		return nil, fmt.Errorf("audit_hmac_size: got=%d want=32", len(raw))
	}
	return raw, nil
}
