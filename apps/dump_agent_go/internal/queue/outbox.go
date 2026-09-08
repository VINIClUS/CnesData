package queue

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"go.etcd.io/bbolt"
)

const bucketName = "outbox"
const rawIndexBucket = "raw_job_fence"
const rawTerminalBucket = "raw_terminal"

// Outbox wraps *bbolt.DB with FIFO semantics over a single bucket.
// Keys are 12 bytes: BigEndian(unix_ns)[8] || durable_seq[4]. Values are
// JSON-encoded Envelopes. Each Append commits its own transaction (fsync).
type Outbox struct {
	db      *bbolt.DB
	nowFunc func() time.Time
}

// Item pairs a bbolt key with its decoded Envelope.
type Item struct {
	Key      []byte
	Envelope Envelope
}

// Open creates parent dirs and opens the bbolt file. Bucket created
// if absent. File lock timeout 5s prevents indefinite hang on stale locks.
func Open(path string) (*Outbox, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("outbox: mkdir: %w", err)
	}
	db, err := bbolt.Open(path, 0o600, &bbolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("outbox: open: %w", err)
	}
	if err := db.Update(func(tx *bbolt.Tx) error {
		for _, name := range []string{bucketName, rawIndexBucket, rawTerminalBucket} {
			if _, err := tx.CreateBucketIfNotExists([]byte(name)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("outbox: bucket: %w", err)
	}
	return &Outbox{db: db, nowFunc: time.Now}, nil
}

// Append persists env. EnqueuedAt is filled if zero.
func (o *Outbox) Append(env Envelope) error {
	if env.EnqueuedAt.IsZero() {
		env.EnqueuedAt = o.nowFunc()
	}
	payload, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("outbox: marshal: %w", err)
	}
	return o.db.Update(func(tx *bbolt.Tx) error {
		if env.Type == TypeRawManifest {
			return o.appendRaw(tx, env, payload)
		}
		_, err := persistEnvelope(tx.Bucket([]byte(bucketName)), env, payload)
		return err
	})
}

func (o *Outbox) appendRaw(tx *bbolt.Tx, env Envelope, payload []byte) error {
	if err := validateRawIdentity(env); err != nil {
		return err
	}
	index := tx.Bucket([]byte(rawIndexBucket))
	b := tx.Bucket([]byte(bucketName))
	identity := rawIdentity(env)
	if key := index.Get(identity); key != nil {
		var existing Envelope
		if err := json.Unmarshal(b.Get(key), &existing); err != nil {
			return fmt.Errorf("raw_envelope_index_invalid=%w", err)
		}
		if existing.SourceKey != env.SourceKey || existing.ManifestSHA256 != env.ManifestSHA256 ||
			existing.SpoolName != env.SpoolName || existing.UploadURL != env.UploadURL ||
			!bytes.Equal(existing.ManifestJSON, env.ManifestJSON) {
			return fmt.Errorf("raw_envelope_conflict=true job_id=%s fence=%d", env.JobID, env.FencingToken)
		}
		return nil
	}
	key, err := persistEnvelope(b, env, payload)
	if err != nil {
		return err
	}
	return index.Put(identity, key)
}

func validateRawIdentity(env Envelope) error {
	if env.JobID == "" || env.FencingToken == 0 || env.SourceKey.Source == "" ||
		env.SourceKey.Intent == "" || env.SourceKey.Competencia == "" ||
		len(env.ManifestJSON) == 0 || env.ManifestSHA256 == "" {
		return fmt.Errorf("raw_envelope_identity_incomplete=true")
	}
	return nil
}

func persistEnvelope(b *bbolt.Bucket, env Envelope, payload []byte) ([]byte, error) {
	for {
		key, err := makeKey(b, env.EnqueuedAt)
		if err != nil {
			return nil, err
		}
		if b.Get(key) == nil {
			return key, b.Put(key, payload)
		}
	}
}

func rawIdentity(env Envelope) []byte {
	return binary.BigEndian.AppendUint64(append([]byte(env.JobID), 0), env.FencingToken)
}

// Peek returns up to n oldest items in FIFO order.
func (o *Outbox) Peek(n int) ([]Item, error) {
	items := make([]Item, 0, n)
	err := o.db.View(func(tx *bbolt.Tx) error {
		c := tx.Bucket([]byte(bucketName)).Cursor()
		for k, v := c.First(); k != nil && len(items) < n; k, v = c.Next() {
			var env Envelope
			if err := json.Unmarshal(v, &env); err != nil {
				continue
			}
			keyCopy := append([]byte(nil), k...)
			items = append(items, Item{Key: keyCopy, Envelope: env})
		}
		return nil
	})
	return items, err
}

// Delete removes the given keys atomically.
func (o *Outbox) Delete(keys ...[]byte) error {
	return o.db.Update(func(tx *bbolt.Tx) error {
		for _, k := range keys {
			if err := deleteEnvelope(tx, k); err != nil {
				return err
			}
		}
		return nil
	})
}

// RawTerminal consulta o recibo que autoriza a limpeza do spool.
func (o *Outbox) RawTerminal(key []byte) (bool, error) {
	var terminal bool
	err := o.db.View(func(tx *bbolt.Tx) error {
		terminal = tx.Bucket([]byte(rawTerminalBucket)).Get(key) != nil
		return nil
	})
	return terminal, err
}

// MarkRawTerminal persiste o recibo somente para um envelope raw existente.
func (o *Outbox) MarkRawTerminal(key []byte) error {
	return o.db.Update(func(tx *bbolt.Tx) error {
		var env Envelope
		if json.Unmarshal(tx.Bucket([]byte(bucketName)).Get(key), &env) != nil ||
			env.Type != TypeRawManifest {
			return fmt.Errorf("raw_terminal=envelope_missing")
		}
		return tx.Bucket([]byte(rawTerminalBucket)).Put(key, []byte{1})
	})
}

func deleteEnvelope(tx *bbolt.Tx, key []byte) error {
	b := tx.Bucket([]byte(bucketName))
	var env Envelope
	if json.Unmarshal(b.Get(key), &env) == nil && env.Type == TypeRawManifest {
		if err := tx.Bucket([]byte(rawIndexBucket)).Delete(rawIdentity(env)); err != nil {
			return err
		}
	}
	if err := tx.Bucket([]byte(rawTerminalBucket)).Delete(key); err != nil {
		return err
	}
	return b.Delete(key)
}

// Evict drops envelopes older than maxAge first; if remaining count > maxCount,
// drops oldest beyond cap. Returns total deleted.
func (o *Outbox) Evict(maxAge time.Duration, maxCount int) (int, error) {
	cutoff := o.nowFunc().Add(-maxAge)
	var deleted int
	err := o.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		ttlDeleted, err := evictByTTL(b, cutoff)
		if err != nil {
			return err
		}
		deleted += ttlDeleted
		capDeleted, err := evictByCap(b, maxCount)
		if err != nil {
			return err
		}
		deleted += capDeleted
		return nil
	})
	return deleted, err
}

// evictByTTL deletes envelopes whose EnqueuedAt is before cutoff.
// Envelopes that fail to unmarshal are also deleted (treated as corrupt).
func evictByTTL(b *bbolt.Bucket, cutoff time.Time) (int, error) {
	var keys [][]byte
	if err := b.ForEach(func(k, v []byte) error {
		var env Envelope
		if err := json.Unmarshal(v, &env); err != nil {
			keys = append(keys, append([]byte(nil), k...))
			return nil
		}
		if env.Type != TypeRawManifest && env.EnqueuedAt.Before(cutoff) {
			keys = append(keys, append([]byte(nil), k...))
		}
		return nil
	}); err != nil {
		return 0, err
	}
	return deleteAll(b, keys)
}

// evictByCap deletes the oldest entries beyond maxCount. No-op if under cap.
func evictByCap(b *bbolt.Bucket, maxCount int) (int, error) {
	c := b.Cursor()
	var keys [][]byte
	for k, v := c.First(); k != nil; k, v = c.Next() {
		var env Envelope
		if json.Unmarshal(v, &env) == nil && env.Type == TypeRawManifest {
			continue
		}
		keys = append(keys, append([]byte(nil), k...))
	}
	if maxCount < 0 {
		maxCount = 0
	}
	if len(keys) <= maxCount {
		return 0, nil
	}
	return deleteAll(b, keys[:len(keys)-maxCount])
}

func deleteAll(b *bbolt.Bucket, keys [][]byte) (int, error) {
	for _, k := range keys {
		if err := b.Delete(k); err != nil {
			return 0, err
		}
	}
	return len(keys), nil
}

// Close releases the bbolt file lock. Idempotent: second call returns nil.
func (o *Outbox) Close() error {
	if o.db == nil {
		return nil
	}
	err := o.db.Close()
	o.db = nil
	return err
}

func makeKey(b *bbolt.Bucket, t time.Time) ([]byte, error) {
	seq, err := b.NextSequence()
	if err != nil {
		return nil, err
	}
	if seq > uint64(^uint32(0)) {
		return nil, fmt.Errorf("outbox_sequence=overflow")
	}
	key := make([]byte, 12)
	// UnixNano is non-negative for any time >= 1970-01-01; safe to cast.
	binary.BigEndian.PutUint64(key[0:8], uint64(t.UnixNano())) //nolint:gosec // G115
	binary.BigEndian.PutUint32(key[8:12], uint32(seq))         //nolint:gosec // Bounds checked above.
	return key, nil
}
