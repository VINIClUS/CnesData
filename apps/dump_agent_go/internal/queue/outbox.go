package queue

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"go.etcd.io/bbolt"
)

const bucketName = "outbox"

// Outbox wraps *bbolt.DB with FIFO semantics over a single bucket.
// Keys are 12 bytes: BigEndian(unix_ns)[8] || atomic_seq[4]. Values are
// JSON-encoded Envelopes. Each Append commits its own transaction (fsync).
type Outbox struct {
	db      *bbolt.DB
	seq     atomic.Uint32
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
		_, e := tx.CreateBucketIfNotExists([]byte(bucketName))
		return e
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
	key := o.makeKey(env.EnqueuedAt)
	return o.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket([]byte(bucketName)).Put(key, payload)
	})
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
		b := tx.Bucket([]byte(bucketName))
		for _, k := range keys {
			if err := b.Delete(k); err != nil {
				return err
			}
		}
		return nil
	})
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
		if env.EnqueuedAt.Before(cutoff) {
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
	remaining := b.Stats().KeyN
	if remaining <= maxCount {
		return 0, nil
	}
	toDrop := remaining - maxCount
	c := b.Cursor()
	keys := make([][]byte, 0, toDrop)
	for k, _ := c.First(); k != nil && len(keys) < toDrop; k, _ = c.Next() {
		keys = append(keys, append([]byte(nil), k...))
	}
	return deleteAll(b, keys)
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

func (o *Outbox) makeKey(t time.Time) []byte {
	key := make([]byte, 12)
	// UnixNano is non-negative for any time >= 1970-01-01; safe to cast.
	binary.BigEndian.PutUint64(key[0:8], uint64(t.UnixNano())) //nolint:gosec // G115
	binary.BigEndian.PutUint32(key[8:12], o.seq.Add(1))
	return key
}
