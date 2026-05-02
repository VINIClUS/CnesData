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
		var ttlKeys [][]byte
		if err := b.ForEach(func(k, v []byte) error {
			var env Envelope
			if err := json.Unmarshal(v, &env); err != nil {
				ttlKeys = append(ttlKeys, append([]byte(nil), k...))
				return nil
			}
			if env.EnqueuedAt.Before(cutoff) {
				ttlKeys = append(ttlKeys, append([]byte(nil), k...))
			}
			return nil
		}); err != nil {
			return err
		}
		for _, k := range ttlKeys {
			if err := b.Delete(k); err != nil {
				return err
			}
			deleted++
		}
		remaining := b.Stats().KeyN
		if remaining > maxCount {
			toDrop := remaining - maxCount
			c := b.Cursor()
			var capKeys [][]byte
			for k, _ := c.First(); k != nil && len(capKeys) < toDrop; k, _ = c.Next() {
				capKeys = append(capKeys, append([]byte(nil), k...))
			}
			for _, k := range capKeys {
				if err := b.Delete(k); err != nil {
					return err
				}
				deleted++
			}
		}
		return nil
	})
	return deleted, err
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
	binary.BigEndian.PutUint64(key[0:8], uint64(t.UnixNano()))
	binary.BigEndian.PutUint32(key[8:12], o.seq.Add(1))
	return key
}
