package delta

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"go.etcd.io/bbolt"
)

const (
	committedBucket = "committed"
	pendingBucket   = "pending"
	createdAtKey    = "_created_at"
)

// ErrPendingExists indicates an existing pending sub-bucket for the same key.
var ErrPendingExists = errors.New("pending_exists")

var errEarlyStop = errors.New("stop")

// Store wraps *bbolt.DB with two top-level buckets: committed and pending.
type Store struct {
	db *bbolt.DB
}

// Open creates parent dirs and opens the bbolt file. Initializes the two
// top-level buckets. File lock timeout 5s prevents indefinite hang.
func Open(path string) (*Store, error) {
	if err := ensureDir(filepath.Dir(path)); err != nil {
		return nil, fmt.Errorf("ensure_dir: %w", err)
	}
	db, err := bbolt.Open(path, 0o644, &bbolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("bbolt_open: %w", err)
	}
	if err := initRoots(db); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("init_roots: %w", err)
	}
	return &Store{db: db}, nil
}

func initRoots(db *bbolt.DB) error {
	return db.Update(func(tx *bbolt.Tx) error {
		for _, name := range []string{committedBucket, pendingBucket} {
			if _, err := tx.CreateBucketIfNotExists([]byte(name)); err != nil {
				return err
			}
		}
		return nil
	})
}

// Close releases the bbolt file lock.
func (s *Store) Close() error { return s.db.Close() }

// GetCommitted returns the pk→hash map for key. Empty map if absent.
func (s *Store) GetCommitted(key SourceKey) (map[string][32]byte, error) {
	out := map[string][32]byte{}
	err := s.db.View(func(tx *bbolt.Tx) error {
		b := navigate(tx.Bucket([]byte(committedBucket)), key.BucketPath())
		if b == nil {
			return nil
		}
		return b.ForEach(func(k, v []byte) error {
			var h [32]byte
			copy(h[:], v)
			out[string(k)] = h
			return nil
		})
	})
	return out, err
}

// PendingTx stages pk→hash entries; flushed atomically on Commit.
type PendingTx struct {
	store  *Store
	key    SourceKey
	jobID  string
	closed bool
	puts   map[string][32]byte
}

// BeginPending opens a staging tx for key. Refuses if any pending sub-bucket
// already exists for key (any jobID under pending/<key>).
func (s *Store) BeginPending(key SourceKey, jobID string) (*PendingTx, error) {
	exists := false
	err := s.db.View(func(tx *bbolt.Tx) error {
		exists = pendingExistsForKey(tx, key)
		return nil
	})
	if err != nil {
		return nil, err
	}
	if exists {
		return nil, ErrPendingExists
	}
	if err := s.writeCreatedAt(key, jobID); err != nil {
		return nil, fmt.Errorf("init_pending: %w", err)
	}
	return &PendingTx{
		store: s, key: key, jobID: jobID,
		puts: map[string][32]byte{},
	}, nil
}

func (s *Store) writeCreatedAt(key SourceKey, jobID string) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		root := tx.Bucket([]byte(pendingBucket))
		b, err := createNested(root, key.BucketPath()+"/"+jobID)
		if err != nil {
			return err
		}
		return b.Put([]byte(createdAtKey),
			[]byte(time.Now().UTC().Format(time.RFC3339Nano)))
	})
}

// Put stages an entry. Flushed during Commit.
func (p *PendingTx) Put(pk string, h [32]byte) error {
	if p.closed {
		return errors.New("pending_closed")
	}
	p.puts[pk] = h
	return nil
}

// Commit atomically: writes staged entries → wipes committed for key →
// recopies from pending → drops pending sub-bucket.
func (p *PendingTx) Commit() error {
	if p.closed {
		return errors.New("pending_closed")
	}
	p.closed = true
	return p.store.db.Update(func(tx *bbolt.Tx) error {
		return p.commitTx(tx)
	})
}

func (p *PendingTx) commitTx(tx *bbolt.Tx) error {
	pendRoot := tx.Bucket([]byte(pendingBucket))
	pendBucket, err := createNested(pendRoot, p.key.BucketPath()+"/"+p.jobID)
	if err != nil {
		return err
	}
	for k, v := range p.puts {
		hash := v
		if err := pendBucket.Put([]byte(k), hash[:]); err != nil {
			return err
		}
	}
	commRoot := tx.Bucket([]byte(committedBucket))
	if err := deleteNested(commRoot, p.key.BucketPath()); err != nil {
		return err
	}
	commBucket, err := createNested(commRoot, p.key.BucketPath())
	if err != nil {
		return err
	}
	if err := copyEntries(pendBucket, commBucket); err != nil {
		return err
	}
	return deleteNestedPrune(pendRoot, p.key.BucketPath()+"/"+p.jobID)
}

func copyEntries(src, dst *bbolt.Bucket) error {
	return src.ForEach(func(k, v []byte) error {
		if string(k) == createdAtKey {
			return nil
		}
		return dst.Put(k, v)
	})
}

// Abort drops the pending sub-bucket without touching committed.
func (p *PendingTx) Abort() {
	if p.closed {
		return
	}
	p.closed = true
	_ = p.store.db.Update(func(tx *bbolt.Tx) error {
		root := tx.Bucket([]byte(pendingBucket))
		return deleteNestedPrune(root, p.key.BucketPath()+"/"+p.jobID)
	})
}

// GarbageCollectStalePending removes pending sub-buckets older than maxAge,
// or those missing/with malformed _created_at marker. Returns count removed.
func (s *Store) GarbageCollectStalePending(maxAge time.Duration) (int, error) {
	stale, err := s.findStalePending(maxAge)
	if err != nil {
		return 0, err
	}
	if len(stale) == 0 {
		return 0, nil
	}
	err = s.db.Update(func(tx *bbolt.Tx) error {
		root := tx.Bucket([]byte(pendingBucket))
		for _, p := range stale {
			if err := deleteNestedPrune(root, p); err != nil {
				return err
			}
		}
		return nil
	})
	return len(stale), err
}

func (s *Store) findStalePending(maxAge time.Duration) ([]string, error) {
	cutoff := time.Now().UTC().Add(-maxAge)
	stale := []string{}
	err := s.db.View(func(tx *bbolt.Tx) error {
		root := tx.Bucket([]byte(pendingBucket))
		return walkLeafBuckets(root, "", func(path string, b *bbolt.Bucket) error {
			ts := b.Get([]byte(createdAtKey))
			if ts == nil {
				stale = append(stale, path)
				return nil
			}
			t, err := time.Parse(time.RFC3339Nano, string(ts))
			if err != nil || t.Before(cutoff) {
				stale = append(stale, path)
			}
			return nil
		})
	})
	return stale, err
}

func navigate(start *bbolt.Bucket, path string) *bbolt.Bucket {
	if start == nil {
		return nil
	}
	cur := start
	for _, seg := range splitPath(path) {
		next := cur.Bucket([]byte(seg))
		if next == nil {
			return nil
		}
		cur = next
	}
	return cur
}

func createNested(start *bbolt.Bucket, path string) (*bbolt.Bucket, error) {
	cur := start
	for _, seg := range splitPath(path) {
		next, err := cur.CreateBucketIfNotExists([]byte(seg))
		if err != nil {
			return nil, err
		}
		cur = next
	}
	return cur, nil
}

func deleteNested(start *bbolt.Bucket, path string) error {
	parts := splitPath(path)
	if len(parts) == 0 {
		return nil
	}
	parent := start
	for i := 0; i < len(parts)-1; i++ {
		next := parent.Bucket([]byte(parts[i]))
		if next == nil {
			return nil
		}
		parent = next
	}
	leaf := parts[len(parts)-1]
	if parent.Bucket([]byte(leaf)) == nil {
		return nil
	}
	return parent.DeleteBucket([]byte(leaf))
}

func splitPath(p string) []string {
	if p == "" {
		return nil
	}
	return strings.Split(p, "/")
}

// deleteNestedPrune deletes the leaf at path then removes ancestor buckets
// that became empty (no keys, no sub-buckets). Root is never deleted.
func deleteNestedPrune(start *bbolt.Bucket, path string) error {
	parts := splitPath(path)
	if len(parts) == 0 {
		return nil
	}
	if err := deleteNested(start, path); err != nil {
		return err
	}
	for i := len(parts) - 1; i >= 1; i-- {
		ancestorPath := strings.Join(parts[:i], "/")
		ancestor := navigate(start, ancestorPath)
		if ancestor == nil {
			return nil
		}
		if !isEmptyBucket(ancestor) {
			return nil
		}
		if err := deleteNested(start, ancestorPath); err != nil {
			return err
		}
	}
	return nil
}

func isEmptyBucket(b *bbolt.Bucket) bool {
	empty := true
	_ = b.ForEach(func(_, _ []byte) error {
		empty = false
		return errEarlyStop
	})
	return empty
}

func pendingExistsForKey(tx *bbolt.Tx, key SourceKey) bool {
	root := tx.Bucket([]byte(pendingBucket))
	if root == nil {
		return false
	}
	parent := navigate(root, key.BucketPath())
	if parent == nil {
		return false
	}
	exists := false
	_ = parent.ForEachBucket(func(_ []byte) error {
		exists = true
		return errEarlyStop
	})
	return exists
}

func walkLeafBuckets(
	b *bbolt.Bucket, prefix string, fn func(path string, b *bbolt.Bucket) error,
) error {
	if b == nil {
		return nil
	}
	return b.ForEachBucket(func(k []byte) error {
		child := b.Bucket(k)
		path := string(k)
		if prefix != "" {
			path = prefix + "/" + path
		}
		hasChild := false
		_ = child.ForEachBucket(func(_ []byte) error {
			hasChild = true
			return errEarlyStop
		})
		if !hasChild {
			return fn(path, child)
		}
		return walkLeafBuckets(child, path, fn)
	})
}

func ensureDir(dir string) error {
	if dir == "" {
		return nil
	}
	return os.MkdirAll(dir, 0o755)
}
