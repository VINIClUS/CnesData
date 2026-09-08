package delta

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cnesdata/dumpagent/internal/manifest"
	"go.etcd.io/bbolt"
)

const (
	committedBucket  = "committed"
	pendingBucket    = "pending"
	rawPendingBucket = "raw_pending"
	headBucket       = "chain_heads"
	forceFullBucket  = "force_full"
	createdAtKey     = "_created_at"
)

var ErrPendingExists = errors.New("pending_exists")
var ErrPendingNotFound = errors.New("pending_missing")

type Store struct{ db *bbolt.DB }

type PendingRef struct {
	SourceKey    SourceKey
	JobID        string
	FencingToken uint64
}

type chainState struct {
	SnapshotID        string
	Sequence          uint32
	ManifestSHA256    string
	CreatedAt         time.Time
	ConfirmedRef      PendingRef
	ConfirmedManifest manifest.Raw
}

type PendingTx struct {
	store  *Store
	key    SourceKey
	path   string
	root   string
	closed bool
	puts   map[string][32]byte
}

func Open(path string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("ensure_dir: %w", err)
	}
	db, err := bbolt.Open(path, 0o644, &bbolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("bbolt_open: %w", err)
	}
	err = db.Update(func(tx *bbolt.Tx) error {
		for _, name := range []string{
			committedBucket, pendingBucket, rawPendingBucket, headBucket, forceFullBucket,
		} {
			if _, err := tx.CreateBucketIfNotExists([]byte(name)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("init_roots: %w", err)
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) GetCommitted(key SourceKey) (map[string][32]byte, error) {
	out := map[string][32]byte{}
	err := s.db.View(func(tx *bbolt.Tx) error {
		b := navigate(tx.Bucket([]byte(committedBucket)), key.BucketPath())
		if b == nil {
			return nil
		}
		return b.ForEach(func(k, v []byte) error {
			var hash [32]byte
			copy(hash[:], v)
			out[string(k)] = hash
			return nil
		})
	})
	return out, err
}

func (s *Store) BeginPending(key SourceKey, jobID string) (*PendingTx, error) {
	p := &PendingTx{store: s, key: key, path: key.BucketPath() + "/" + jobID, root: pendingBucket}
	p.puts = map[string][32]byte{}
	err := s.db.Update(func(tx *bbolt.Tx) error {
		root := tx.Bucket([]byte(pendingBucket))
		parent := navigate(root, key.BucketPath())
		if parent != nil && !isEmptyBucket(parent) {
			return ErrPendingExists
		}
		b, err := createNested(root, p.path)
		if err != nil {
			return err
		}
		return b.Put([]byte(createdAtKey), []byte(time.Now().UTC().Format(time.RFC3339Nano)))
	})
	if err != nil {
		return nil, err
	}
	return p, nil
}

func (s *Store) BeginPendingRef(ref PendingRef) (*PendingTx, error) {
	if err := ref.validate(); err != nil {
		return nil, err
	}
	err := s.db.Update(func(tx *bbolt.Tx) error {
		root := tx.Bucket([]byte(rawPendingBucket))
		if navigate(root, ref.path()) != nil {
			return ErrPendingExists
		}
		_, err := createNested(root, ref.path())
		return err
	})
	if err != nil {
		return nil, err
	}
	return &PendingTx{store: s, key: ref.SourceKey, path: ref.path(), root: rawPendingBucket}, nil
}

func (s *Store) ResumePendingRef(ref PendingRef) (*PendingTx, error) {
	if err := ref.validate(); err != nil {
		return nil, err
	}
	err := s.db.View(func(tx *bbolt.Tx) error {
		if navigate(tx.Bucket([]byte(rawPendingBucket)), ref.path()) == nil {
			return ErrPendingNotFound
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &PendingTx{store: s, key: ref.SourceKey, path: ref.path(), root: rawPendingBucket}, nil
}

func (r PendingRef) path() string {
	return fmt.Sprintf("%s/%s/%d", r.SourceKey.BucketPath(), r.JobID, r.FencingToken)
}

func (r PendingRef) validate() error {
	for _, segment := range []string{
		r.SourceKey.Source, r.SourceKey.Intent, r.SourceKey.Competencia, r.JobID,
	} {
		if segment == "" || strings.Contains(segment, "/") {
			return errors.New("pending_ref=invalid")
		}
	}
	return nil
}

func (p *PendingTx) Put(pk string, h [32]byte) error {
	if p.closed {
		return errors.New("pending_closed")
	}
	if p.root == pendingBucket {
		p.puts[pk] = h
		return nil
	}
	return p.store.db.Update(func(tx *bbolt.Tx) error {
		b := navigate(tx.Bucket([]byte(p.root)), p.path)
		if b == nil {
			return errors.New("pending_missing")
		}
		return b.Put([]byte(pk), h[:])
	})
}

func (p *PendingTx) Replace(hashes map[string][32]byte) error {
	if p.closed || p.root != rawPendingBucket {
		return errors.New("pending=not_raw_or_closed")
	}
	return p.store.db.Update(func(tx *bbolt.Tx) error {
		root := tx.Bucket([]byte(p.root))
		if navigate(root, p.path) == nil {
			return errors.New("pending_missing")
		}
		if err := deleteNested(root, p.path); err != nil {
			return err
		}
		b, err := createNested(root, p.path)
		if err != nil {
			return err
		}
		for pk, hash := range hashes {
			if err := b.Put([]byte(pk), hash[:]); err != nil {
				return err
			}
		}
		return nil
	})
}

func (p *PendingTx) Commit() error {
	if p.closed || p.root == rawPendingBucket {
		return errors.New("pending=closed_or_requires_ack")
	}
	err := p.store.db.Update(func(tx *bbolt.Tx) error {
		root := tx.Bucket([]byte(p.root))
		b := navigate(root, p.path)
		if b == nil {
			return errors.New("pending_missing")
		}
		for pk, hash := range p.puts {
			if err := b.Put([]byte(pk), hash[:]); err != nil {
				return err
			}
		}
		if err := promote(tx, p.key, b); err != nil {
			return err
		}
		return deleteNestedPrune(root, p.path)
	})
	if err == nil {
		p.closed = true
	}
	return err
}

func (p *PendingTx) Abort() {
	if p.closed {
		return
	}
	p.closed = true
	_ = p.store.db.Update(func(tx *bbolt.Tx) error {
		return deleteNestedPrune(tx.Bucket([]byte(p.root)), p.path)
	})
}

func (s *Store) ChainHead(key SourceKey) (string, uint32, string, time.Time, bool, error) {
	var head chainState
	var ok bool
	err := s.db.View(func(tx *bbolt.Tx) error {
		var err error
		head, ok, err = readHead(tx, key)
		return err
	})
	return head.SnapshotID, head.Sequence, head.ManifestSHA256, head.CreatedAt, ok, err
}

func readHead(tx *bbolt.Tx, key SourceKey) (chainState, bool, error) {
	var head chainState
	data := tx.Bucket([]byte(headBucket)).Get([]byte(key.BucketPath()))
	if data == nil {
		return head, false, nil
	}
	err := json.Unmarshal(data, &head)
	return head, err == nil, err
}

func (s *Store) ConfirmPending(ref PendingRef, raw manifest.Raw, serverHash string) error {
	if err := ref.validate(); err != nil {
		return err
	}
	if err := validateConfirmation(ref, raw, serverHash); err != nil {
		return err
	}
	return s.db.Update(func(tx *bbolt.Tx) error {
		head, _, err := readHead(tx, ref.SourceKey)
		if err != nil {
			return err
		}
		root := tx.Bucket([]byte(rawPendingBucket))
		pending := navigate(root, ref.path())
		if pending == nil {
			if head.matches(ref, raw, serverHash) {
				return nil
			}
			return errors.New("pending_missing")
		}
		next, err := nextHead(head, ref, raw, serverHash)
		if err != nil {
			return err
		}
		if err := promote(tx, ref.SourceKey, pending); err != nil {
			return err
		}
		if err := saveHead(tx, ref.SourceKey, next); err != nil {
			return err
		}
		return deleteNestedPrune(root, ref.path())
	})
}

func (h chainState) matches(ref PendingRef, raw manifest.Raw, hash string) bool {
	want, err := json.Marshal(raw)
	got, storedErr := json.Marshal(h.ConfirmedManifest)
	return err == nil && storedErr == nil && h.ConfirmedRef == ref &&
		h.ManifestSHA256 == hash && bytes.Equal(want, got)
}

func validateConfirmation(ref PendingRef, raw manifest.Raw, hash string) error {
	decoded, err := hex.DecodeString(hash)
	if err != nil || len(decoded) != 32 || strings.ToLower(hash) != hash {
		return errors.New("manifest_sha256=invalid")
	}
	if raw.ManifestID != ref.JobID || raw.SnapshotID != ref.JobID || raw.CreatedAt.IsZero() {
		return errors.New("confirmation_identity=invalid")
	}
	return nil
}

func nextHead(head chainState, ref PendingRef, raw manifest.Raw, hash string) (chainState, error) {
	switch raw.SnapshotMode {
	case manifest.SnapshotModeFull:
		if raw.Sequence != 1 || raw.BaseSnapshotID != nil || raw.PreviousManifestSHA256 != nil {
			return head, errors.New("full_chain=invalid")
		}
		head.SnapshotID, head.CreatedAt = raw.SnapshotID, raw.CreatedAt
	case manifest.SnapshotModeDelta:
		if !validDeltaHead(head, raw) {
			return head, errors.New("delta_chain=invalid")
		}
	default:
		return head, errors.New("snapshot_mode=invalid")
	}
	head.Sequence, head.ManifestSHA256, head.ConfirmedRef = raw.Sequence, hash, ref
	head.ConfirmedManifest = raw
	return head, nil
}

func validDeltaHead(head chainState, raw manifest.Raw) bool {
	return head.Sequence > 0 && head.Sequence < ^uint32(0) && raw.Sequence == head.Sequence+1 &&
		raw.BaseSnapshotID != nil && *raw.BaseSnapshotID == head.SnapshotID &&
		raw.PreviousManifestSHA256 != nil && *raw.PreviousManifestSHA256 == head.ManifestSHA256
}

func saveHead(tx *bbolt.Tx, key SourceKey, head chainState) error {
	data, err := json.Marshal(head)
	if err != nil {
		return err
	}
	if err := tx.Bucket([]byte(headBucket)).Put([]byte(key.BucketPath()), data); err != nil {
		return err
	}
	if head.Sequence == 1 {
		return tx.Bucket([]byte(forceFullBucket)).Delete([]byte(key.BucketPath()))
	}
	return nil
}

func promote(tx *bbolt.Tx, key SourceKey, rows *bbolt.Bucket) error {
	root := tx.Bucket([]byte(committedBucket))
	if err := deleteNested(root, key.BucketPath()); err != nil {
		return err
	}
	committed, err := createNested(root, key.BucketPath())
	if err != nil {
		return err
	}
	return rows.ForEach(func(pk, hash []byte) error {
		if string(pk) == createdAtKey && len(hash) != 32 {
			return nil
		}
		return committed.Put(pk, hash)
	})
}

func (s *Store) ForceFull(key SourceKey) (string, bool, error) {
	var reason string
	var required bool
	err := s.db.View(func(tx *bbolt.Tx) error {
		data := tx.Bucket([]byte(forceFullBucket)).Get([]byte(key.BucketPath()))
		reason, required = string(data), data != nil
		return nil
	})
	return reason, required, err
}

func (s *Store) RequireFull(ref PendingRef, reason string) error {
	if err := ref.validate(); err != nil {
		return err
	}
	if reason == "" {
		return errors.New("force_full_reason=empty")
	}
	return s.db.Update(func(tx *bbolt.Tx) error {
		if err := tx.Bucket([]byte(forceFullBucket)).Put([]byte(ref.SourceKey.BucketPath()),
			[]byte(reason)); err != nil {
			return err
		}
		return deleteNestedPrune(tx.Bucket([]byte(rawPendingBucket)), ref.path())
	})
}

func (s *Store) GarbageCollectStalePending(maxAge time.Duration) (int, error) {
	var stale []string
	err := s.db.Update(func(tx *bbolt.Tx) error {
		root := tx.Bucket([]byte(pendingBucket))
		cutoff := time.Now().UTC().Add(-maxAge)
		err := walkLeafBuckets(root, "", func(path string, b *bbolt.Bucket) error {
			created, err := time.Parse(time.RFC3339Nano, string(b.Get([]byte(createdAtKey))))
			if err != nil || created.Before(cutoff) {
				stale = append(stale, path)
			}
			return nil
		})
		if err != nil {
			return err
		}
		for _, path := range stale {
			if err := deleteNestedPrune(root, path); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return len(stale), nil
}

func navigate(start *bbolt.Bucket, path string) *bbolt.Bucket {
	cur := start
	for _, segment := range strings.Split(path, "/") {
		if cur == nil {
			return nil
		}
		cur = cur.Bucket([]byte(segment))
	}
	return cur
}

func createNested(start *bbolt.Bucket, path string) (*bbolt.Bucket, error) {
	cur := start
	for _, segment := range strings.Split(path, "/") {
		var err error
		cur, err = cur.CreateBucketIfNotExists([]byte(segment))
		if err != nil {
			return nil, err
		}
	}
	return cur, nil
}

func deleteNested(start *bbolt.Bucket, path string) error {
	parent, leaf := start, path
	if index := strings.LastIndex(path, "/"); index >= 0 {
		parent, leaf = navigate(start, path[:index]), path[index+1:]
	}
	if parent == nil || parent.Bucket([]byte(leaf)) == nil {
		return nil
	}
	return parent.DeleteBucket([]byte(leaf))
}

func deleteNestedPrune(start *bbolt.Bucket, path string) error {
	if err := deleteNested(start, path); err != nil {
		return err
	}
	for index := strings.LastIndex(path, "/"); index >= 0; index = strings.LastIndex(path, "/") {
		path = path[:index]
		ancestor := navigate(start, path)
		if ancestor == nil || !isEmptyBucket(ancestor) {
			return nil
		}
		if err := deleteNested(start, path); err != nil {
			return err
		}
	}
	return nil
}

func isEmptyBucket(b *bbolt.Bucket) bool { k, _ := b.Cursor().First(); return k == nil }

func walkLeafBuckets(b *bbolt.Bucket, prefix string, fn func(string, *bbolt.Bucket) error) error {
	return b.ForEachBucket(func(k []byte) error {
		child := b.Bucket(k)
		path := string(k)
		if prefix != "" {
			path = prefix + "/" + path
		}
		hasChild := false
		_ = child.ForEachBucket(func([]byte) error { hasChild = true; return nil })
		if !hasChild {
			return fn(path, child)
		}
		return walkLeafBuckets(child, path, fn)
	})
}
