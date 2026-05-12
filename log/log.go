// Package log is the caller-facing figwal log: append-only, segment
// rotated, fork-able, with an in-memory cache that serves reads
// lock-free over an atomic snapshot pointer. The underlying disk
// layout and segment management live in figwal/disk; this package
// wraps them.
package log

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/jack-work/figwal/disk"
)

// Re-exported error sentinels so callers don't need to import the
// disk subpackage just to check error identity.
var (
	ErrNotFound = disk.ErrNotFound
	ErrReadOnly = disk.ErrReadOnly
)

// Options aliases disk.Options so the caller-facing API is a single
// package. Code that already imported figwal/log keeps working with a
// trivial type-name change at the import site.
type Options = disk.Options

// SyncMode aliases disk.SyncMode for the same reason.
type SyncMode = disk.SyncMode

const (
	SyncAlways = disk.SyncAlways
	SyncManual = disk.SyncManual
)

// Log is a figwal write-ahead log. Reads are lock-free over an
// immutable snapshot held in an atomic.Pointer; many goroutines can
// read in parallel with zero contention. Writes serialize on a
// writer mutex, fsync to disk (per SyncMode), then publish a new
// snapshot. A reader either sees the pre-write or post-write snapshot;
// never a partial state.
//
// Forks reshape both the on-disk log and the in-memory cache. The
// parent's snapshot is truncated to [first, atIdx-1] (a reslice that
// shares the underlying entry array), and the child gets a fresh
// snapshot whose parent pointer is the truncated trunk. Sibling forks
// share parent state by pointer.
type Log struct {
	inner *disk.Log
	wmu   sync.Mutex
	snap  atomic.Pointer[cacheSnapshot]
}

// cacheSnapshot is an immutable view of a Log's entries. The entries
// slice is keyed by `firstIdx + i`. For forked children, parent (with
// forkBase) covers indices below firstIdx.
//
// Once a snapshot is published via atomic.Store, its fields are not
// mutated. New snapshots are built by re-slicing or appending.
type cacheSnapshot struct {
	firstIdx uint64
	entries  [][]byte
	parent   *cacheSnapshot
	forkBase uint64
}

// Open opens (or creates) a figwal log at dir and loads its entries
// into an in-memory cache. For a forked dir, the parent chain is
// materialized too.
func Open(dir string, opts Options) (*Log, error) {
	inner, err := disk.Open(dir, opts)
	if err != nil {
		return nil, err
	}
	l := &Log{inner: inner}
	snap, err := buildSnapshotFromDisk(inner)
	if err != nil {
		inner.Close()
		return nil, err
	}
	l.snap.Store(snap)
	return l, nil
}

// buildSnapshotFromDisk materializes a cacheSnapshot from a disk.Log,
// including the parent chain. Called once at Open; post-publish,
// snapshots evolve via Write/Fork.
func buildSnapshotFromDisk(l *disk.Log) (*cacheSnapshot, error) {
	var parentSnap *cacheSnapshot
	if p := l.Parent(); p != nil {
		ps, err := buildSnapshotFromDisk(p)
		if err != nil {
			return nil, err
		}
		parentSnap = ps
	}
	snap := &cacheSnapshot{
		parent:   parentSnap,
		forkBase: l.ForkBase(),
	}
	err := l.RangeOwn(0, func(idx uint64, payload []byte) error {
		cp := make([]byte, len(payload))
		copy(cp, payload)
		if len(snap.entries) == 0 {
			snap.firstIdx = idx
		}
		snap.entries = append(snap.entries, cp)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return snap, nil
}

// Read returns the payload at idx. Lock-free.
func (l *Log) Read(idx uint64) ([]byte, error) {
	return l.snap.Load().read(idx)
}

// Range iterates over entries from `from` to LastIndex, calling fn
// for each. Lock-free: a single snapshot Load fixes the view for the
// duration of the iteration; later writes are not visible. Walks the
// parent chain for indices below the fork's forkBase.
func (l *Log) Range(from uint64, fn func(idx uint64, payload []byte) error) error {
	return l.snap.Load().rangeFromIdx(from, fn)
}

// FirstIndex returns the first index visible from this Log, walking
// the parent chain if it's a fork.
func (l *Log) FirstIndex() uint64 {
	return l.snap.Load().firstIndexRecursive()
}

// LastIndex returns the highest index in this Log's own entries. For
// an empty fork it returns forkBase - 1 to reflect that the next
// write will be forkBase.
func (l *Log) LastIndex() uint64 {
	s := l.snap.Load()
	if n := len(s.entries); n > 0 {
		return s.firstIdx + uint64(n) - 1
	}
	if s.forkBase > 0 {
		return s.forkBase - 1
	}
	return 0
}

// Write appends an entry. Disk write + fsync (per SyncMode) happens
// under the writer mutex, then the cache snapshot is replaced
// atomically.
func (l *Log) Write(idx uint64, payload []byte) error {
	l.wmu.Lock()
	defer l.wmu.Unlock()

	if err := l.inner.Write(idx, payload); err != nil {
		return err
	}
	old := l.snap.Load()
	cp := make([]byte, len(payload))
	copy(cp, payload)
	entries := append(old.entries, cp)
	firstIdx := old.firstIdx
	if len(old.entries) == 0 {
		firstIdx = idx
	}
	l.snap.Store(&cacheSnapshot{
		firstIdx: firstIdx,
		entries:  entries,
		parent:   old.parent,
		forkBase: old.forkBase,
	})
	return nil
}

// Sync forces an fsync on the active segment.
func (l *Log) Sync() error { return l.inner.Sync() }

// Hash returns the codec's integrity token for the payload at idx.
func (l *Log) Hash(idx uint64) (string, error) {
	payload, err := l.Read(idx)
	if err != nil {
		return "", err
	}
	return l.inner.HashPayload(payload)
}

// Fork splits this Log at atIdx. See disk.Log.Fork for semantics.
func (l *Log) Fork(atIdx uint64, name string, oldFutureNameOpt ...string) (*Log, error) {
	l.wmu.Lock()
	defer l.wmu.Unlock()

	childInner, err := l.inner.Fork(atIdx, name, oldFutureNameOpt...)
	if err != nil {
		return nil, err
	}

	old := l.snap.Load()
	keep := uint64(0)
	if atIdx > old.firstIdx {
		keep = atIdx - old.firstIdx
	}
	if keep > uint64(len(old.entries)) {
		return nil, fmt.Errorf("log fork: keep %d exceeds entries %d",
			keep, len(old.entries))
	}
	truncated := &cacheSnapshot{
		firstIdx: old.firstIdx,
		entries:  old.entries[:keep],
		parent:   old.parent,
		forkBase: old.forkBase,
	}
	l.snap.Store(truncated)

	child := &Log{inner: childInner}
	child.snap.Store(&cacheSnapshot{
		parent:   truncated,
		forkBase: atIdx,
	})
	return child, nil
}

// TruncateFront drops entries below beforeIdx, both on disk and in
// the cache. Only this Log's own entries are affected; parent
// entries are untouched.
func (l *Log) TruncateFront(beforeIdx uint64) error {
	l.wmu.Lock()
	defer l.wmu.Unlock()
	if err := l.inner.TruncateFront(beforeIdx); err != nil {
		return err
	}
	old := l.snap.Load()
	if len(old.entries) == 0 || beforeIdx <= old.firstIdx {
		return nil
	}
	cut := beforeIdx - old.firstIdx
	if cut > uint64(len(old.entries)) {
		cut = uint64(len(old.entries))
	}
	l.snap.Store(&cacheSnapshot{
		firstIdx: old.firstIdx + cut,
		entries:  old.entries[cut:],
		parent:   old.parent,
		forkBase: old.forkBase,
	})
	return nil
}

// Disk returns the underlying disk.Log for advanced operations.
// Bypassing the Log's writer mutex will desync the cache; prefer the
// Log methods on the hot path.
func (l *Log) Disk() *disk.Log { return l.inner }

// Close closes the underlying disk.Log. Parent logs auto-opened
// during Open are not closed automatically; manage them via a Store
// or explicit handles for shared lifetimes.
func (l *Log) Close() error { return l.inner.Close() }

// Snapshot exposes the current snapshot pointer for callers that
// want a point-in-time consistent view across many operations
// (typical use: dump the entire log to the network as of "now").
func (l *Log) Snapshot() *Snapshot {
	return &Snapshot{s: l.snap.Load()}
}

// Snapshot is an immutable point-in-time view of a Log. All access
// is lock-free; later writes to the Log are not visible.
type Snapshot struct{ s *cacheSnapshot }

func (s *Snapshot) Read(idx uint64) ([]byte, error) { return s.s.read(idx) }
func (s *Snapshot) Range(from uint64, fn func(idx uint64, payload []byte) error) error {
	return s.s.rangeFromIdx(from, fn)
}
func (s *Snapshot) FirstIndex() uint64 { return s.s.firstIndexRecursive() }
func (s *Snapshot) LastIndex() uint64 {
	if n := len(s.s.entries); n > 0 {
		return s.s.firstIdx + uint64(n) - 1
	}
	if s.s.forkBase > 0 {
		return s.s.forkBase - 1
	}
	return 0
}

// --- cacheSnapshot methods ---

func (s *cacheSnapshot) read(idx uint64) ([]byte, error) {
	if s.parent != nil && idx < s.forkBase {
		return s.parent.read(idx)
	}
	if len(s.entries) == 0 || idx < s.firstIdx {
		return nil, ErrNotFound
	}
	i := idx - s.firstIdx
	if i >= uint64(len(s.entries)) {
		return nil, ErrNotFound
	}
	return s.entries[i], nil
}

func (s *cacheSnapshot) rangeFromIdx(from uint64, fn func(idx uint64, payload []byte) error) error {
	if s.parent != nil && from < s.forkBase {
		if err := s.parent.rangeFromIdx(from, fn); err != nil {
			return err
		}
		from = s.forkBase
	}
	if len(s.entries) == 0 {
		return nil
	}
	start := s.firstIdx
	if from > start {
		start = from
	}
	end := s.firstIdx + uint64(len(s.entries))
	for i := start; i < end; i++ {
		if err := fn(i, s.entries[i-s.firstIdx]); err != nil {
			return err
		}
	}
	return nil
}

func (s *cacheSnapshot) firstIndexRecursive() uint64 {
	if s.parent != nil {
		return s.parent.firstIndexRecursive()
	}
	if len(s.entries) == 0 {
		return 0
	}
	return s.firstIdx
}
