// Package log is the caller-facing figwal log: append-only, segment
// rotated, fork-able, with an in-memory cache that serves reads
// lock-free over an atomic snapshot pointer. The underlying disk
// layout and segment management live in figwal/disk; this package
// wraps them.
package log

import (
	"errors"
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
	// ErrSharedMutation rejects filesystem topology changes through a
	// Store-owned log. Retire the shared generation and reopen privately
	// before forking or truncating.
	ErrSharedMutation = errors.New("log topology mutation requires a private log")
	errRangeBoundary  = errors.New("log cache range reached fork boundary")
)

// Options aliases disk.Options so the caller-facing API is a single
// package. Code that already imported figwal/log keeps working with a
// trivial type-name change at the import site.
type Options = disk.Options

// Log is a figwal write-ahead log. Reads are lock-free over an
// immutable snapshot held in an atomic.Pointer; many goroutines can
// read in parallel with zero contention. Writes serialize on a
// writer mutex, publish a new snapshot immediately, and buffer the
// payload for a later Flush; disk follows memory with bounded lag. A
// reader either sees the pre-write or post-write snapshot; never a
// partial state.
//
// Forks reshape both the on-disk log and the in-memory cache. The
// parent's snapshot is truncated to [first, atIdx-1] (a reslice that
// shares the underlying entry array), and the child gets a fresh
// snapshot whose parent pointer is the truncated trunk. Sibling forks
// share parent state by pointer.
type Log struct {
	inner  *disk.Log
	wmu    sync.Mutex
	fmu    sync.Mutex
	snap   atomic.Pointer[cacheSnapshot]
	shared bool

	pending      [][]byte
	pendingFirst uint64
}

func (s *cacheSnapshot) scanFromEnd(from uint64, fn func(idx uint64, payload []byte) error) error {
	if len(s.entries) > 0 {
		first := s.firstIdx
		idx := first + uint64(len(s.entries)) - 1
		if from != 0 && from < idx {
			idx = from
		}
		if idx >= first {
			for {
				if err := fn(idx, s.entries[idx-first]); err != nil {
					return err
				}
				if idx == first {
					break
				}
				idx--
			}
		}
	}
	if s.parent != nil && s.forkBase > 0 {
		parentFrom := s.forkBase - 1
		if from != 0 && from < parentFrom {
			parentFrom = from
		}
		return s.parent.scanFromEnd(parentFrom, fn)
	}
	return nil
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
	return buildOwnSnapshot(l, parentSnap)
}

func buildOwnSnapshot(l *disk.Log, parentSnap *cacheSnapshot) (*cacheSnapshot, error) {
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

// ScanFromEnd iterates entries in descending index order from from (or the
// current tail when from is past it). It reads the immutable cache snapshot,
// including fork prefixes, instead of re-reading disk segments.
func (l *Log) ScanFromEnd(from uint64, fn func(idx uint64, payload []byte) error) error {
	return l.snap.Load().scanFromEnd(from, fn)
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

// Write appends an entry to the in-memory snapshot and buffers it for
// the next Flush. It returns without touching disk.
func (l *Log) Write(idx uint64, payload []byte) error {
	l.wmu.Lock()
	defer l.wmu.Unlock()

	if l.inner.IsReadOnly() {
		return fmt.Errorf("%w: fork parent", ErrReadOnly)
	}
	old := l.snap.Load()
	expected := uint64(1)
	if n := len(old.entries); n > 0 {
		expected = old.firstIdx + uint64(n)
	} else if old.forkBase > 0 {
		expected = old.forkBase
	}
	if idx != expected {
		return fmt.Errorf("%w: got %d, want %d", disk.ErrOutOfOrder, idx, expected)
	}
	cp := make([]byte, len(payload))
	copy(cp, payload)
	entries := append(old.entries, cp)
	firstIdx := old.firstIdx
	if len(old.entries) == 0 {
		firstIdx = idx
	}
	if len(l.pending) == 0 {
		l.pendingFirst = idx
	}
	l.pending = append(l.pending, cp)
	l.snap.Store(&cacheSnapshot{
		firstIdx: firstIdx,
		entries:  entries,
		parent:   old.parent,
		forkBase: old.forkBase,
	})
	return nil
}

// Flush persists every buffered entry and fsyncs. Appends are only
// briefly blocked (buffer bookkeeping); the disk IO runs outside the
// writer mutex.
func (l *Log) Flush() error { return l.FlushTo(^uint64(0)) }

// FlushTo persists buffered entries with index <= target and fsyncs.
func (l *Log) FlushTo(target uint64) error {
	l.fmu.Lock()
	defer l.fmu.Unlock()
	l.wmu.Lock()
	n := 0
	if len(l.pending) > 0 && target >= l.pendingFirst {
		n = len(l.pending)
		if span := target - l.pendingFirst + 1; span < uint64(n) {
			n = int(span)
		}
	}
	batch := l.pending[:n:n]
	first := l.pendingFirst
	l.wmu.Unlock()
	if n == 0 {
		return nil
	}
	for i, p := range batch {
		if err := l.inner.Write(first+uint64(i), p); err != nil {
			return err
		}
	}
	if err := l.inner.Sync(); err != nil {
		return err
	}
	l.wmu.Lock()
	l.pending = l.pending[n:]
	l.pendingFirst = first + uint64(n)
	l.wmu.Unlock()
	return nil
}

// PendingBounds reports the unflushed index range, if any.
func (l *Log) PendingBounds() (first, last uint64, ok bool) {
	l.wmu.Lock()
	defer l.wmu.Unlock()
	if len(l.pending) == 0 {
		return 0, 0, false
	}
	return l.pendingFirst, l.pendingFirst + uint64(len(l.pending)) - 1, true
}

// Hash returns the codec's integrity token for the payload at idx.
func (l *Log) Hash(idx uint64) (string, error) {
	payload, err := l.Read(idx)
	if err != nil {
		return "", err
	}
	return l.inner.HashPayload(payload)
}

// Fork splits this Log at atIdx. See disk.Log.Fork for semantics
// (N-ary siblings, re-split below an existing branch point). The cache
// snapshot is updated for this handle and the returned child; sibling
// handles already open in memory are NOT updated on a re-split-below —
// their in-memory parent pointer goes stale. Reopen affected
// descendants (the on-disk layout is always correct).
func (l *Log) Fork(atIdx uint64, name string, oldFutureNameOpt ...string) (*Log, error) {
	if l.shared {
		return nil, ErrSharedMutation
	}
	if err := l.Flush(); err != nil {
		return nil, err
	}
	l.wmu.Lock()
	defer l.wmu.Unlock()
	if len(l.pending) > 0 {
		return nil, fmt.Errorf("log fork: raced a concurrent write")
	}

	childInner, err := l.inner.Fork(atIdx, name, oldFutureNameOpt...)
	if err != nil {
		return nil, err
	}
	return l.forkCached(atIdx, childInner)
}

// ForkRehome forks with an explicit list of children to move into the old
// future. It has the same cache semantics as Fork.
func (l *Log) ForkRehome(atIdx uint64, name, oldFutureName string, rehome []string) (*Log, error) {
	if l.shared {
		return nil, ErrSharedMutation
	}
	if err := l.Flush(); err != nil {
		return nil, err
	}
	l.wmu.Lock()
	defer l.wmu.Unlock()
	if len(l.pending) > 0 {
		return nil, fmt.Errorf("log fork: raced a concurrent write")
	}

	childInner, err := l.inner.ForkRehome(atIdx, name, oldFutureName, rehome)
	if err != nil {
		return nil, err
	}
	return l.forkCached(atIdx, childInner)
}

func (l *Log) forkCached(atIdx uint64, childInner *disk.Log) (*Log, error) {
	old := l.snap.Load()
	keep := uint64(0)
	if len(old.entries) > 0 && atIdx > old.firstIdx {
		keep = atIdx - old.firstIdx
	}
	if keep > uint64(len(old.entries)) {
		return nil, fmt.Errorf("log fork: keep %d exceeds entries %d",
			keep, len(old.entries))
	}
	truncated := &cacheSnapshot{
		firstIdx: old.firstIdx,
		// Clamp capacity so a future append cannot overwrite suffix slots
		// still visible through a pre-fork snapshot.
		entries:  old.entries[:keep:keep],
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
	if l.shared {
		return ErrSharedMutation
	}
	if err := l.Flush(); err != nil {
		return err
	}
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

// ForkBase returns the first index this log owns, or zero for a root log.
func (l *Log) ForkBase() uint64 { return l.inner.ForkBase() }

// RangeOwn delegates an own-entry iteration for topology operations. Normal
// reads should use Range so they stay on the immutable cache.
func (l *Log) RangeOwn(from uint64, fn func(idx uint64, payload []byte) error) error {
	return l.inner.RangeOwn(from, fn)
}

// ChildForkBases reports child fork boundaries for topology planning.
func (l *Log) ChildForkBases() (map[string]uint64, error) {
	return l.inner.ChildForkBases()
}

// StateAt reconstructs a header-mode state from the on-disk watermark.
// The disk fold must see every appended patch, so buffered entries are
// flushed first.
func (l *Log) StateAt(idx uint64) ([]byte, error) {
	if err := l.Flush(); err != nil {
		return nil, err
	}
	return l.inner.StateAt(idx)
}

// SegmentBaseIndexes returns this log's own segment bases.
func (l *Log) SegmentBaseIndexes() []uint64 { return l.inner.SegmentBaseIndexes() }

// Close flushes buffered entries and closes the underlying disk.Log.
// Parent logs auto-opened during Open are not closed automatically;
// manage them via a Store or explicit handles for shared lifetimes.
func (l *Log) Close() error {
	if l.shared {
		return nil
	}
	flushErr := l.Flush()
	if err := l.inner.Close(); err != nil {
		return err
	}
	return flushErr
}

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
func (s *Snapshot) ScanFromEnd(from uint64, fn func(idx uint64, payload []byte) error) error {
	return s.s.scanFromEnd(from, fn)
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
		err := s.parent.rangeFromIdx(from, func(idx uint64, payload []byte) error {
			if idx >= s.forkBase {
				return errRangeBoundary
			}
			return fn(idx, payload)
		})
		if err != nil && !errors.Is(err, errRangeBoundary) {
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
		if first := s.parent.firstIndexRecursive(); first != 0 {
			return first
		}
	}
	if len(s.entries) == 0 {
		return 0
	}
	return s.firstIdx
}
