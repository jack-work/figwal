package log

import (
	"figwal/segment"
	"fmt"
	"sync"
	"sync/atomic"
)

// Cached wraps a Log with an in-memory mirror of its entries. Reads
// serve out of the cache without acquiring any lock: an atomic
// pointer points at the current immutable cacheSnapshot, readers
// Load it and access the entry slice directly. Many goroutines can
// read in parallel with zero contention.
//
// Writes serialize on a writer mutex. Each write fsyncs to disk
// (per the underlying Log's SyncMode), then publishes a new
// snapshot pointer that includes the appended entry. Concurrent
// readers either see the pre-write snapshot or the post-write one;
// they never see a partial state.
//
// Forks reshape both the on-disk log and the cache. The parent's
// snapshot is truncated to [first, atIdx-1] (a reslice that shares
// the underlying entry array), and the child Cached gets a fresh
// snapshot whose parent pointer is the truncated trunk snapshot.
// Sibling forks share parent state by pointer.
type Cached struct {
	log  *Log
	wmu  sync.Mutex
	snap atomic.Pointer[cacheSnapshot]
}

// cacheSnapshot is an immutable view of a Cached log's entries. The
// entries slice is keyed by `firstIdx + i`. For forked children,
// parent (with forkBase) covers indices below firstIdx.
//
// Once a snapshot is published via atomic.Store, its fields are not
// mutated. New snapshots are built by re-slicing or appending.
type cacheSnapshot struct {
	firstIdx uint64
	entries  [][]byte
	parent   *cacheSnapshot
	forkBase uint64
}

// OpenCached opens a Log and loads its entries into an in-memory
// cache. For a forked dir, the parent chain is materialized too via
// a Range walk through the underlying Log.
func OpenCached(dir string, opts Options) (*Cached, error) {
	l, err := Open(dir, opts)
	if err != nil {
		return nil, err
	}
	c := &Cached{log: l}
	snap, err := buildSnapshotFromLog(l)
	if err != nil {
		l.Close()
		return nil, err
	}
	c.snap.Store(snap)
	return c, nil
}

// buildSnapshotFromLog materializes a cacheSnapshot from a Log,
// including the parent chain. Called once at OpenCached time;
// post-publish, snapshots evolve via Write/Fork.
func buildSnapshotFromLog(l *Log) (*cacheSnapshot, error) {
	var parentSnap *cacheSnapshot
	if l.parent != nil {
		ps, err := buildSnapshotFromLog(l.parent)
		if err != nil {
			return nil, err
		}
		parentSnap = ps
	}
	snap := &cacheSnapshot{
		parent:   parentSnap,
		forkBase: l.forkBase,
	}
	// Iterate own segments only. We cannot use l.Range here because
	// Range walks the parent chain via Log.Read; we want our own
	// entries materialized into snap.entries and parent's materialized
	// into parentSnap.
	l.mu.RLock()
	own := l.sealed
	active := l.active
	l.mu.RUnlock()
	loadSeg := func(s *segment.Segment) error {
		for i := uint64(0); i < s.Count(); i++ {
			payload, err := s.ReadIndex(i)
			if err != nil {
				return err
			}
			cp := make([]byte, len(payload))
			copy(cp, payload)
			if len(snap.entries) == 0 {
				snap.firstIdx = s.BaseIndex() + i
			}
			snap.entries = append(snap.entries, cp)
		}
		return nil
	}
	for _, s := range own {
		if err := loadSeg(s); err != nil {
			return nil, err
		}
	}
	if active != nil {
		if err := loadSeg(active); err != nil {
			return nil, err
		}
	}
	return snap, nil
}

// Read returns the payload at idx. Lock-free: loads the current
// snapshot and walks the parent chain for low indices.
func (c *Cached) Read(idx uint64) ([]byte, error) {
	return c.snap.Load().read(idx)
}

// Range iterates over entries from `from` to LastIndex, calling fn
// for each. Lock-free: a single snapshot Load fixes the view for the
// duration of the iteration; later writes are not visible. Walks the
// parent chain for indices below the fork's forkBase.
func (c *Cached) Range(from uint64, fn func(idx uint64, payload []byte) error) error {
	return c.snap.Load().rangeFromIdx(from, fn)
}

// FirstIndex returns the first index visible from this Cached,
// walking the parent chain if it's a fork.
func (c *Cached) FirstIndex() uint64 {
	return c.snap.Load().firstIndexRecursive()
}

// LastIndex returns the highest index in this Cached's own entries.
// For an empty fork it returns forkBase - 1 to reflect that the next
// write will be forkBase.
func (c *Cached) LastIndex() uint64 {
	s := c.snap.Load()
	if n := len(s.entries); n > 0 {
		return s.firstIdx + uint64(n) - 1
	}
	if s.forkBase > 0 {
		return s.forkBase - 1
	}
	return 0
}

// Write appends an entry. Disk write + fsync (per SyncMode) happens
// under the writer mutex, then the cache snapshot is replaced atomically.
func (c *Cached) Write(idx uint64, payload []byte) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()

	if err := c.log.Write(idx, payload); err != nil {
		return err
	}
	old := c.snap.Load()
	cp := make([]byte, len(payload))
	copy(cp, payload)
	entries := append(old.entries, cp)
	firstIdx := old.firstIdx
	if len(old.entries) == 0 {
		firstIdx = idx
	}
	c.snap.Store(&cacheSnapshot{
		firstIdx: firstIdx,
		entries:  entries,
		parent:   old.parent,
		forkBase: old.forkBase,
	})
	return nil
}

// Sync proxies to the underlying Log.
func (c *Cached) Sync() error { return c.log.Sync() }

// Fork splits this Cached. Disk fork executes via Log.Fork under the
// writer mutex; the in-memory snapshot is then resliced to the prefix
// and a child Cached is constructed with the trunk's truncated
// snapshot as its parent.
func (c *Cached) Fork(atIdx uint64, name string, oldFutureNameOpt ...string) (*Cached, error) {
	c.wmu.Lock()
	defer c.wmu.Unlock()

	childLog, err := c.log.Fork(atIdx, name, oldFutureNameOpt...)
	if err != nil {
		return nil, err
	}

	old := c.snap.Load()
	// Compute how many of our own entries stay in the prefix.
	keep := uint64(0)
	if atIdx > old.firstIdx {
		keep = atIdx - old.firstIdx
	}
	if keep > uint64(len(old.entries)) {
		return nil, fmt.Errorf("cached fork: keep %d exceeds entries %d",
			keep, len(old.entries))
	}
	truncated := &cacheSnapshot{
		firstIdx: old.firstIdx,
		entries:  old.entries[:keep],
		parent:   old.parent,
		forkBase: old.forkBase,
	}
	c.snap.Store(truncated)

	child := &Cached{log: childLog}
	child.snap.Store(&cacheSnapshot{
		parent:   truncated,
		forkBase: atIdx,
	})
	return child, nil
}

// TruncateFront drops the in-memory cache of entries below beforeIdx
// once the on-disk truncation succeeds. Only entries in this Cached's
// own snapshot are affected; parent entries are untouched.
func (c *Cached) TruncateFront(beforeIdx uint64) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	if err := c.log.TruncateFront(beforeIdx); err != nil {
		return err
	}
	old := c.snap.Load()
	if len(old.entries) == 0 || beforeIdx <= old.firstIdx {
		return nil
	}
	cut := beforeIdx - old.firstIdx
	if cut > uint64(len(old.entries)) {
		cut = uint64(len(old.entries))
	}
	c.snap.Store(&cacheSnapshot{
		firstIdx: old.firstIdx + cut,
		entries:  old.entries[cut:],
		parent:   old.parent,
		forkBase: old.forkBase,
	})
	return nil
}

// Log returns the underlying Log for advanced operations. Bypassing
// Cached's mutex for writes will desync the cache; use the Cached
// methods on the hot path.
func (c *Cached) Log() *Log { return c.log }

// Close closes the underlying Log. Parent logs that were auto-opened
// during OpenCached are not closed automatically; manage them via a
// Store or explicit handles for shared lifetimes.
func (c *Cached) Close() error { return c.log.Close() }

// Snapshot exposes the current snapshot pointer for callers that
// want a point-in-time consistent view across many operations
// (typical use: dump the entire log to the network as of "now").
func (c *Cached) Snapshot() *Snapshot {
	return &Snapshot{s: c.snap.Load()}
}

// Snapshot is an immutable point-in-time view of a Cached log. All
// access is lock-free; later writes to the Cached are not visible.
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
