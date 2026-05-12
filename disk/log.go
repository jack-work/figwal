package disk

import (
	"errors"
	"github.com/jack-work/figwal/segment"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const (
	defaultSegSize = 64 * 1024 * 1024
	segNameWidth   = 20
)

var (
	ErrOutOfOrder      = errors.New("write index out of order")
	ErrNotFound        = errors.New("index not found")
	ErrEmpty           = errors.New("log is empty")
	ErrCodecMismatch   = errors.New("log directory contains segments with a different codec")
	ErrPayloadTooLarge = errors.New("payload too large for segment size")
	ErrReadOnly        = errors.New("log is read-only (branch point with child forks)")
	ErrForkMismatch    = errors.New(".fork base does not match first segment baseIndex")
)

// SyncMode controls when Write fsyncs the active segment.
type SyncMode int

const (
	// SyncAlways fsyncs after every Write (default; the standard WAL
	// durability guarantee).
	SyncAlways SyncMode = iota
	// SyncManual disables automatic fsync. Callers must invoke
	// Log.Sync() to make writes durable. Useful for batched workloads
	// that drive their own commit cadence.
	SyncManual
)

// Options configures a Log.
type Options struct {
	SegmentSize int64                // 0 = default
	Codec       segment.SegmentCodec // nil = segment.BinaryCodec{}
	SyncMode    SyncMode             // zero value = SyncAlways
	// Parent, if non-nil, is the log this dir was forked from. When nil
	// and the dir contains a .fork marker file, Open auto-walks `..` to
	// resolve the parent. Use a Store if you need to deduplicate parent
	// instances across many sibling forks.
	Parent *Log
}

type Log struct {
	mu       sync.RWMutex
	dir      string
	opts     Options
	codec    segment.SegmentCodec
	ext      string
	sealed   []*segment.Segment
	active   *segment.Segment
	parent   *Log   // nil for root logs
	forkBase uint64 // first index this log owns; 0 if not a fork
	readOnly bool   // true when the dir has child subdirs (branch point)
}

func Open(dir string, opts Options) (*Log, error) {
	if opts.SegmentSize == 0 {
		opts.SegmentSize = defaultSegSize
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	// Auto-detect codec from the on-disk extension when the caller did
	// not specify one. Empty dirs default to binary.
	if opts.Codec == nil {
		detected, err := detectCodec(dir)
		if err != nil {
			return nil, err
		}
		if detected != nil {
			opts.Codec = detected
		} else {
			opts.Codec = segment.BinaryCodec{}
		}
	}
	// A leftover fork sentinel means a previous fork operation crashed
	// before completing; refuse to open until an operator resolves it.
	if pending, err := hasForkPending(dir); err != nil {
		return nil, err
	} else if pending {
		return nil, fmt.Errorf("%w: %s", ErrForkPending, dir)
	}
	base, err := readForkMarker(dir)
	if err != nil {
		return nil, err
	}
	parent := opts.Parent
	if parent == nil && base > 0 {
		// Auto-walk `..` for the parent. Inherit codec/segment options
		// so the parent reads its own files consistently. Plain Open
		// without dedup; callers that need sharing should use a Store.
		parentDir := filepath.Dir(dir)
		parentOpts := opts
		parentOpts.Parent = nil // walk fully up the chain
		p, err := Open(parentDir, parentOpts)
		if err != nil {
			return nil, fmt.Errorf("auto-open parent %q: %w", parentDir, err)
		}
		parent = p
	}
	hasKids, err := hasSubdirs(dir)
	if err != nil {
		return nil, err
	}
	l := &Log{
		dir:      dir,
		opts:     opts,
		codec:    opts.Codec,
		ext:      opts.Codec.FileExt(),
		parent:   parent,
		forkBase: base,
		readOnly: hasKids,
	}
	if err := l.loadSegments(); err != nil {
		return nil, err
	}
	if base > 0 && len(l.sealed)+boolToInt(l.active != nil) > 0 {
		// Validate the on-disk first segment lines up with the marker.
		var firstBase uint64
		if len(l.sealed) > 0 {
			firstBase = l.sealed[0].FirstIndex()
		} else {
			firstBase = l.active.FirstIndex()
		}
		if firstBase != base {
			return nil, fmt.Errorf("%w: marker=%d firstSegment=%d",
				ErrForkMismatch, base, firstBase)
		}
	}
	slog.Info("log opened",
		"dir", dir,
		"codec", l.codec.Name(),
		"segmentSize", opts.SegmentSize,
		"sealed", len(l.sealed),
		"hasActive", l.active != nil,
		"readOnly", l.readOnly,
		"forkBase", l.forkBase,
		"hasParent", l.parent != nil)
	return l, nil
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func (l *Log) Write(idx uint64, payload []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.readOnly {
		return fmt.Errorf("%w: %s", ErrReadOnly, l.dir)
	}

	expected := l.lastIndexLocked() + 1
	if l.isEmptyLocked() {
		// Fresh log: first index is 1 for a root, forkBase for a fork.
		if l.forkBase > 0 {
			expected = l.forkBase
		} else {
			expected = 1
		}
	}
	if idx != expected {
		return fmt.Errorf("%w: got %d, want %d", ErrOutOfOrder, idx, expected)
	}

	if l.active == nil {
		if err := l.openActiveLocked(idx); err != nil {
			return err
		}
	}

	_, err := l.active.Append(payload)
	if errors.Is(err, segment.ErrFull) {
		if l.active.Count() == 0 {
			return fmt.Errorf("%w: segmentSize=%d", ErrPayloadTooLarge, l.opts.SegmentSize)
		}
		if err := l.rotateLocked(idx); err != nil {
			return err
		}
		_, err = l.active.Append(payload)
		if errors.Is(err, segment.ErrFull) {
			return fmt.Errorf("%w: segmentSize=%d", ErrPayloadTooLarge, l.opts.SegmentSize)
		}
	}
	if err != nil {
		return err
	}
	if l.opts.SyncMode == SyncAlways {
		return l.active.Sync()
	}
	return nil
}

// Sync fsyncs the active segment. Useful with SyncManual to flush a
// batch of writes, or as an extra durability point with SyncAlways.
func (l *Log) Sync() error {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.active == nil {
		return nil
	}
	return l.active.Sync()
}

// Range iterates entries from `from` to the current LastIndex, calling
// fn for each. If fn returns a non-nil error, iteration stops and that
// error is returned. If `from` is below FirstIndex, iteration begins at
// FirstIndex. For forks, low indices are served from the parent chain.
func (l *Log) Range(from uint64, fn func(idx uint64, payload []byte) error) error {
	// Walk parent chain for indices below this fork's range. After a
	// fork the parent's segments are truncated to end at forkBase-1, so
	// parent.Range will not yield beyond our forkBase on its own.
	if l.parent != nil && from < l.forkBase {
		if err := l.parent.Range(from, fn); err != nil {
			return err
		}
		from = l.forkBase
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.isEmptyLocked() {
		return nil
	}
	first := l.firstIndexLocked()
	last := l.lastIndexLocked()
	if from < first {
		from = first
	}
	cur := from
	for cur <= last {
		s := l.findSegmentLocked(cur)
		if s == nil {
			return ErrNotFound
		}
		segEnd := s.LastIndex()
		if segEnd > last {
			segEnd = last
		}
		for i := cur; i <= segEnd; i++ {
			payload, err := s.ReadIndex(i - s.BaseIndex())
			if err != nil {
				return err
			}
			if err := fn(i, payload); err != nil {
				return err
			}
		}
		cur = segEnd + 1
	}
	return nil
}

// TruncateFront removes whole sealed segments whose LastIndex is below
// beforeIdx. Partial segments (those containing entries on both sides
// of beforeIdx) and the active segment are left intact; callers should
// size segments so the compaction granularity matches their needs.
//
// After deleting files, the directory is fsynced so the unlinks are
// durable.
func (l *Log) TruncateFront(beforeIdx uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.sealed) == 0 {
		return nil
	}
	kept := make([]*segment.Segment, 0, len(l.sealed))
	dropped := 0
	for _, s := range l.sealed {
		if s.LastIndex() < beforeIdx {
			path := s.Path()
			if err := s.Close(); err != nil {
				return err
			}
			if err := os.Remove(path); err != nil {
				return err
			}
			dropped++
			continue
		}
		kept = append(kept, s)
	}
	l.sealed = kept
	if dropped == 0 {
		return nil
	}
	slog.Info("log truncated",
		"dir", l.dir,
		"beforeIdx", beforeIdx,
		"droppedSegments", dropped,
		"remainingSealed", len(l.sealed))
	return syncDir(l.dir)
}

func (l *Log) Read(idx uint64) ([]byte, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	// Indices below this fork's range live in the parent. Delegate
	// recursively; the parent's own lock protects its state.
	if l.parent != nil && idx < l.forkBase {
		return l.parent.Read(idx)
	}
	if l.isEmptyLocked() {
		return nil, ErrEmpty
	}
	s := l.findSegmentLocked(idx)
	if s == nil {
		return nil, ErrNotFound
	}
	return s.ReadIndex(idx - s.BaseIndex())
}

// FirstIndex returns the first index visible from this log, walking
// the parent chain if this log is a fork.
func (l *Log) FirstIndex() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.parent != nil {
		return l.parent.FirstIndex()
	}
	return l.firstIndexLocked()
}

// LastIndex returns the highest index in this log's own segments. For
// a fork with no local entries yet, LastIndex returns forkBase-1 to
// reflect that the fork starts immediately after the parent.
func (l *Log) LastIndex() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if last := l.lastIndexLocked(); last > 0 {
		return last
	}
	if l.forkBase > 0 {
		return l.forkBase - 1
	}
	return 0
}

func (l *Log) Hash(idx uint64) (string, error) {
	b, err := l.Read(idx)
	if err != nil {
		return "", err
	}
	return l.codec.Hash(b)
}

// HashPayload returns this log's codec hash for an arbitrary payload.
// Cheaper than Hash when the caller already has the payload in hand.
func (l *Log) HashPayload(payload []byte) (string, error) {
	return l.codec.Hash(payload)
}

// Parent returns the parent log if this one is a fork, or nil for a
// trunk. Exposed so higher layers (e.g. the cached Log wrapper) can
// walk the fork chain without poking internal fields.
func (l *Log) Parent() *Log { return l.parent }

// ForkBase returns the global index at which this log begins relative
// to its parent. Zero for a trunk.
func (l *Log) ForkBase() uint64 { return l.forkBase }

// RangeOwn iterates over this log's own entries (does NOT walk the
// parent chain), calling fn for each entry from the given index
// forward. A read lock is held for the duration of the iteration so
// new writes cannot append while a caller is materializing entries.
// Intended for cache-snapshot construction.
func (l *Log) RangeOwn(from uint64, fn func(idx uint64, payload []byte) error) error {
	l.mu.RLock()
	defer l.mu.RUnlock()
	walk := func(s *segment.Segment) error {
		base := s.BaseIndex()
		count := s.Count()
		for i := uint64(0); i < count; i++ {
			idx := base + i
			if idx < from {
				continue
			}
			payload, err := s.ReadIndex(i)
			if err != nil {
				return err
			}
			if err := fn(idx, payload); err != nil {
				return err
			}
		}
		return nil
	}
	for _, s := range l.sealed {
		if err := walk(s); err != nil {
			return err
		}
	}
	if l.active != nil {
		if err := walk(l.active); err != nil {
			return err
		}
	}
	return nil
}

func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	var errs []error
	for _, s := range l.sealed {
		if err := s.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if l.active != nil {
		if err := l.active.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (l *Log) openActiveLocked(baseIndex uint64) error {
	path := filepath.Join(l.dir, l.segName(baseIndex))
	s, err := segment.Create(path, l.codec, baseIndex, l.opts.SegmentSize)
	if err != nil {
		return err
	}
	// fsync the directory so the new dentry survives a crash.
	if err := syncDir(l.dir); err != nil {
		s.Close()
		return err
	}
	l.active = s
	return nil
}

func (l *Log) rotateLocked(baseIndex uint64) error {
	if l.active != nil {
		if err := l.active.Sync(); err != nil {
			return err
		}
		slog.Info("segment rotated",
			"codec", l.codec.Name(),
			"sealedBase", l.active.BaseIndex(),
			"sealedLast", l.active.LastIndex(),
			"newBase", baseIndex)
		l.sealed = append(l.sealed, l.active)
		l.active = nil
	}
	return l.openActiveLocked(baseIndex)
}

func (l *Log) isEmptyLocked() bool {
	if len(l.sealed) > 0 {
		return false
	}
	return l.active == nil || l.active.Count() == 0
}

func (l *Log) firstIndexLocked() uint64 {
	if len(l.sealed) > 0 {
		return l.sealed[0].FirstIndex()
	}
	if l.active != nil && l.active.Count() > 0 {
		return l.active.FirstIndex()
	}
	return 0
}

func (l *Log) lastIndexLocked() uint64 {
	if l.active != nil && l.active.Count() > 0 {
		return l.active.LastIndex()
	}
	if n := len(l.sealed); n > 0 {
		return l.sealed[n-1].LastIndex()
	}
	return 0
}

func (l *Log) findSegmentLocked(idx uint64) *segment.Segment {
	for _, s := range l.sealed {
		if idx >= s.FirstIndex() && idx <= s.LastIndex() {
			return s
		}
	}
	if l.active != nil && l.active.Count() > 0 &&
		idx >= l.active.FirstIndex() && idx <= l.active.LastIndex() {
		return l.active
	}
	return nil
}

// known segment extensions, used to detect codec mismatch in a directory.
var knownExts = []string{".seg", ".jsonl"}

// detectCodec inspects dir for files with known codec extensions and
// returns the matching codec. Returns (nil, nil) if the dir is empty
// or contains no segment files (caller picks a default). Returns an
// ErrCodecMismatch when both extensions are present.
func detectCodec(dir string) (segment.SegmentCodec, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var seenBinary, seenJSONL bool
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if strings.HasSuffix(name, ".seg") {
			seenBinary = true
		}
		if strings.HasSuffix(name, ".jsonl") {
			seenJSONL = true
		}
	}
	if seenBinary && seenJSONL {
		return nil, fmt.Errorf("%w: dir %s has both .seg and .jsonl segments",
			ErrCodecMismatch, dir)
	}
	if seenBinary {
		return segment.BinaryCodec{}, nil
	}
	if seenJSONL {
		return segment.JSONLCodec{}, nil
	}
	return nil, nil
}

func (l *Log) loadSegments() error {
	entries, err := os.ReadDir(l.dir)
	if err != nil {
		return err
	}
	var bases []uint64
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasSuffix(name, l.ext) {
			for _, other := range knownExts {
				if other != l.ext && strings.HasSuffix(name, other) {
					return fmt.Errorf("%w: found %s in %s log",
						ErrCodecMismatch, name, l.codec.Name())
				}
			}
			continue
		}
		base, err := parseSegName(name, l.ext)
		if err != nil {
			return fmt.Errorf("bad segment name %q: %w", name, err)
		}
		bases = append(bases, base)
	}
	sort.Slice(bases, func(i, j int) bool { return bases[i] < bases[j] })
	for i, base := range bases {
		path := filepath.Join(l.dir, l.segName(base))
		isLast := i == len(bases)-1
		if isLast {
			s, err := segment.Open(path, l.codec, base, l.opts.SegmentSize)
			if err != nil {
				return fmt.Errorf("open active segment %d: %w", base, err)
			}
			l.active = s
		} else {
			s, err := segment.OpenReadOnly(path, l.codec, base)
			if err != nil {
				return fmt.Errorf("open sealed segment %d: %w", base, err)
			}
			l.sealed = append(l.sealed, s)
		}
	}
	return nil
}

// syncDir opens the directory and fsyncs it, durably persisting recent
// dentry changes (file creation, unlink) in that directory.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	if err := d.Sync(); err != nil {
		d.Close()
		return err
	}
	return d.Close()
}

func (l *Log) segName(base uint64) string {
	return fmt.Sprintf("%0*d%s", segNameWidth, base, l.ext)
}

func parseSegName(name, ext string) (uint64, error) {
	stem := strings.TrimSuffix(name, ext)
	return strconv.ParseUint(stem, 10, 64)
}
