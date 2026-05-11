package log

import (
	"errors"
	"figwal/segment"
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
}

type Log struct {
	mu     sync.RWMutex
	dir    string
	opts   Options
	codec  segment.SegmentCodec
	ext    string
	sealed []*segment.Segment
	active *segment.Segment
}

func Open(dir string, opts Options) (*Log, error) {
	if opts.SegmentSize == 0 {
		opts.SegmentSize = defaultSegSize
	}
	if opts.Codec == nil {
		opts.Codec = segment.BinaryCodec{}
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	l := &Log{
		dir:   dir,
		opts:  opts,
		codec: opts.Codec,
		ext:   opts.Codec.FileExt(),
	}
	if err := l.loadSegments(); err != nil {
		return nil, err
	}
	slog.Info("log opened",
		"dir", dir,
		"codec", l.codec.Name(),
		"segmentSize", opts.SegmentSize,
		"sealed", len(l.sealed),
		"hasActive", l.active != nil)
	return l, nil
}

func (l *Log) Write(idx uint64, payload []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	expected := l.lastIndexLocked() + 1
	if l.isEmptyLocked() {
		expected = 1
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
// FirstIndex.
func (l *Log) Range(from uint64, fn func(idx uint64, payload []byte) error) error {
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
	if l.isEmptyLocked() {
		return nil, ErrEmpty
	}
	s := l.findSegmentLocked(idx)
	if s == nil {
		return nil, ErrNotFound
	}
	return s.ReadIndex(idx - s.BaseIndex())
}

func (l *Log) FirstIndex() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.firstIndexLocked()
}

func (l *Log) LastIndex() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.lastIndexLocked()
}

func (l *Log) Hash(idx uint64) (string, error) {
	b, err := l.Read(idx)
	if err != nil {
		return "", err
	}
	return l.codec.Hash(b)
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
