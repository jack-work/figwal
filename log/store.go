package log

import (
	"errors"
	"path/filepath"
	"sync"

	"github.com/jack-work/figwal/disk"
)

// ErrStoreExplicitParent reports an unsupported attempt to combine the
// shared store with a caller-owned disk parent. The Store cannot safely
// deduplicate or track the lifetime of that parent.
var ErrStoreExplicitParent = errors.New("log store does not support Options.Parent")

// Store owns shared log handles for one physical directory tree. It wraps a
// disk.Store and creates exactly one immutable cache snapshot per physical
// log. Fork children point at the same parent snapshot rather than copying
// their inherited history.
//
// A Store owns all returned logs. Close the Store, not the individual logs.
// Writes are safe through the shared handles; filesystem topology mutations
// must retire the Store generation and reopen a private Log first.
type Store struct {
	mu      sync.Mutex
	disk    *disk.Store
	logs    map[string]*Log
	opening map[string]*storeOpen
	closed  bool
}

type storeOpen struct {
	ready chan struct{}
	log   *Log
	err   error
}

// NewStore creates an empty shared log store.
func NewStore() *Store {
	return &Store{
		disk:    disk.NewStore(),
		logs:    make(map[string]*Log),
		opening: make(map[string]*storeOpen),
	}
}

// Open returns the single shared cached Log for dir. Concurrent opens of the
// same log wait for one cache construction; sibling opens share the same
// parent snapshot pointer.
func (s *Store) Open(dir string, opts Options) (*Log, error) {
	if opts.Parent != nil {
		return nil, ErrStoreExplicitParent
	}
	abs, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	abs = filepath.Clean(abs)

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, errors.New("log store is closed")
	}
	if l := s.logs[abs]; l != nil {
		s.mu.Unlock()
		return l, nil
	}
	if pending := s.opening[abs]; pending != nil {
		s.mu.Unlock()
		<-pending.ready
		return pending.log, pending.err
	}
	pending := &storeOpen{ready: make(chan struct{})}
	s.opening[abs] = pending
	s.mu.Unlock()

	l, openErr := s.open(abs, opts)

	s.mu.Lock()
	if s.closed && openErr == nil {
		openErr = errors.New("log store is closed")
		l = nil
	}
	if openErr == nil {
		s.logs[abs] = l
	}
	pending.log, pending.err = l, openErr
	delete(s.opening, abs)
	close(pending.ready)
	s.mu.Unlock()
	return l, openErr
}

func (s *Store) open(abs string, opts Options) (*Log, error) {
	inner, err := s.disk.Open(abs, opts)
	if err != nil {
		return nil, err
	}

	var parent *cacheSnapshot
	if inner.Parent() != nil {
		parentOpts := opts
		parentOpts.Parent = nil
		parentLog, err := s.Open(filepath.Dir(abs), parentOpts)
		if err != nil {
			return nil, err
		}
		parent = parentLog.snap.Load()
	}
	snap, err := buildOwnSnapshot(inner, parent)
	if err != nil {
		return nil, err
	}
	l := &Log{inner: inner, shared: true, maxLag: maxLagFor(opts)}
	l.snap.Store(snap)
	return l, nil
}

// FlushAll flushes every cached log's buffered entries to disk.
func (s *Store) FlushAll() error {
	s.mu.Lock()
	logs := make([]*Log, 0, len(s.logs))
	for _, l := range s.logs {
		logs = append(logs, l)
	}
	s.mu.Unlock()
	var errs []error
	for _, l := range logs {
		if err := l.Flush(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// Close flushes buffered entries and closes every disk log after all
// in-flight cache constructions finish. The Store cannot be used again.
func (s *Store) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	for len(s.opening) > 0 {
		var ready <-chan struct{}
		for _, pending := range s.opening {
			ready = pending.ready
			break
		}
		s.mu.Unlock()
		<-ready
		s.mu.Lock()
	}
	logs := make([]*Log, 0, len(s.logs))
	for _, l := range s.logs {
		logs = append(logs, l)
	}
	d := s.disk
	s.logs = nil
	s.mu.Unlock()
	var errs []error
	for _, l := range logs {
		if err := l.Flush(); err != nil {
			errs = append(errs, err)
		}
	}
	if err := d.Close(); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}
