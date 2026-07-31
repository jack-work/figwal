package xwal

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

type StoreOptions struct {
	Main              string
	FlushInterval     time.Duration
	MaxUnflushedBytes int64
	// IdleUnload evicts a lineage's in-RAM head after this much time
	// without an append or read; 0 = default 5m, negative = never.
	IdleUnload  time.Duration
	Reducers    map[string]Reducer
	Opaque      []string
	Codec       string
	SegmentSize int64
	Genesis     []byte
	MintTrunkID func() string
}

type Store struct {
	*Trunks
	opts     StoreOptions
	lockFile *os.File

	mu           sync.Mutex
	dirty        map[string]struct{}
	touch        map[string]time.Time
	lineageFails map[string]int
	lineageErr   map[string]error
	kick         chan struct{}
	stop         chan struct{}
	done         chan struct{}

	closeOnce sync.Once
	closeErr  error
}

const (
	defaultFlushInterval = time.Second
	defaultIdleUnload    = 5 * time.Minute
	storePoisonThreshold = 3
)

func OpenStore(root string, opts StoreOptions) (*Store, error) {
	if root == "" {
		return nil, fmt.Errorf("xwal: empty root")
	}
	if opts.Main == "" {
		opts.Main = "main"
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, err
	}
	lockFile, err := lockRoot(root)
	if err != nil {
		return nil, err
	}
	cfg := opts.config()
	wasUnclean := pathExists(uncleanPath(root))
	var t *Trunks
	if pathExists(filepath.Join(root, manifestName)) {
		t, err = openTrunks(root, cfg)
	} else {
		t, err = createTrunks(root, cfg)
	}
	if err != nil {
		unlockRoot(lockFile)
		return nil, err
	}
	if wasUnclean {
		if err := repairCoherentCuts(t); err != nil {
			t.Close()
			unlockRoot(lockFile)
			return nil, err
		}
	}
	if err := markUnclean(root); err != nil {
		t.Close()
		unlockRoot(lockFile)
		return nil, err
	}
	if opts.FlushInterval <= 0 {
		opts.FlushInterval = defaultFlushInterval
	}
	if opts.IdleUnload == 0 {
		opts.IdleUnload = defaultIdleUnload
	}
	s := &Store{
		Trunks:       t,
		opts:         opts,
		lockFile:     lockFile,
		dirty:        map[string]struct{}{},
		touch:        map[string]time.Time{},
		lineageFails: map[string]int{},
		lineageErr:   map[string]error{},
		kick:         make(chan struct{}, 1),
		stop:         make(chan struct{}),
		done:         make(chan struct{}),
	}
	go s.run()
	return s, nil
}

func (s *Store) run() {
	defer close(s.done)
	ticker := time.NewTicker(s.opts.FlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-ticker.C:
		case <-s.kick:
		}
		s.flushDirty()
		s.evictIdle()
	}
}

// Kick schedules an immediate asynchronous flush of dirty lineages.
func (s *Store) Kick() {
	select {
	case s.kick <- struct{}{}:
	default:
	}
}

func (s *Store) markDirty(trunk string) {
	now := time.Now()
	s.mu.Lock()
	s.dirty[trunk] = struct{}{}
	s.touch[trunk] = now
	s.mu.Unlock()
}

func (s *Store) markTouched(trunk string) {
	now := time.Now()
	s.mu.Lock()
	s.touch[trunk] = now
	s.mu.Unlock()
}

func (s *Store) noteFlushFailure(trunk string, err error) {
	s.mu.Lock()
	s.lineageFails[trunk]++
	s.lineageErr[trunk] = err
	s.mu.Unlock()
}

func (s *Store) noteFlushSuccess(trunk string) {
	s.mu.Lock()
	delete(s.lineageFails, trunk)
	delete(s.lineageErr, trunk)
	s.mu.Unlock()
}

func (s *Store) poisoned(trunk string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if n := s.lineageFails[trunk]; n >= storePoisonThreshold {
		return fmt.Errorf("xwal: lineage %s flushes failing (%d consecutive): %w", trunk, n, s.lineageErr[trunk])
	}
	return nil
}

// purgeLineage drops all flusher bookkeeping for trunk ids that ceased
// to exist (Remove) or were relabeled away (Promote).
func (s *Store) purgeLineage(ids ...string) {
	s.mu.Lock()
	for _, id := range ids {
		delete(s.dirty, id)
		delete(s.touch, id)
		delete(s.lineageFails, id)
		delete(s.lineageErr, id)
	}
	s.mu.Unlock()
}

func (s *Store) purgeVanished() {
	s.mu.Lock()
	var stale []string
	for id := range s.dirty {
		stale = append(stale, id)
	}
	for id := range s.touch {
		stale = append(stale, id)
	}
	for id := range s.lineageFails {
		stale = append(stale, id)
	}
	s.mu.Unlock()
	for _, id := range stale {
		if !s.Trunks.hasHead(id) {
			s.purgeLineage(id)
		}
	}
}

// Remove deletes a trunk (and, recursively, its branches) and clears
// their flusher bookkeeping so a dirty-at-removal lineage cannot poison
// the store. Unflushed appends on the removed trunks are flushed first
// (best effort; they are deleted either way).
func (s *Store) Remove(trunk string, recursive bool) error {
	s.flushDirty()
	removed, err := s.Trunks.remove(trunk, recursive)
	if len(removed) > 0 {
		s.purgeLineage(removed...)
	}
	return err
}

// Promote relabels trunk markers; the absorbed parent id may cease to
// exist, so its bookkeeping is drained first and purged after.
func (s *Store) Promote(trunk TrunkID, levels int) (int, error) {
	s.flushDirty()
	n, err := s.Trunks.Promote(trunk, levels)
	s.purgeVanished()
	return n, err
}

// Clear wipes a channel's own data for a trunk's lineage and reseeds it
// empty, atomically with the flusher: pending buffers for the channel
// are drained and every hot handle is retired under the topology lock
// before the wipe, so an in-flight flush can never resurrect wiped
// records. Intended for trunk-level cache resets (translation caches).
func (s *Store) Clear(trunk, channel string) error {
	endMutation, err := s.Trunks.beginTopologyMutation()
	if err != nil {
		return err
	}
	defer endMutation()
	if err := s.Trunks.ensureNoOpenHeads(); err != nil {
		return err
	}
	s.Trunks.retireRootHotPreservingValidation()
	branch, err := s.Trunks.headBranch(trunk)
	if err != nil {
		return err
	}
	x, err := Open(s.Trunks.root, s.Trunks.cfg, branch...)
	if err != nil {
		return err
	}
	clearErr := x.Clear(channel)
	if cerr := x.Close(); clearErr == nil {
		clearErr = cerr
	}
	return clearErr
}

// FlushStump synchronously persists a stump's birth records, lineage-
// coherently. Raw StumpHead writes are invisible to the flusher; call
// this before spawning children under a freshly written stump.
func (s *Store) FlushStump(name string) error {
	sx, err := s.Trunks.StumpHead(name)
	if err != nil {
		return err
	}
	defer sx.Close()
	return sx.flushCoherent()
}

func (s *Store) flushDirty() {
	s.mu.Lock()
	trunks := make([]string, 0, len(s.dirty))
	for tr := range s.dirty {
		trunks = append(trunks, tr)
	}
	s.dirty = map[string]struct{}{}
	s.mu.Unlock()
	sort.Strings(trunks)
	for _, tr := range trunks {
		err := s.flushLineage(tr)
		switch {
		case err == nil:
			s.noteFlushSuccess(tr)
		case errors.Is(err, ErrUnknownTrunk):
			slog.Info("xwal: dropping flush bookkeeping for vanished trunk", "trunk", tr)
			s.purgeLineage(tr)
		default:
			slog.Warn("xwal: lineage flush failed", "trunk", tr, "err", err)
			s.markDirty(tr)
			s.noteFlushFailure(tr, err)
		}
	}
	if err := s.Trunks.flushHot(); err != nil {
		slog.Warn("xwal: stray flush failed", "err", err)
	}
}

func (s *Store) evictIdle() {
	if s.opts.IdleUnload < 0 {
		return
	}
	now := time.Now()
	s.mu.Lock()
	var idle []string
	for tr, at := range s.touch {
		if _, isDirty := s.dirty[tr]; isDirty {
			continue
		}
		if now.Sub(at) >= s.opts.IdleUnload {
			idle = append(idle, tr)
		}
	}
	s.mu.Unlock()
	sort.Strings(idle)
	for _, tr := range idle {
		evicted, err := s.Trunks.evictLineage(tr)
		if err != nil {
			slog.Warn("xwal: lineage evict failed", "trunk", tr, "err", err)
			continue
		}
		if evicted {
			s.mu.Lock()
			delete(s.touch, tr)
			s.mu.Unlock()
		}
	}
}

// LoadedHeads reports how many lineage heads are currently resident in
// memory (loaded hot snapshots).
func (s *Store) LoadedHeads() int {
	t := s.Trunks
	t.hotMu.Lock()
	defer t.hotMu.Unlock()
	n := 0
	if t.hot != nil {
		n += len(t.hot.heads)
	}
	for h := range t.retired {
		n += len(h.heads)
	}
	return n
}

func (s *Store) flushLineage(trunk string) error {
	x, err := s.Trunks.Head(trunk)
	if err != nil {
		return err
	}
	defer x.Close()
	return x.flushCoherent()
}

func (o StoreOptions) config() Config {
	cfg := Config{
		Main:              o.Main,
		Registry:          o.Reducers,
		Codec:             o.Codec,
		SegmentSize:       o.SegmentSize,
		MaxUnflushedBytes: o.MaxUnflushedBytes,
		Genesis:           o.Genesis,
		MintTrunkID:       o.MintTrunkID,
	}
	opaque := make(map[string]bool, len(o.Opaque))
	for _, name := range o.Opaque {
		opaque[name] = true
	}
	seen := map[string]bool{o.Main: true}
	cfg.Channels = append(cfg.Channels, ChannelSpec{Name: o.Main, Opaque: opaque[o.Main]})
	reducible := make([]string, 0, len(o.Reducers))
	for name := range o.Reducers {
		reducible = append(reducible, name)
	}
	sort.Strings(reducible)
	for _, name := range reducible {
		if seen[name] {
			continue
		}
		seen[name] = true
		cfg.Channels = append(cfg.Channels, ChannelSpec{
			Name: name, Kind: ChannelReducible, Reducer: name, Opaque: opaque[name],
		})
	}
	for _, name := range o.Opaque {
		if seen[name] {
			continue
		}
		seen[name] = true
		cfg.Channels = append(cfg.Channels, ChannelSpec{Name: name, Opaque: true})
	}
	return cfg
}

func (s *Store) Append(trunk, channel string, mainLT uint64, payload, meta []byte) (uint64, error) {
	if err := s.poisoned(trunk); err != nil {
		return 0, err
	}
	if channel == s.Trunks.main {
		_, lt, err := s.Trunks.Append(trunk, 0, payload, meta)
		if err != nil {
			return 0, err
		}
		s.markDirty(trunk)
		return lt, nil
	}
	lt, err := s.Trunks.AppendChannel(trunk, channel, mainLT, payload, meta)
	if errors.Is(err, ErrNoChannel) {
		if cerr := s.autoCreateChannel(channel); cerr != nil {
			return 0, cerr
		}
		lt, err = s.Trunks.AppendChannel(trunk, channel, mainLT, payload, meta)
	}
	if err != nil {
		return 0, err
	}
	s.markDirty(trunk)
	return lt, nil
}

func (s *Store) autoCreateChannel(channel string) error {
	spec := ChannelSpec{Name: channel, Kind: ChannelLog}
	for _, name := range s.opts.Opaque {
		if name == channel {
			spec.Opaque = true
		}
	}
	if _, ok := s.opts.Reducers[channel]; ok {
		spec.Kind = ChannelReducible
		spec.Reducer = channel
	}
	return s.Trunks.ensureChannel(spec)
}

func (s *Store) Fork(trunk string, atMainLT uint64) (string, error) {
	return s.Trunks.ForkAt(trunk, atMainLT)
}

func (s *Store) Read(trunk, channel string, channelLT uint64) (uint64, []byte, error) {
	s.markTouched(trunk)
	x, err := s.Trunks.Head(trunk)
	if err != nil {
		return 0, nil, err
	}
	defer x.Close()
	return x.Read(channel, channelLT)
}

func (s *Store) ReadAt(trunk, channel string, channelLT uint64) (Record, error) {
	s.markTouched(trunk)
	x, err := s.Trunks.Head(trunk)
	if err != nil {
		return Record{}, err
	}
	defer x.Close()
	return x.ReadAt(channel, channelLT)
}

func (s *Store) Lookup(trunk, channel string, mainLT uint64) (Record, bool, error) {
	s.markTouched(trunk)
	x, err := s.Trunks.Head(trunk)
	if err != nil {
		return Record{}, false, err
	}
	defer x.Close()
	return x.Lookup(channel, mainLT)
}

func (s *Store) StateAt(trunk, channel string, channelLT uint64) ([]byte, error) {
	s.markTouched(trunk)
	x, err := s.Trunks.Head(trunk)
	if err != nil {
		return nil, err
	}
	defer x.Close()
	return x.StateAt(channel, channelLT)
}

func (s *Store) RecordsFrom(trunk, channel string, fromMainLT uint64, limit int) ([]Record, error) {
	s.markTouched(trunk)
	x, err := s.Trunks.Head(trunk)
	if err != nil {
		return nil, err
	}
	defer x.Close()
	return x.RecordsFrom(channel, fromMainLT, limit)
}

func (s *Store) Channels(trunk string) ([]ChannelInfo, error) {
	s.markTouched(trunk)
	x, err := s.Trunks.Head(trunk)
	if err != nil {
		return nil, err
	}
	defer x.Close()
	return x.Channels(), nil
}

func (s *Store) Close() error {
	s.closeOnce.Do(func() {
		close(s.stop)
		<-s.done
		s.flushDirty()
		s.closeErr = s.Trunks.Close()
		if s.closeErr == nil {
			s.closeErr = clearUnclean(s.Trunks.root)
		}
		if err := unlockRoot(s.lockFile); err != nil && s.closeErr == nil {
			s.closeErr = err
		}
	})
	return s.closeErr
}
