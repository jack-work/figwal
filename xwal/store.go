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
	Reducers          map[string]Reducer
	Opaque            []string
	Codec             string
	SegmentSize       int64
	Genesis           []byte
	MintTrunkID       func() string
}

type Store struct {
	*Trunks
	opts     StoreOptions
	lockFile *os.File

	mu    sync.Mutex
	dirty map[string]struct{}
	kick  chan struct{}
	stop  chan struct{}
	done  chan struct{}

	closeOnce sync.Once
	closeErr  error
}

const defaultFlushInterval = time.Second

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
		t, err = OpenTrunks(root, cfg)
	} else {
		t, err = CreateTrunks(root, cfg)
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
	if err := ensureDeclaredChannels(t, cfg); err != nil {
		t.Close()
		unlockRoot(lockFile)
		return nil, err
	}
	if err := markUnclean(root); err != nil {
		t.Close()
		unlockRoot(lockFile)
		return nil, err
	}
	if opts.FlushInterval <= 0 {
		opts.FlushInterval = defaultFlushInterval
	}
	s := &Store{
		Trunks:   t,
		opts:     opts,
		lockFile: lockFile,
		dirty:    map[string]struct{}{},
		kick:     make(chan struct{}, 1),
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
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
	s.mu.Lock()
	s.dirty[trunk] = struct{}{}
	s.mu.Unlock()
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
		if err := s.flushLineage(tr); err != nil {
			slog.Warn("xwal: lineage flush failed", "trunk", tr, "err", err)
			s.markDirty(tr)
		}
	}
	if err := s.Trunks.flushHot(); err != nil {
		slog.Warn("xwal: stray flush failed", "err", err)
	}
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

func ensureDeclaredChannels(t *Trunks, cfg Config) error {
	names, err := channelNames(t.root)
	if err != nil {
		return err
	}
	existing := make(map[string]bool, len(names))
	for _, name := range names {
		existing[name] = true
	}
	for _, spec := range cfg.Channels {
		if existing[spec.Name] {
			continue
		}
		if err := t.EnsureChannel(spec); err != nil {
			return err
		}
	}
	return nil
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
	return s.Trunks.EnsureChannel(spec)
}

func (s *Store) Fork(trunk string, atMainLT uint64) (string, error) {
	return s.Trunks.ForkAt(trunk, atMainLT)
}

func (s *Store) Read(trunk, channel string, channelLT uint64) (uint64, []byte, error) {
	x, err := s.Trunks.Head(trunk)
	if err != nil {
		return 0, nil, err
	}
	defer x.Close()
	return x.Read(channel, channelLT)
}

func (s *Store) ReadAt(trunk, channel string, channelLT uint64) (Record, error) {
	x, err := s.Trunks.Head(trunk)
	if err != nil {
		return Record{}, err
	}
	defer x.Close()
	return x.ReadAt(channel, channelLT)
}

func (s *Store) Lookup(trunk, channel string, mainLT uint64) (Record, bool, error) {
	x, err := s.Trunks.Head(trunk)
	if err != nil {
		return Record{}, false, err
	}
	defer x.Close()
	return x.Lookup(channel, mainLT)
}

func (s *Store) StateAt(trunk, channel string, channelLT uint64) ([]byte, error) {
	x, err := s.Trunks.Head(trunk)
	if err != nil {
		return nil, err
	}
	defer x.Close()
	return x.StateAt(channel, channelLT)
}

func (s *Store) RecordsFrom(trunk, channel string, fromMainLT uint64, limit int) ([]Record, error) {
	x, err := s.Trunks.Head(trunk)
	if err != nil {
		return nil, err
	}
	defer x.Close()
	return x.RecordsFrom(channel, fromMainLT, limit)
}

func (s *Store) Channels(trunk string) ([]ChannelInfo, error) {
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
