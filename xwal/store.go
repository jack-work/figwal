package xwal

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

type StoreOptions struct {
	Main          string
	FlushInterval time.Duration
	Reducers      map[string]Reducer
	Opaque        []string
	Codec         string
	SegmentSize   int64
	Genesis       []byte
	MintTrunkID   func() string
}

type Store struct {
	*Trunks
	opts     StoreOptions
	lockFile *os.File

	closeOnce sync.Once
	closeErr  error
}

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
	return &Store{Trunks: t, opts: opts, lockFile: lockFile}, nil
}

func (o StoreOptions) config() Config {
	cfg := Config{
		Main:        o.Main,
		Registry:    o.Reducers,
		Codec:       o.Codec,
		SegmentSize: o.SegmentSize,
		Genesis:     o.Genesis,
		MintTrunkID: o.MintTrunkID,
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
		return lt, err
	}
	return s.Trunks.AppendChannel(trunk, channel, mainLT, payload, meta)
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
		s.closeErr = s.Trunks.Close()
		if err := unlockRoot(s.lockFile); err != nil && s.closeErr == nil {
			s.closeErr = err
		}
	})
	return s.closeErr
}
