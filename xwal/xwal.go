// Package xwal is a multi-fork wrapper over figwal: one main timeline
// plus related separate timelines, forked as a unit. Each channel is its
// own figwal log; every channel entry carries the main-timeline LT it
// belongs to. Reducible channels (state expressed as patches on a base)
// ride figwal's per-segment watermark headers, so state at any point is
// the nearest watermark folded with the patches after it.
package xwal

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/jack-work/figwal/disk"
	"github.com/jack-work/figwal/segment"
)

// Kind is a channel's storage discipline.
type Kind int

const (
	// ChannelLog is an append-only stream of opaque entries (the main IR
	// timeline, translation streams).
	ChannelLog Kind = iota
	// ChannelReducible is a patch stream over a base state: each segment
	// leads with a watermark and StateAt folds the patches onto it (the
	// chalkboard).
	ChannelReducible
)

func (k Kind) String() string {
	if k == ChannelReducible {
		return "reducible"
	}
	return "log"
}

// ReduceFunc applies one patch to a state, returning the new state. A
// nil/empty state is the initial (empty) value. Used both to fold
// watermarks on rotation/fork and to answer StateAt.
type ReduceFunc func(state, patch []byte) ([]byte, error)

// ChannelSpec declares one channel at creation time.
type ChannelSpec struct {
	Name    string
	Kind    Kind
	Reducer string // registry key; required iff Kind == ChannelReducible
}

// Config opens or creates an xwal. On first open the manifest is written
// from Main+Channels; afterwards the manifest is authoritative and those
// fields are ignored. Registry resolves reducer names to functions on
// every open (functions are not persisted).
type Config struct {
	Main        string
	Channels    []ChannelSpec
	Registry    map[string]ReduceFunc
	SegmentSize int64
}

var errStopRange = errors.New("xwal: stop range")

// XWAL is an opened branch of a multi-channel log.
type XWAL struct {
	root   string // dir holding the manifest and the per-channel trees
	branch []string
	main   string
	order  []string
	chans  map[string]*channel
	cfg    Config
}

type channel struct {
	name   string
	kind   Kind
	rname  string
	log    *disk.Log
	reduce ReduceFunc
}

const manifestName = "xwal.json"

type manifest struct {
	Main     string            `json:"main"`
	Channels []manifestChannel `json:"channels"`
}

type manifestChannel struct {
	Name    string `json:"name"`
	Kind    string `json:"kind"`
	Reducer string `json:"reducer,omitempty"`
}

// Open opens (creating if absent) the xwal rooted at dir. branch selects
// a forked sub-branch by its chain of fork names (empty = the trunk).
func Open(dir string, cfg Config, branch ...string) (*XWAL, error) {
	if dir == "" {
		return nil, fmt.Errorf("xwal: empty dir")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	man, err := loadOrCreateManifest(dir, cfg)
	if err != nil {
		return nil, err
	}
	x := &XWAL{
		root:   dir,
		branch: append([]string(nil), branch...),
		main:   man.Main,
		chans:  make(map[string]*channel, len(man.Channels)),
		cfg:    cfg,
	}
	for _, mc := range man.Channels {
		ch := &channel{name: mc.Name, rname: mc.Reducer}
		switch mc.Kind {
		case "reducible":
			ch.kind = ChannelReducible
			fn := cfg.Registry[mc.Reducer]
			if fn == nil {
				return nil, fmt.Errorf("xwal: no reducer %q registered for channel %q", mc.Reducer, mc.Name)
			}
			ch.reduce = fn
		default:
			ch.kind = ChannelLog
		}
		opts := disk.Options{Codec: segment.BinaryCodec{}, SegmentSize: cfg.SegmentSize}
		if ch.kind == ChannelReducible {
			opts.OnSegmentOpen = reducibleFold(ch.reduce)
		}
		l, err := disk.Open(x.channelDir(mc.Name), opts)
		if err != nil {
			x.Close()
			return nil, fmt.Errorf("xwal: open channel %q: %w", mc.Name, err)
		}
		ch.log = l
		x.chans[mc.Name] = ch
		x.order = append(x.order, mc.Name)
	}
	return x, nil
}

// channelDir resolves a channel's directory for this branch, falling
// back to the deepest existing ancestor. A branch component absent for a
// given channel — e.g. an old-future that was a tail fork, so no subdir
// was created — resolves to the parent that actually holds the content.
func (x *XWAL) channelDir(name string) string {
	base := filepath.Join(x.root, name)
	dir := base
	for i := 1; i <= len(x.branch); i++ {
		cand := filepath.Join(append([]string{base}, x.branch[:i]...)...)
		if pathExists(cand) {
			dir = cand
		} else {
			break
		}
	}
	return dir
}

func pathExists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}

// reducibleFold adapts a ReduceFunc into figwal's OnSegmentOpen: the new
// watermark is the previous one with every sealed entry's patch folded
// in. Entries carry an 8-byte main-LT prefix that the fold strips.
func reducibleFold(reduce ReduceFunc) func(prevHeader []byte, sealed [][]byte) ([]byte, error) {
	return func(prevHeader []byte, sealed [][]byte) ([]byte, error) {
		state := prevHeader
		for _, f := range sealed {
			_, patch, err := decodeFrame(f)
			if err != nil {
				return nil, err
			}
			state, err = reduce(state, patch)
			if err != nil {
				return nil, err
			}
		}
		return state, nil
	}
}

func loadOrCreateManifest(dir string, cfg Config) (manifest, error) {
	path := filepath.Join(dir, manifestName)
	data, err := os.ReadFile(path)
	if err == nil {
		var m manifest
		if jerr := json.Unmarshal(data, &m); jerr != nil {
			return manifest{}, fmt.Errorf("xwal: parse manifest: %w", jerr)
		}
		return m, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return manifest{}, err
	}
	// Create from cfg.
	if cfg.Main == "" || len(cfg.Channels) == 0 {
		return manifest{}, fmt.Errorf("xwal: no manifest at %s and no Config to create one", dir)
	}
	m := manifest{Main: cfg.Main}
	seenMain := false
	for _, c := range cfg.Channels {
		if c.Name == cfg.Main {
			seenMain = true
		}
		if c.Kind == ChannelReducible && c.Reducer == "" {
			return manifest{}, fmt.Errorf("xwal: reducible channel %q needs a reducer name", c.Name)
		}
		m.Channels = append(m.Channels, manifestChannel{Name: c.Name, Kind: c.Kind.String(), Reducer: c.Reducer})
	}
	if !seenMain {
		return manifest{}, fmt.Errorf("xwal: main channel %q not in Channels", cfg.Main)
	}
	body, _ := json.MarshalIndent(m, "", "  ")
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, body, 0o644); err != nil {
		return manifest{}, err
	}
	if err := os.Rename(tmp, path); err != nil {
		return manifest{}, err
	}
	return m, nil
}

// AppendMain appends payload to the main channel. The returned mainLT is
// the channel index it landed at; related-channel entries reference it.
func (x *XWAL) AppendMain(payload []byte) (uint64, error) {
	ch := x.chans[x.main]
	next := ch.log.LastIndex() + 1
	if err := ch.log.Write(next, encodeFrame(next, payload)); err != nil {
		return 0, err
	}
	return next, nil
}

// Append appends payload to a related channel, tagged with the main LT
// it belongs to. mainLT must be >= the channel's last referenced main LT
// (it may exceed the current main tail, to support catch-up). The
// returned value is the channel's own LT.
func (x *XWAL) Append(channelName string, mainLT uint64, payload []byte) (uint64, error) {
	ch := x.chans[channelName]
	if ch == nil {
		return 0, fmt.Errorf("xwal: no channel %q", channelName)
	}
	if channelName == x.main {
		return 0, fmt.Errorf("xwal: use AppendMain for the main channel")
	}
	if lastMain, ok, err := x.tailMain(ch); err != nil {
		return 0, err
	} else if ok && mainLT < lastMain {
		return 0, fmt.Errorf("xwal: channel %q main-LT must be non-decreasing: got %d, last %d",
			channelName, mainLT, lastMain)
	}
	next := ch.log.LastIndex() + 1
	if err := ch.log.Write(next, encodeFrame(mainLT, payload)); err != nil {
		return 0, err
	}
	return next, nil
}

// Read returns the (mainLT, payload) at channelLT in a channel.
func (x *XWAL) Read(channelName string, channelLT uint64) (uint64, []byte, error) {
	ch := x.chans[channelName]
	if ch == nil {
		return 0, nil, fmt.Errorf("xwal: no channel %q", channelName)
	}
	f, err := ch.log.Read(channelLT)
	if err != nil {
		return 0, nil, err
	}
	return decodeFrame(f)
}

// StateAt folds a reducible channel to channelLT (watermark + patches).
func (x *XWAL) StateAt(channelName string, channelLT uint64) ([]byte, error) {
	ch := x.chans[channelName]
	if ch == nil {
		return nil, fmt.Errorf("xwal: no channel %q", channelName)
	}
	if ch.kind != ChannelReducible {
		return nil, fmt.Errorf("xwal: channel %q is not reducible", channelName)
	}
	return ch.log.StateAt(channelLT)
}

// tailMain returns the main LT of the channel's last entry.
func (x *XWAL) tailMain(ch *channel) (uint64, bool, error) {
	last := ch.log.LastIndex()
	first := ch.log.FirstIndex()
	if first == 0 || last < first {
		return 0, false, nil
	}
	f, err := ch.log.Read(last)
	if err != nil {
		return 0, false, err
	}
	m, _, err := decodeFrame(f)
	return m, true, err
}

// ChannelInfo is a read-only snapshot of a channel's bounds.
type ChannelInfo struct {
	Name     string
	Kind     Kind
	Reducer  string
	First    uint64
	Last     uint64
	Segments int
}

// Channels reports each channel's current bounds, in declared order.
func (x *XWAL) Channels() []ChannelInfo {
	out := make([]ChannelInfo, 0, len(x.order))
	for _, name := range x.order {
		ch := x.chans[name]
		out = append(out, ChannelInfo{
			Name:     name,
			Kind:     ch.kind,
			Reducer:  ch.rname,
			First:    ch.log.FirstIndex(),
			Last:     ch.log.LastIndex(),
			Segments: len(ch.log.SegmentBaseIndexes()),
		})
	}
	return out
}

// Main returns the main channel name.
func (x *XWAL) Main() string { return x.main }

// Branch returns this branch's fork chain (empty for the trunk).
func (x *XWAL) Branch() []string { return append([]string(nil), x.branch...) }

// Close closes every channel.
func (x *XWAL) Close() error {
	var first error
	for _, ch := range x.chans {
		if ch.log != nil {
			if err := ch.log.Close(); err != nil && first == nil {
				first = err
			}
		}
	}
	return first
}

func encodeFrame(mainLT uint64, payload []byte) []byte {
	b := make([]byte, 8+len(payload))
	binary.BigEndian.PutUint64(b[:8], mainLT)
	copy(b[8:], payload)
	return b
}

func decodeFrame(f []byte) (uint64, []byte, error) {
	if len(f) < 8 {
		return 0, nil, fmt.Errorf("xwal: short frame (%d bytes)", len(f))
	}
	return binary.BigEndian.Uint64(f[:8]), f[8:], nil
}
