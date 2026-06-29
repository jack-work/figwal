// Package xwal is a multi-fork wrapper over figwal: one main timeline
// plus related separate timelines, forked as a unit. Each channel is its
// own figwal log; every channel entry carries the main-timeline LT it
// belongs to. Reducible channels (state expressed as patches on a base)
// ride figwal's per-segment watermark headers, so state at any point is
// the nearest watermark folded with the patches after it.
package xwal

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

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

// ReduceFunc applies one patch to a state, returning the new state. Used
// both to fold watermarks on rotation/fork and to answer StateAt.
type ReduceFunc func(state, patch []byte) ([]byte, error)

// Reducer is a reducible channel's fold plus its initial (empty) state.
// Initial must be a valid value for the codec — a JSON object such as
// `{}` under the JSONL codec, since it seeds the very first watermark.
type Reducer struct {
	Reduce  ReduceFunc
	Initial []byte
}

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
	Registry    map[string]Reducer
	Codec       string // "jsonl" (default) | "binary"; persisted in the manifest
	SegmentSize int64
	// Genesis is the main-channel genesis payload written by CreateTrunks
	// (the root trunk's first entry, which every trunk inherits). Lets the
	// caller use its own genesis encoding instead of the default marker.
	// Used only at creation; ignored on open.
	Genesis []byte
	// MintTrunkID, if set, generates trunk ids instead of the default
	// sequential "t<N>" (the Trunks layer retries on collision). Lets a
	// consumer use opaque ids; not persisted directly — the ids land in
	// the .trunk markers.
	MintTrunkID func() string
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
	codec  segment.SegmentCodec
}

type channel struct {
	name    string
	kind    Kind
	rname   string
	dir     string
	log     *disk.Log
	reduce  ReduceFunc
	initial []byte
	fk      map[uint64]uint64 // main-LT -> channel-LT (last wins)
}

// buildFK scans the channel to populate its main-LT -> channel-LT index.
func (ch *channel) buildFK() error {
	ch.fk = map[uint64]uint64{}
	first := ch.log.FirstIndex()
	if first == 0 {
		return nil
	}
	return ch.log.Range(first, func(idx uint64, payload []byte) error {
		m, _, err := decodeFrame(payload)
		if err != nil {
			return err
		}
		ch.fk[m] = idx
		return nil
	})
}

const manifestName = "xwal.json"

type manifest struct {
	Main     string            `json:"main"`
	Codec    string            `json:"codec"`
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
	// Complete any interrupted joint fork before serving a branch, so the
	// triune is never observed half-diverged.
	if plan, pending, perr := readForkPlan(dir); perr != nil {
		return nil, perr
	} else if pending {
		if err := recoverFork(dir, cfg, man, plan); err != nil {
			return nil, fmt.Errorf("xwal: recover interrupted fork: %w", err)
		}
		if err := removeForkPlan(dir); err != nil {
			return nil, err
		}
	}
	codec, err := codecByName(man.Codec)
	if err != nil {
		return nil, err
	}
	x := &XWAL{
		root:   dir,
		branch: append([]string(nil), branch...),
		main:   man.Main,
		chans:  make(map[string]*channel, len(man.Channels)),
		cfg:    cfg,
		codec:  codec,
	}
	for _, mc := range man.Channels {
		ch := &channel{name: mc.Name, rname: mc.Reducer}
		switch mc.Kind {
		case "reducible":
			ch.kind = ChannelReducible
			r, ok := resolveReducer(cfg, mc.Reducer)
			if !ok || r.Reduce == nil {
				return nil, fmt.Errorf("xwal: no reducer %q registered for channel %q", mc.Reducer, mc.Name)
			}
			ch.reduce = r.Reduce
			ch.initial = r.Initial
		default:
			ch.kind = ChannelLog
		}
		opts := disk.Options{Codec: codec, SegmentSize: cfg.SegmentSize}
		if ch.kind == ChannelReducible {
			opts.OnSegmentOpen = reducibleFold(ch.reduce, ch.initial)
		}
		cdir := x.channelDir(mc.Name)
		l, err := disk.Open(cdir, opts)
		if err != nil {
			x.Close()
			return nil, fmt.Errorf("xwal: open channel %q: %w", mc.Name, err)
		}
		ch.dir = cdir
		ch.log = l
		if err := ch.buildFK(); err != nil {
			x.Close()
			return nil, fmt.Errorf("xwal: index channel %q: %w", mc.Name, err)
		}
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

// reducibleFold adapts a Reducer into figwal's OnSegmentOpen: the new
// watermark is the previous one (or the initial state, for the very
// first segment) with every sealed entry's patch folded in. Each sealed
// frame is the channel's JSON envelope; the fold pulls out the patch.
func reducibleFold(reduce ReduceFunc, initial []byte) func(prevHeader []byte, sealed [][]byte) ([]byte, error) {
	return func(prevHeader []byte, sealed [][]byte) ([]byte, error) {
		state := prevHeader
		if len(state) == 0 {
			state = initial
		}
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

func codecByName(name string) (segment.SegmentCodec, error) {
	switch name {
	case "", "jsonl":
		return segment.JSONLCodec{}, nil
	case "binary":
		return segment.BinaryCodec{}, nil
	default:
		return nil, fmt.Errorf("xwal: unknown codec %q", name)
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
	codecName := cfg.Codec
	if codecName == "" {
		codecName = "jsonl"
	}
	if _, err := codecByName(codecName); err != nil {
		return manifest{}, err
	}
	m := manifest{Main: cfg.Main, Codec: codecName}
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
	if err := writeManifest(dir, m); err != nil {
		return manifest{}, err
	}
	return m, nil
}

func writeManifest(dir string, m manifest) error {
	body, _ := json.MarshalIndent(m, "", "  ")
	path := filepath.Join(dir, manifestName)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, body, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// fullChannelDir is the channel dir at this exact branch (no ancestor
// fallback) — used when creating a brand-new channel on this branch.
func (x *XWAL) fullChannelDir(name string) string {
	return filepath.Join(append([]string{x.root, name}, x.branch...)...)
}

func (x *XWAL) channelOpts(ch *channel) disk.Options {
	opts := disk.Options{Codec: x.codec, SegmentSize: x.cfg.SegmentSize}
	if ch.kind == ChannelReducible {
		opts.OnSegmentOpen = reducibleFold(ch.reduce, ch.initial)
	}
	return opts
}

// Clear wipes a channel's own data and reopens it empty, resetting its
// index — for caches that invalidate wholesale (translation fingerprint
// drift). NOTE: on a forked branch this also drops the branch's link to
// its parent for that channel; intended for trunk-level cache resets.
func (x *XWAL) Clear(channelName string) error {
	ch := x.chans[channelName]
	if ch == nil {
		return fmt.Errorf("xwal: no channel %q", channelName)
	}
	if err := ch.log.Close(); err != nil {
		return err
	}
	if err := os.RemoveAll(ch.dir); err != nil {
		return err
	}
	if err := os.MkdirAll(ch.dir, 0o755); err != nil {
		return err
	}
	l, err := disk.Open(ch.dir, x.channelOpts(ch))
	if err != nil {
		return err
	}
	ch.log = l
	ch.fk = map[uint64]uint64{}
	return nil
}

// AddChannel adds a channel to an existing xwal (e.g. a translation stream
// for a newly-seen provider), updating the manifest, then ROOTING and
// BACKFILLING it: the channel is born at the channel root and its node tree
// is mirrored from the main channel's tree so every existing node (stumps +
// trunks) has a matching, empty, correctly-rooted node. Without this a
// lazily-added channel would exist only on the branch it was added from, and
// forks could not propagate it. The handle is then opened at THIS branch.
//
// Backfilled nodes carry no own segments — their content is derivable (a log
// channel is empty; a reducible channel seeds each node with the reducer's
// Initial watermark so StateAt folds from a defined base). Per-channel
// forkBases are recomputed for an empty channel (every fork lands at the
// channel's own first index), never copied from the main channel — the index
// spaces differ. Reducible channels must name a registered reducer.
func (x *XWAL) AddChannel(spec ChannelSpec) error {
	if _, exists := x.chans[spec.Name]; exists {
		return fmt.Errorf("xwal: channel %q already exists", spec.Name)
	}
	man, err := loadOrCreateManifest(x.root, x.cfg)
	if err != nil {
		return err
	}
	if spec.Kind == ChannelReducible && spec.Reducer == "" {
		return fmt.Errorf("xwal: reducible channel %q needs a reducer name", spec.Name)
	}
	man.Channels = append(man.Channels, manifestChannel{
		Name: spec.Name, Kind: spec.Kind.String(), Reducer: spec.Reducer,
	})
	if err := writeManifest(x.root, man); err != nil {
		return err
	}
	ch := &channel{name: spec.Name, kind: spec.Kind, rname: spec.Reducer}
	if spec.Kind == ChannelReducible {
		r, ok := resolveReducer(x.cfg, spec.Reducer)
		if !ok || r.Reduce == nil {
			return fmt.Errorf("xwal: no reducer %q registered for channel %q", spec.Reducer, spec.Name)
		}
		ch.reduce = r.Reduce
		ch.initial = r.Initial
	}
	// Root + backfill the channel's node tree to mirror the main channel.
	if err := x.backfillChannel(ch); err != nil {
		return fmt.Errorf("xwal: backfill channel %q: %w", spec.Name, err)
	}
	// Open the handle for THIS branch (now that the structure exists).
	cdir := x.channelDir(spec.Name)
	l, err := disk.Open(cdir, x.channelOpts(ch))
	if err != nil {
		return err
	}
	ch.dir = cdir
	ch.log = l
	ch.fk = map[uint64]uint64{}
	if err := ch.buildFK(); err != nil {
		l.Close()
		return err
	}
	x.chans[spec.Name] = ch
	x.order = append(x.order, spec.Name)
	return nil
}

// backfillChannel materializes a newly-added channel's node tree to mirror
// the main channel's directory tree: for every main-channel node dir it
// creates the corresponding channel dir, with a .fork marker for non-root
// nodes (the empty-channel fork base — the node's own first index) and, for a
// reducible channel, an Initial-watermark seed segment so StateAt has a base
// to fold from. No payload entries are written (content is derivable).
func (x *XWAL) backfillChannel(ch *channel) error {
	mainBase := filepath.Join(x.root, x.main)
	chBase := filepath.Join(x.root, ch.name)
	var walk func(mainDir, chDir string, depth int) error
	walk = func(mainDir, chDir string, depth int) error {
		if err := os.MkdirAll(chDir, 0o755); err != nil {
			return err
		}
		if depth > 0 {
			// Empty-channel fork base: an all-empty channel forks at its own
			// first index (1) at every level — what the joint-fork boundary
			// computation yields for an empty channel. Reads below it resolve
			// up the (empty) parent chain.
			const base = uint64(1)
			if err := writeBackfillFork(chDir, base, x.codec, ch); err != nil {
				return err
			}
		} else if ch.kind == ChannelReducible {
			// Seed the channel root with the Initial watermark too.
			if err := seedWatermark(chDir, 1, x.codec, ch.initial); err != nil {
				return err
			}
		}
		ents, err := os.ReadDir(mainDir)
		if err != nil {
			return err
		}
		for _, e := range ents {
			if e.IsDir() && !strings.HasPrefix(e.Name(), ".") {
				if err := walk(filepath.Join(mainDir, e.Name()), filepath.Join(chDir, e.Name()), depth+1); err != nil {
					return err
				}
			}
		}
		return nil
	}
	return walk(mainBase, chBase, 0)
}

// writeBackfillFork writes a child node's .fork marker (and, for a reducible
// channel, its Initial-watermark seed segment at base).
func writeBackfillFork(chDir string, base uint64, codec segment.SegmentCodec, ch *channel) error {
	body := fmt.Sprintf("base=%d\n", base)
	tmp := filepath.Join(chDir, ".fork.tmp")
	if err := os.WriteFile(tmp, []byte(body), 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(chDir, ".fork")); err != nil {
		return err
	}
	if ch.kind == ChannelReducible {
		return seedWatermark(chDir, base, codec, ch.initial)
	}
	return nil
}

// seedWatermark writes a header-only segment at baseIndex carrying the
// reducible Initial state, so an empty reducible node folds from a defined
// watermark (mirrors disk.Fork's writeWatermarkSeg).
func seedWatermark(chDir string, baseIndex uint64, codec segment.SegmentCodec, initial []byte) error {
	name := fmt.Sprintf("%020d%s", baseIndex, codec.FileExt())
	seg, err := segment.Create(filepath.Join(chDir, name), codec, baseIndex, 0)
	if err != nil {
		return err
	}
	if err := seg.WriteHeader(initial); err != nil {
		seg.Close()
		return err
	}
	if err := seg.Sync(); err != nil {
		seg.Close()
		return err
	}
	return seg.Close()
}

// AppendMain appends payload (with optional opaque meta) to the main
// channel. The returned mainLT is the channel index it landed at;
// related-channel entries reference it.
func (x *XWAL) AppendMain(payload, meta []byte) (uint64, error) {
	ch := x.chans[x.main]
	next := ch.log.LastIndex() + 1
	if err := ch.log.Write(next, encodeFrame(next, payload, meta)); err != nil {
		return 0, err
	}
	ch.fk[next] = next
	return next, nil
}

// Append appends payload (with optional opaque meta) to a related
// channel, tagged with the main LT it belongs to. mainLT must be >= the
// channel's last referenced main LT (it may exceed the current main tail,
// to support catch-up). The returned value is the channel's own LT.
func (x *XWAL) Append(channelName string, mainLT uint64, payload, meta []byte) (uint64, error) {
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
	if err := ch.log.Write(next, encodeFrame(mainLT, payload, meta)); err != nil {
		return 0, err
	}
	ch.fk[mainLT] = next
	return next, nil
}

// Read returns the (mainLT, payload) at channelLT — the meta-free view.
func (x *XWAL) Read(channelName string, channelLT uint64) (uint64, []byte, error) {
	r, err := x.ReadAt(channelName, channelLT)
	if err != nil {
		return 0, nil, err
	}
	return r.MainLT, r.Payload, nil
}

// ReadAt returns the full record (incl. meta) at channelLT in a channel.
func (x *XWAL) ReadAt(channelName string, channelLT uint64) (Record, error) {
	ch := x.chans[channelName]
	if ch == nil {
		return Record{}, fmt.Errorf("xwal: no channel %q", channelName)
	}
	f, err := ch.log.Read(channelLT)
	if err != nil {
		return Record{}, err
	}
	return decodeRecord(channelLT, f)
}

// Lookup finds the entry referencing a given main LT in a channel (the
// foreign-key view; last entry wins if several share the main LT).
func (x *XWAL) Lookup(channelName string, mainLT uint64) (Record, bool, error) {
	ch := x.chans[channelName]
	if ch == nil {
		return Record{}, false, fmt.Errorf("xwal: no channel %q", channelName)
	}
	lt, ok := ch.fk[mainLT]
	if !ok {
		return Record{}, false, nil
	}
	r, err := x.ReadAt(channelName, lt)
	if err != nil {
		return Record{}, false, err
	}
	return r, true, nil
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

// A channel entry is stored as a JSON object so it round-trips through
// either codec (the JSONL codec needs JSON objects; the binary codec
// stores the bytes opaquely): {"m":<mainLT>,"p":<payload>,"x":<meta>}.
// Payload (and optional opaque meta) are embedded raw when themselves
// valid JSON, else as a JSON string. Meta is a free side-channel for the
// caller (e.g. a cache fingerprint). Reducible watermarks are stored as
// the bare state object, not wrapped.
type frameObj struct {
	M uint64          `json:"m"`
	P json.RawMessage `json:"p"`
	X json.RawMessage `json:"x,omitempty"`
}

// Record is a decoded channel entry.
type Record struct {
	ChannelLT uint64
	MainLT    uint64
	Payload   []byte
	Meta      []byte
}

func embedJSON(b []byte) json.RawMessage {
	if len(b) == 0 {
		return nil
	}
	if json.Valid(b) {
		return json.RawMessage(b)
	}
	q, _ := json.Marshal(string(b))
	return json.RawMessage(q)
}

func encodeFrame(mainLT uint64, payload, meta []byte) []byte {
	o := frameObj{M: mainLT, P: embedJSON(payload)}
	if len(o.P) == 0 {
		o.P = json.RawMessage("null")
	}
	o.X = embedJSON(meta)
	b, _ := json.Marshal(o)
	return b
}

// decodeFrame returns the main-LT and payload, ignoring meta. Used by the
// fold and fork-boundary paths that don't care about meta.
func decodeFrame(f []byte) (uint64, []byte, error) {
	var o frameObj
	if err := json.Unmarshal(f, &o); err != nil {
		return 0, nil, fmt.Errorf("xwal: decode frame: %w", err)
	}
	return o.M, o.P, nil
}

func decodeRecord(channelLT uint64, f []byte) (Record, error) {
	var o frameObj
	if err := json.Unmarshal(f, &o); err != nil {
		return Record{}, fmt.Errorf("xwal: decode frame: %w", err)
	}
	return Record{ChannelLT: channelLT, MainLT: o.M, Payload: o.P, Meta: o.X}, nil
}
