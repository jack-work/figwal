// Package xwal is a multi-fork wrapper over figwal: one main timeline
// plus related separate timelines, forked as a unit. Each channel is its
// own figwal log; every channel entry carries the main-timeline LT it
// belongs to. Reducible channels (state expressed as patches on a base)
// ride figwal's per-segment watermark headers, so state at any point is
// the nearest watermark folded with the patches after it.
package xwal

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/jack-work/figwal/disk"
	"github.com/jack-work/figwal/log"
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

// ChannelSpec declares one channel's persisted shape.
type ChannelSpec struct {
	Name    string
	Kind    Kind
	Reducer string // registry key; required iff Kind == ChannelReducible
	Opaque  bool   // persist payload bytes without JSON canonicalization
}

// Config opens or creates an xwal. On first open the manifest is written
// from Main+Channels; afterwards the manifest is authoritative for channel
// shape, while Channels still supplies runtime sync policy by name. Registry
// resolves reducer names to functions on every open (functions and sync modes
// are not persisted).
type Config struct {
	Main        string
	Channels    []ChannelSpec
	Registry    map[string]Reducer
	Codec             string // "jsonl" (default) | "binary"; persisted in the manifest
	SegmentSize       int64
	MaxUnflushedBytes int64
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
	shared bool

	closeOnce      sync.Once
	closeErr       error
	release        func() error
	releaseRoot    func()
	releaseLineage func()
	retire         func()
	borrowRoot     string
	borrowOwner    *Trunks
}

type channel struct {
	mu      sync.Mutex
	name    string
	kind    Kind
	rname   string
	dir     string
	log     *log.Log
	reduce  ReduceFunc
	initial []byte
	opaque  bool
	fk      map[uint64]uint64 // main-LT -> channel-LT (last wins)
	fkBuilt bool              // all entries indexed?
	fkNext  uint64            // highest channel-LT not yet indexed
	fkFloor uint64            // lowest main-LT seen in the indexed suffix
	fkScan  bool
}

func (ch *channel) lookup(mainLT uint64) (uint64, bool, error) {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	if lt, ok := ch.fk[mainLT]; ok {
		return lt, true, nil
	}
	if ch.fkBuilt || (ch.fkScan && mainLT >= ch.fkFloor) {
		return 0, false, nil
	}
	if !ch.fkScan {
		ch.fkNext = ch.log.LastIndex()
		ch.fkScan = true
	}
	stopped := false
	err := ch.log.ScanFromEnd(ch.fkNext, func(idx uint64, payload []byte) error {
		m, err := decodeMainLT(payload)
		if err != nil {
			return err
		}
		if _, exists := ch.fk[m]; !exists {
			ch.fk[m] = idx
		}
		ch.fkFloor = m
		if idx > 0 {
			ch.fkNext = idx - 1
		}
		if m <= mainLT {
			stopped = true
			return errStopRange
		}
		return nil
	})
	if err != nil && err != errStopRange {
		return 0, false, err
	}
	if !stopped {
		ch.fkBuilt = true
	}
	lt, ok := ch.fk[mainLT]
	return lt, ok, nil
}

const manifestName = "xwal.json"
const channelPendingName = ".xwal-channel-pending"

type manifest struct {
	Main     string            `json:"main"`
	Codec    string            `json:"codec"`
	Channels []manifestChannel `json:"channels"`
}

type manifestChannel struct {
	Name    string `json:"name"`
	Kind    string `json:"kind"`
	Reducer string `json:"reducer,omitempty"`
	Opaque  bool   `json:"opaque,omitempty"`
}

type channelPendingPlan struct {
	Channel manifestChannel `json:"channel"`
}

// Open opens (creating if absent) the xwal rooted at dir. branch selects
// a forked sub-branch by its chain of fork names (empty = the trunk).
func Open(dir string, cfg Config, branch ...string) (*XWAL, error) {
	return open(dir, cfg, nil, branch...)
}

func open(dir string, cfg Config, store *log.Store, branch ...string) (*XWAL, error) {
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
	man, err = recoverChannelPending(dir, cfg, man)
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
		shared: store != nil,
	}
	for _, mc := range man.Channels {
		ch := &channel{name: mc.Name, rname: mc.Reducer, opaque: mc.Opaque}
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
		opts := disk.Options{
			Codec: codec, SegmentSize: cfg.SegmentSize, MaxUnflushedBytes: cfg.MaxUnflushedBytes,
		}
		if ch.kind == ChannelReducible {
			opts.OnSegmentOpen = reducibleFold(ch.reduce, ch.initial)
		}
		cdir := x.channelDir(mc.Name)
		var l *log.Log
		if store == nil {
			l, err = log.Open(cdir, opts)
		} else {
			l, err = store.Open(cdir, opts)
		}
		if err != nil {
			x.Close()
			return nil, fmt.Errorf("xwal: open channel %q: %w", mc.Name, err)
		}
		ch.dir = cdir
		ch.log = l
		// Lookup indexes related channels lazily from the tail. Most opens
		// never need a foreign-key index at all.
		ch.fk = map[uint64]uint64{}
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
			if name != x.main {
				if _, err := readForkBaseFile(filepath.Join(cand, ".fork")); err != nil &&
					!errors.Is(err, os.ErrNotExist) {
					break
				}
			}
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
	if err := recoverAtomicReplacement(path); err != nil {
		return manifest{}, err
	}
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
	seen := map[string]struct{}{}
	for _, c := range cfg.Channels {
		if _, ok := seen[c.Name]; ok {
			return manifest{}, fmt.Errorf("xwal: duplicate channel %q", c.Name)
		}
		if err := validateChannelSpec(dir, cfg, m, c); err != nil {
			return manifest{}, err
		}
		seen[c.Name] = struct{}{}
		if c.Name == cfg.Main {
			seenMain = true
		}
		if c.Kind == ChannelReducible && c.Reducer == "" {
			return manifest{}, fmt.Errorf("xwal: reducible channel %q needs a reducer name", c.Name)
		}
		m.Channels = append(m.Channels, manifestChannel{
			Name: c.Name, Kind: c.Kind.String(), Reducer: c.Reducer, Opaque: c.Opaque,
		})
	}
	if !seenMain {
		return manifest{}, fmt.Errorf("xwal: main channel %q not in Channels", cfg.Main)
	}
	if err := prepareInitialChannels(dir, cfg, m); err != nil {
		return manifest{}, err
	}
	if err := writeManifest(dir, m); err != nil {
		return manifest{}, err
	}
	return m, nil
}

func prepareInitialChannels(dir string, cfg Config, m manifest) error {
	codec, err := codecByName(m.Codec)
	if err != nil {
		return err
	}
	for _, mc := range m.Channels {
		chDir := filepath.Join(dir, mc.Name)
		if err := mkdirAllSynced(chDir); err != nil {
			return err
		}
		if mc.Kind != ChannelReducible.String() {
			continue
		}
		reducer, ok := resolveReducer(cfg, mc.Reducer)
		if !ok || reducer.Reduce == nil {
			return fmt.Errorf("xwal: no reducer %q registered for channel %q", mc.Reducer, mc.Name)
		}
		if err := ensureWatermark(chDir, 1, codec, reducer.Initial); err != nil {
			return err
		}
	}
	return nil
}

func validateChannelSpec(root string, cfg Config, man manifest, spec ChannelSpec) error {
	nativeName := filepath.FromSlash(spec.Name)
	if spec.Name == "" || filepath.IsAbs(nativeName) || filepath.Clean(nativeName) != nativeName ||
		nativeName == "." || nativeName == ".." ||
		strings.HasPrefix(nativeName, ".."+string(filepath.Separator)) {
		return fmt.Errorf("xwal: invalid channel name %q", spec.Name)
	}
	for _, component := range strings.FieldsFunc(nativeName, func(r rune) bool {
		return r == '/' || r == '\\'
	}) {
		if strings.HasPrefix(component, ".") ||
			component == manifestName ||
			strings.HasSuffix(component, ".tmp") ||
			strings.HasSuffix(component, ".invalid") ||
			strings.HasSuffix(component, ".replace-pending") {
			return fmt.Errorf("xwal: reserved channel path component %q", component)
		}
	}
	switch spec.Kind {
	case ChannelLog:
		if spec.Reducer != "" {
			return fmt.Errorf("xwal: log channel %q cannot name reducer %q", spec.Name, spec.Reducer)
		}
	case ChannelReducible:
		if spec.Reducer == "" {
			return fmt.Errorf("xwal: reducible channel %q needs a reducer name", spec.Name)
		}
		reducer, ok := resolveReducer(cfg, spec.Reducer)
		if !ok || reducer.Reduce == nil {
			return fmt.Errorf("xwal: no reducer %q registered for channel %q", spec.Reducer, spec.Name)
		}
		codec, err := codecByName(man.Codec)
		if err != nil {
			return err
		}
		if _, err := codec.Hash(reducer.Initial); err != nil {
			return fmt.Errorf("xwal: invalid initial state for channel %q: %w", spec.Name, err)
		}
	default:
		return fmt.Errorf("xwal: invalid kind %d for channel %q", spec.Kind, spec.Name)
	}
	path := filepath.Join(root, spec.Name)
	if info, err := os.Stat(path); err == nil && !info.IsDir() {
		return fmt.Errorf("xwal: channel path %q is not a directory", path)
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	for _, existing := range man.Channels {
		if existing.Name != spec.Name {
			continue
		}
		if existing.Kind != spec.Kind.String() ||
			existing.Reducer != spec.Reducer ||
			existing.Opaque != spec.Opaque {
			return fmt.Errorf("xwal: channel %q already exists as kind=%s reducer=%q opaque=%t",
				spec.Name, existing.Kind, existing.Reducer, existing.Opaque)
		}
	}
	return nil
}

func withChannelSpec(cfg Config, spec ChannelSpec) Config {
	channels := append([]ChannelSpec(nil), cfg.Channels...)
	for i := range channels {
		if channels[i].Name == spec.Name {
			channels[i] = spec
			cfg.Channels = channels
			return cfg
		}
	}
	cfg.Channels = append(channels, spec)
	return cfg
}

func writeManifest(dir string, m manifest) error {
	body, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	path := filepath.Join(dir, manifestName)
	tmp := path + ".tmp"
	if err := writeSyncedFile(tmp, body); err != nil {
		return err
	}
	if err := atomicReplaceFile(tmp, path); err != nil {
		return err
	}
	return disk.SyncDir(dir)
}

func writeChannelPending(root string, plan channelPendingPlan) error {
	body, err := json.Marshal(plan)
	if err != nil {
		return err
	}
	path := filepath.Join(root, channelPendingName)
	tmp := path + ".tmp"
	if err := writeSyncedFile(tmp, body); err != nil {
		return err
	}
	if err := atomicReplaceFile(tmp, path); err != nil {
		return err
	}
	return disk.SyncDir(root)
}

func readChannelPending(root string) (channelPendingPlan, bool, error) {
	path := filepath.Join(root, channelPendingName)
	if err := recoverAtomicReplacement(path); err != nil {
		return channelPendingPlan{}, false, err
	}
	body, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return channelPendingPlan{}, false, nil
	}
	if err != nil {
		return channelPendingPlan{}, false, err
	}
	var plan channelPendingPlan
	if err := json.Unmarshal(body, &plan); err != nil {
		return channelPendingPlan{}, false, err
	}
	return plan, true, nil
}

func removeChannelPending(root string) error {
	if err := os.Remove(filepath.Join(root, channelPendingName)); err != nil &&
		!errors.Is(err, os.ErrNotExist) {
		return err
	}
	return disk.SyncDir(root)
}

func recoverChannelPending(root string, cfg Config, man manifest) (manifest, error) {
	plan, pending, err := readChannelPending(root)
	if err != nil || !pending {
		return man, err
	}
	codec, err := codecByName(man.Codec)
	if err != nil {
		return man, err
	}
	ch, err := channelFromManifest(cfg, plan.Channel)
	if err != nil {
		return man, err
	}
	recovery := &XWAL{root: root, main: man.Main, cfg: cfg, codec: codec}
	if err := recovery.backfillChannel(ch); err != nil {
		return man, err
	}
	found := false
	for _, existing := range man.Channels {
		if existing.Name == plan.Channel.Name {
			if existing != plan.Channel {
				return man, fmt.Errorf("xwal: pending channel %q conflicts with manifest", existing.Name)
			}
			found = true
			break
		}
	}
	if !found {
		man.Channels = append(man.Channels, plan.Channel)
		if err := writeManifest(root, man); err != nil {
			return man, err
		}
	}
	if err := removeChannelPending(root); err != nil {
		return man, err
	}
	return man, nil
}

func channelFromManifest(cfg Config, mc manifestChannel) (*channel, error) {
	ch := &channel{name: mc.Name, kind: ChannelLog, rname: mc.Reducer, opaque: mc.Opaque}
	if mc.Kind != ChannelReducible.String() {
		return ch, nil
	}
	ch.kind = ChannelReducible
	reducer, ok := resolveReducer(cfg, ch.rname)
	if !ok || reducer.Reduce == nil {
		return nil, fmt.Errorf("xwal: no reducer %q registered for channel %q", ch.rname, ch.name)
	}
	ch.reduce = reducer.Reduce
	ch.initial = reducer.Initial
	return ch, nil
}

func recoverManifestTopology(root string, cfg Config, man manifest) (manifest, error) {
	codec, err := codecByName(man.Codec)
	if err != nil {
		return man, err
	}
	for _, mc := range man.Channels {
		if mc.Name == man.Main {
			continue
		}
		ch, err := channelFromManifest(cfg, mc)
		if err != nil {
			return man, err
		}
		recovery := &XWAL{root: root, main: man.Main, cfg: cfg, codec: codec}
		needsRepair, err := recovery.channelTopologyNeedsRepair(ch)
		if err != nil {
			return man, err
		}
		if !needsRepair {
			continue
		}
		if err := writeChannelPending(root, channelPendingPlan{Channel: mc}); err != nil {
			return man, err
		}
		man, err = recoverChannelPending(root, cfg, man)
		if err != nil {
			return man, err
		}
	}
	return man, nil
}

func forkTopologyStructurallyComplete(root string, cfg Config, branch []string) (bool, error) {
	if pathExists(filepath.Join(root, channelPendingName)) {
		return false, nil
	}
	man, err := loadOrCreateManifest(root, cfg)
	if err != nil {
		return false, err
	}
	codec, err := codecByName(man.Codec)
	if err != nil {
		return false, err
	}
	channels := make([]*channel, 0, len(man.Channels))
	for _, mc := range man.Channels {
		ch, err := channelFromManifest(cfg, mc)
		if err != nil {
			return false, err
		}
		channels = append(channels, ch)
		dir := filepath.Join(append([]string{root, mc.Name}, branch...)...)
		complete, err := channelNodeStructurallyComplete(dir, len(branch) == 0, ch, codec)
		if err != nil || !complete {
			return complete, err
		}
	}

	mainDir := filepath.Join(append([]string{root, man.Main}, branch...)...)
	entries, err := os.ReadDir(mainDir)
	if err != nil {
		return false, err
	}
	for _, entry := range entries {
		if !entry.IsDir() || strings.HasPrefix(entry.Name(), ".") {
			continue
		}
		for i, mc := range man.Channels {
			dir := filepath.Join(append([]string{root, mc.Name}, branch...)...)
			complete, err := channelNodeStructurallyComplete(
				filepath.Join(dir, entry.Name()), false, channels[i], codec,
			)
			if err != nil || !complete {
				return complete, err
			}
		}
	}
	return true, nil
}

func channelNodeStructurallyComplete(
	dir string,
	root bool,
	ch *channel,
	codec segment.SegmentCodec,
) (bool, error) {
	info, err := os.Stat(dir)
	if errors.Is(err, os.ErrNotExist) || err == nil && !info.IsDir() {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	base := uint64(1)
	if !root {
		base, err = readForkBaseFile(filepath.Join(dir, ".fork"))
		if err != nil {
			first, ok, firstErr := firstSegmentBase(dir, codec)
			if firstErr != nil {
				return false, firstErr
			}
			if !ok || first != 1 {
				return false, nil
			}
			base = 1
		}
	}
	if ch.kind != ChannelReducible {
		return true, nil
	}
	info, err = os.Stat(watermarkPath(dir, base, codec))
	return err == nil && info.Mode().IsRegular() && info.Size() > 0, nil
}

func repairBranchChannels(root string, cfg Config, branch []string) error {
	man, err := loadOrCreateManifest(root, cfg)
	if err != nil {
		return err
	}
	man, err = recoverChannelPending(root, cfg, man)
	if err != nil {
		return err
	}
	codec, err := codecByName(man.Codec)
	if err != nil {
		return err
	}
	for _, mc := range man.Channels {
		if mc.Name == man.Main {
			continue
		}
		ch, err := channelFromManifest(cfg, mc)
		if err != nil {
			return err
		}
		needsRepair, err := branchChannelNeedsRepair(root, man.Main, cfg, codec, branch, ch)
		if err != nil {
			return err
		}
		if !needsRepair {
			continue
		}
		if err := writeChannelPending(root, channelPendingPlan{Channel: mc}); err != nil {
			return err
		}
		man, err = recoverChannelPending(root, cfg, man)
		if err != nil {
			return err
		}
		needsRepair, err = branchChannelNeedsRepair(root, man.Main, cfg, codec, branch, ch)
		if err != nil {
			return err
		}
		if needsRepair {
			return fmt.Errorf("%w: channel %q branch %q remains incomplete after repair",
				ErrTopologyIncomplete, mc.Name, strings.Join(branch, "/"))
		}
	}
	return nil
}

func repairRehomeDescendants(root string, cfg Config, branch []string) error {
	man, err := loadOrCreateManifest(root, cfg)
	if err != nil {
		return err
	}
	mainDir := filepath.Join(append([]string{root, man.Main}, branch...)...)
	entries, err := os.ReadDir(mainDir)
	if err != nil {
		return err
	}
	var descendants []string
	for _, entry := range entries {
		if entry.IsDir() && !strings.HasPrefix(entry.Name(), ".") {
			descendants = append(descendants, entry.Name())
		}
	}
	if len(descendants) == 0 {
		return nil
	}
	codec, err := codecByName(man.Codec)
	if err != nil {
		return err
	}
	for _, mc := range man.Channels {
		if mc.Name == man.Main {
			continue
		}
		channelDir := filepath.Join(append([]string{root, mc.Name}, branch...)...)
		for _, descendant := range descendants {
			dir := filepath.Join(channelDir, descendant)
			if validForkNode(dir, codec) {
				continue
			}
			if err := writeChannelPending(root, channelPendingPlan{Channel: mc}); err != nil {
				return err
			}
			man, err = recoverChannelPending(root, cfg, man)
			if err != nil {
				return err
			}
			if !validForkNode(dir, codec) {
				return fmt.Errorf("%w: channel %q descendant %q has no valid fork marker",
					ErrTopologyIncomplete, mc.Name, descendant)
			}
		}
	}
	return nil
}

func validForkNode(dir string, codec segment.SegmentCodec) bool {
	if _, err := readForkBaseFile(filepath.Join(dir, ".fork")); err == nil {
		return true
	}
	first, ok, err := firstSegmentBase(dir, codec)
	return err == nil && ok && first == 1
}

func branchChannelNeedsRepair(
	root, main string,
	cfg Config,
	codec segment.SegmentCodec,
	branch []string,
	ch *channel,
) (bool, error) {
	parentDir := filepath.Join(root, ch.name)
	info, err := os.Stat(parentDir)
	if errors.Is(err, os.ErrNotExist) || err == nil && !info.IsDir() {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	if ch.kind == ChannelReducible {
		valid, err := validateWatermark(parentDir, 1, codec, ch.initial)
		if err != nil || !valid {
			return true, nil
		}
	}
	recovery := &XWAL{root: root, main: main, cfg: cfg, codec: codec}
	for _, part := range branch {
		dir := filepath.Join(parentDir, part)
		info, err := os.Stat(dir)
		if errors.Is(err, os.ErrNotExist) || err == nil && !info.IsDir() {
			return true, nil
		}
		if err != nil {
			return false, err
		}
		base, err := readForkBaseFile(filepath.Join(dir, ".fork"))
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				first, ok, segmentErr := firstSegmentBase(dir, codec)
				if segmentErr != nil {
					return false, segmentErr
				}
				if ok && first == 1 {
					if ch.kind == ChannelReducible {
						valid, err := validateWatermark(dir, 1, codec, ch.initial)
						if err != nil || !valid {
							return true, nil
						}
					}
					parentDir = dir
					continue
				}
			}
			return true, nil
		}
		if ch.kind == ChannelReducible {
			state, err := recovery.backfillWatermarkState(parentDir, base, ch)
			if err != nil {
				return false, err
			}
			valid, err := validateWatermark(dir, base, codec, state)
			if err != nil || !valid {
				return true, nil
			}
		}
		parentDir = dir
	}
	return false, nil
}

func (x *XWAL) channelTopologyNeedsRepair(ch *channel) (bool, error) {
	mainBase := filepath.Join(x.root, x.main)
	chBase := filepath.Join(x.root, ch.name)
	var walk func(mainDir, chDir, parentChDir string, depth int) (bool, error)
	walk = func(mainDir, chDir, parentChDir string, depth int) (bool, error) {
		info, err := os.Stat(chDir)
		if errors.Is(err, os.ErrNotExist) || err == nil && !info.IsDir() {
			return true, nil
		}
		if err != nil {
			return false, err
		}
		if depth == 0 {
			if ch.kind == ChannelReducible {
				valid, err := validateWatermark(chDir, 1, x.codec, ch.initial)
				if err != nil || !valid {
					return true, nil
				}
			}
		} else {
			base, markerErr := readForkBaseFile(filepath.Join(chDir, ".fork"))
			if markerErr != nil {
				if first, ok, segmentErr := firstSegmentBase(chDir, x.codec); segmentErr != nil {
					return false, segmentErr
				} else if !ok || first != 1 {
					return true, nil
				}
			} else if ch.kind == ChannelReducible {
				state, err := x.backfillWatermarkState(parentChDir, base, ch)
				if err != nil {
					return false, err
				}
				valid, err := validateWatermark(chDir, base, x.codec, state)
				if err != nil || !valid {
					return true, nil
				}
			}
		}
		entries, err := os.ReadDir(mainDir)
		if err != nil {
			return false, err
		}
		for _, entry := range entries {
			if !entry.IsDir() || strings.HasPrefix(entry.Name(), ".") {
				continue
			}
			needsRepair, err := walk(
				filepath.Join(mainDir, entry.Name()),
				filepath.Join(chDir, entry.Name()),
				chDir,
				depth+1,
			)
			if err != nil || needsRepair {
				return needsRepair, err
			}
		}
		return false, nil
	}
	return walk(mainBase, chBase, "", 0)
}

func (x *XWAL) channelOpts(ch *channel) disk.Options {
	opts := disk.Options{
		Codec: x.codec, SegmentSize: x.cfg.SegmentSize, MaxUnflushedBytes: x.cfg.MaxUnflushedBytes,
	}
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
	if err := x.ensurePrivate(); err != nil {
		return err
	}
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
	if ch.kind == ChannelReducible {
		if err := seedWatermark(ch.dir, 1, x.codec, ch.initial); err != nil {
			return err
		}
	} else if err := seedEmptyLog(ch.dir, x.codec); err != nil {
		return err
	}
	l, err := log.Open(ch.dir, x.channelOpts(ch))
	if err != nil {
		return err
	}
	ch.log = l
	ch.fk = map[uint64]uint64{}
	ch.fkBuilt = true // wiped channel: the empty index is current
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
	if err := x.ensurePrivate(); err != nil {
		return err
	}
	endMutation, err := beginRootAdditiveMutation(x.root)
	if err != nil {
		return err
	}
	defer endMutation()
	retireTrunkStores(x.root)
	if _, exists := x.chans[spec.Name]; exists {
		return fmt.Errorf("xwal: channel %q already exists", spec.Name)
	}
	man, err := loadOrCreateManifest(x.root, x.cfg)
	if err != nil {
		return err
	}
	if err := validateChannelSpec(x.root, x.cfg, man, spec); err != nil {
		return err
	}
	ch := &channel{
		name: spec.Name, kind: spec.Kind, rname: spec.Reducer, opaque: spec.Opaque,
	}
	if spec.Kind == ChannelReducible {
		r, ok := resolveReducer(x.cfg, spec.Reducer)
		if !ok || r.Reduce == nil {
			return fmt.Errorf("xwal: no reducer %q registered for channel %q", spec.Reducer, spec.Name)
		}
		ch.reduce = r.Reduce
		ch.initial = r.Initial
	}
	pending := channelPendingPlan{Channel: manifestChannel{
		Name: spec.Name, Kind: spec.Kind.String(), Reducer: spec.Reducer, Opaque: spec.Opaque,
	}}
	if err := writeChannelPending(x.root, pending); err != nil {
		return err
	}
	x.cfg = withChannelSpec(x.cfg, spec)
	if _, err := recoverChannelPending(x.root, x.cfg, man); err != nil {
		return fmt.Errorf("xwal: complete channel %q: %w", spec.Name, err)
	}
	retireTrunkStores(x.root)
	// Open the handle for THIS branch (now that the structure exists).
	cdir := x.channelDir(spec.Name)
	l, err := log.Open(cdir, x.channelOpts(ch))
	if err != nil {
		return err
	}
	ch.dir = cdir
	ch.log = l
	ch.fk = map[uint64]uint64{} // indexed lazily from the tail on Lookup
	x.chans[spec.Name] = ch
	x.order = append(x.order, spec.Name)
	return nil
}

// backfillChannel materializes a newly-added channel's node tree to mirror
// the main channel's directory tree: for every main-channel node dir it
// creates the corresponding channel dir. Existing fork markers keep their
// base; missing nodes inherit the parent's visible tail. Reducible watermarks
// are repaired from parent state. No payload entries are written.
func (x *XWAL) backfillChannel(ch *channel) error {
	mainBase := filepath.Join(x.root, x.main)
	chBase := filepath.Join(x.root, ch.name)
	var walk func(mainDir, chDir, parentChDir string, depth int) error
	walk = func(mainDir, chDir, parentChDir string, depth int) error {
		if err := mkdirAllSynced(chDir); err != nil {
			return err
		}
		if depth > 0 {
			if err := x.ensureBackfillFork(mainDir, parentChDir, chDir, ch); err != nil {
				return err
			}
		} else if ch.kind == ChannelReducible {
			// Seed the channel root with the Initial watermark too.
			if err := ensureWatermark(chDir, 1, x.codec, ch.initial); err != nil {
				return err
			}
		}
		ents, err := os.ReadDir(mainDir)
		if err != nil {
			return err
		}
		for _, e := range ents {
			if e.IsDir() && !strings.HasPrefix(e.Name(), ".") {
				if err := walk(
					filepath.Join(mainDir, e.Name()),
					filepath.Join(chDir, e.Name()),
					chDir,
					depth+1,
				); err != nil {
					return err
				}
			}
		}
		return nil
	}
	return walk(mainBase, chBase, "", 0)
}

func (x *XWAL) ensureBackfillFork(mainDir, parentDir, chDir string, ch *channel) error {
	if _, err := os.Stat(filepath.Join(chDir, ".fork")); errors.Is(err, os.ErrNotExist) {
		if first, ok, baseErr := firstSegmentBase(chDir, x.codec); baseErr != nil {
			return baseErr
		} else if ok && first == 1 {
			if ch.kind == ChannelReducible {
				return ensureWatermark(chDir, 1, x.codec, ch.initial)
			}
			return nil
		}
	}
	marker := filepath.Join(chDir, ".fork")
	base, err := readForkBaseFile(marker)
	switch {
	case err == nil:
	default:
		if first, ok, segmentErr := firstSegmentBase(chDir, x.codec); segmentErr != nil {
			return segmentErr
		} else if ok {
			base = first
		} else {
			parent, openErr := log.Open(parentDir, x.channelOpts(ch))
			if openErr != nil {
				return openErr
			}
			last := parent.LastIndex()
			if last > 0 {
				frame, readErr := parent.Read(last)
				if readErr != nil {
					parent.Close()
					return readErr
				}
				lastMainLT, decodeErr := decodeMainLT(frame)
				if decodeErr != nil {
					parent.Close()
					return decodeErr
				}
				mainForkBase, markerErr := readForkBaseFile(filepath.Join(mainDir, ".fork"))
				if markerErr != nil {
					parent.Close()
					return markerErr
				}
				if lastMainLT >= mainForkBase {
					parent.Close()
					return fmt.Errorf(
						"%w: channel %q has main-LT %d at or after missing branch base %d",
						ErrTopologyIncomplete, ch.name, lastMainLT, mainForkBase,
					)
				}
			}
			closeErr := parent.Close()
			if closeErr != nil {
				return closeErr
			}
			if last == ^uint64(0) {
				return fmt.Errorf("xwal: cannot backfill channel after max index")
			}
			base = last + 1
		}
		if err := writeBackfillFork(chDir, base); err != nil {
			return err
		}
	}
	if ch.kind != ChannelReducible {
		return nil
	}
	state, err := x.backfillWatermarkState(parentDir, base, ch)
	if err != nil {
		return err
	}
	return ensureWatermark(chDir, base, x.codec, state)
}

func mkdirAllSynced(path string) error {
	if info, err := os.Stat(path); err == nil {
		if !info.IsDir() {
			return fmt.Errorf("xwal: %q is not a directory", path)
		}
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	parent := filepath.Dir(path)
	if parent != path {
		if err := mkdirAllSynced(parent); err != nil {
			return err
		}
	}
	if err := os.Mkdir(path, 0o755); err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	if err := disk.SyncDir(parent); err != nil {
		return err
	}
	return disk.SyncDir(path)
}

func (x *XWAL) backfillWatermarkState(parentDir string, base uint64, ch *channel) ([]byte, error) {
	if base <= 1 {
		return ch.initial, nil
	}
	parent, err := log.Open(parentDir, x.channelOpts(ch))
	if err != nil {
		return nil, err
	}
	state, err := parent.StateAt(base - 1)
	closeErr := parent.Close()
	if err != nil {
		return nil, err
	}
	if closeErr != nil {
		return nil, closeErr
	}
	return state, nil
}

func readForkBaseFile(path string) (uint64, error) {
	if err := recoverAtomicReplacement(path); err != nil {
		return 0, err
	}
	body, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	for _, line := range strings.Split(strings.TrimSpace(string(body)), "\n") {
		if value, ok := strings.CutPrefix(strings.TrimSpace(line), "base="); ok {
			base, err := strconv.ParseUint(strings.TrimSpace(value), 10, 64)
			if err != nil || base == 0 {
				return 0, fmt.Errorf("xwal: malformed fork marker %q", path)
			}
			return base, nil
		}
	}
	return 0, fmt.Errorf("xwal: malformed fork marker %q", path)
}

func writeBackfillFork(chDir string, base uint64) error {
	body := fmt.Sprintf("base=%d\n", base)
	final := filepath.Join(chDir, ".fork")
	tmp := final + ".tmp"
	file, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := file.Write([]byte(body)); err != nil {
		file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := atomicReplaceFile(tmp, final); err != nil {
		return err
	}
	return disk.SyncDir(chDir)
}

func seedEmptyLog(dir string, codec segment.SegmentCodec) error {
	final := watermarkPath(dir, 1, codec)
	if pathExists(final) {
		return nil
	}
	tmp := final + ".tmp"
	_ = os.Remove(tmp)
	seg, err := segment.Create(tmp, codec, 1, 0)
	if err != nil {
		return err
	}
	if err := seg.Sync(); err != nil {
		seg.Close()
		return err
	}
	if err := seg.Close(); err != nil {
		return err
	}
	if err := atomicReplaceFile(tmp, final); err != nil {
		return err
	}
	return disk.SyncDir(dir)
}

func ensureWatermark(chDir string, baseIndex uint64, codec segment.SegmentCodec, initial []byte) error {
	valid, err := validateWatermark(chDir, baseIndex, codec, initial)
	if err != nil {
		return err
	}
	if valid {
		return nil
	}
	return rewriteWatermark(chDir, baseIndex, codec, initial)
}

func watermarkPath(chDir string, baseIndex uint64, codec segment.SegmentCodec) string {
	return filepath.Join(chDir, fmt.Sprintf("%020d%s", baseIndex, codec.FileExt()))
}

func firstSegmentBase(dir string, codec segment.SegmentCodec) (uint64, bool, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, false, err
	}
	var first uint64
	found := false
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != codec.FileExt() {
			continue
		}
		base, err := strconv.ParseUint(strings.TrimSuffix(entry.Name(), codec.FileExt()), 10, 64)
		if err != nil {
			continue
		}
		if !found || base < first {
			first = base
			found = true
		}
	}
	return first, found, nil
}

func validateWatermark(
	chDir string,
	baseIndex uint64,
	codec segment.SegmentCodec,
	expected []byte,
) (bool, error) {
	path := watermarkPath(chDir, baseIndex, codec)
	if err := recoverAtomicReplacement(path); err != nil {
		return false, err
	}
	if !pathExists(path) {
		return false, nil
	}
	header, _, ok, err := readWatermarkSegment(path, codec)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	return statesEqual(header, expected), nil
}

func statesEqual(left, right []byte) bool {
	if bytes.Equal(left, right) {
		return true
	}
	var l, r any
	ld := json.NewDecoder(bytes.NewReader(left))
	rd := json.NewDecoder(bytes.NewReader(right))
	ld.UseNumber()
	rd.UseNumber()
	if ld.Decode(&l) != nil || rd.Decode(&r) != nil {
		return false
	}
	lc, lerr := json.Marshal(l)
	rc, rerr := json.Marshal(r)
	return lerr == nil && rerr == nil && bytes.Equal(lc, rc)
}

func readWatermarkSegment(path string, codec segment.SegmentCodec) ([]byte, uint64, bool, error) {
	header, entries, ok, err := readHeaderedSegment(path, codec)
	return header, uint64(len(entries)), ok, err
}

func readHeaderedSegment(
	path string,
	codec segment.SegmentCodec,
) ([]byte, [][]byte, bool, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, false, err
	}
	defer file.Close()
	type span struct {
		off int64
		len int
	}
	var spans []span
	if err := codec.ScanFrames(file, func(off int64, frameLen int) error {
		spans = append(spans, span{off: off, len: frameLen})
		return nil
	}); err != nil {
		return nil, nil, false, err
	}
	if len(spans) == 0 {
		return nil, nil, false, nil
	}
	frames := make([][]byte, 0, len(spans))
	for _, span := range spans {
		payload, _, err := codec.ReadFrame(file, span.off, span.off+int64(span.len))
		if err != nil {
			return nil, nil, false, err
		}
		frames = append(frames, payload)
	}
	return frames[0], frames[1:], true, nil
}

func rewriteWatermark(
	chDir string,
	baseIndex uint64,
	codec segment.SegmentCodec,
	header []byte,
) error {
	final := watermarkPath(chDir, baseIndex, codec)
	var entries [][]byte
	if pathExists(final) {
		_, existing, readable, err := readHeaderedSegment(final, codec)
		if err != nil {
			return err
		}
		if readable {
			entries = existing
		}
	}
	tmp := final + ".tmp"
	_ = os.Remove(tmp)
	seg, err := segment.Create(tmp, codec, baseIndex, 0)
	if err != nil {
		return err
	}
	if err := seg.WriteHeader(header); err != nil {
		seg.Close()
		return err
	}
	for _, entry := range entries {
		if _, err := seg.Append(entry); err != nil {
			seg.Close()
			return err
		}
	}
	if err := seg.Sync(); err != nil {
		seg.Close()
		return err
	}
	if err := seg.Close(); err != nil {
		return err
	}
	if err := atomicReplaceFile(tmp, final); err != nil {
		return err
	}
	return disk.SyncDir(chDir)
}

// seedWatermark writes a header-only segment at baseIndex carrying the
// reducible Initial state, so an empty reducible node folds from a defined
// watermark (mirrors disk.Fork's writeWatermarkSeg).
func seedWatermark(chDir string, baseIndex uint64, codec segment.SegmentCodec, initial []byte) error {
	final := watermarkPath(chDir, baseIndex, codec)
	tmp := final + ".tmp"
	_ = os.Remove(tmp)
	seg, err := segment.Create(tmp, codec, baseIndex, 0)
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
	if err := seg.Close(); err != nil {
		return err
	}
	if err := atomicReplaceFile(tmp, final); err != nil {
		return err
	}
	return disk.SyncDir(chDir)
}

func atomicReplaceFile(tmp, final string) error {
	if err := os.Rename(tmp, final); err == nil {
		_ = os.Remove(final + ".invalid")
		_ = os.Remove(final + ".replace-pending")
		return nil
	} else if !pathExists(final) {
		return err
	}
	invalid := final + ".invalid"
	pending := final + ".replace-pending"
	if err := writeSyncedFile(pending, []byte("replace\n")); err != nil {
		return err
	}
	if err := disk.SyncDir(filepath.Dir(final)); err != nil {
		return err
	}
	_ = os.Remove(invalid)
	if err := os.Rename(final, invalid); err != nil {
		return err
	}
	if err := disk.SyncDir(filepath.Dir(final)); err != nil {
		return err
	}
	if err := os.Rename(tmp, final); err != nil {
		_ = os.Rename(invalid, final)
		return err
	}
	if err := disk.SyncDir(filepath.Dir(final)); err != nil {
		return err
	}
	_ = os.Remove(invalid)
	_ = os.Remove(pending)
	return nil
}

func recoverAtomicReplacement(final string) error {
	pending := final + ".replace-pending"
	if !pathExists(pending) {
		return nil
	}
	tmp := final + ".tmp"
	invalid := final + ".invalid"
	if !pathExists(final) {
		switch {
		case pathExists(tmp):
			if err := os.Rename(tmp, final); err != nil {
				return err
			}
		case pathExists(invalid):
			if err := os.Rename(invalid, final); err != nil {
				return err
			}
		default:
			return fmt.Errorf("xwal: replacement pending for %q without recoverable file", final)
		}
	}
	_ = os.Remove(tmp)
	_ = os.Remove(invalid)
	_ = os.Remove(pending)
	return disk.SyncDir(filepath.Dir(final))
}

func writeSyncedFile(path string, body []byte) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := file.Write(body); err != nil {
		file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	return file.Close()
}

// AppendMain appends payload (with optional opaque meta) to the main
// channel. The returned mainLT is the channel index it landed at;
// related-channel entries reference it.
func (x *XWAL) AppendMain(payload, meta []byte) (uint64, error) {
	ch := x.chans[x.main]
	ch.mu.Lock()
	defer ch.mu.Unlock()
	next := ch.log.LastIndex() + 1
	if err := ch.log.Write(next, encodeChannelFrame(next, payload, meta, ch.opaque)); err != nil {
		return 0, err
	}
	if ch.fkScan || ch.fkBuilt {
		ch.fk[next] = next
	}
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
	ch.mu.Lock()
	defer ch.mu.Unlock()
	if lastMain, ok, err := x.tailMain(ch); err != nil {
		return 0, err
	} else if ok && mainLT < lastMain {
		return 0, fmt.Errorf("xwal: channel %q main-LT must be non-decreasing: got %d, last %d",
			channelName, mainLT, lastMain)
	}
	next := ch.log.LastIndex() + 1
	if err := ch.log.Write(next, encodeChannelFrame(mainLT, payload, meta, ch.opaque)); err != nil {
		return 0, err
	}
	if ch.fkScan || ch.fkBuilt {
		ch.fk[mainLT] = next
	}
	return next, nil
}

func (x *XWAL) flushAll() error {
	if ch := x.chans[x.main]; ch != nil {
		if err := ch.log.Flush(); err != nil {
			return err
		}
	}
	for _, name := range x.order {
		if name == x.main {
			continue
		}
		if err := x.chans[name].log.Flush(); err != nil {
			return err
		}
	}
	return nil
}

// flushCoherent persists this lineage's channels as one cut: the main
// channel first, then each related channel only up to the last record
// whose main-LT referent is already durable.
func (x *XWAL) flushCoherent() error {
	main := x.chans[x.main]
	if main == nil {
		return x.flushAll()
	}
	if err := main.log.Flush(); err != nil {
		return err
	}
	mainTail := main.log.LastIndex()
	for _, name := range x.order {
		if name == x.main {
			continue
		}
		ch := x.chans[name]
		target, err := coherentTarget(ch, mainTail)
		if err != nil {
			return err
		}
		if err := ch.log.FlushTo(target); err != nil {
			return err
		}
	}
	return nil
}

// coherentTarget is the highest pending index whose record references a
// main-LT at or below mainTail. Records are main-LT-non-decreasing, so
// everything at or below it is safe to persist.
func coherentTarget(ch *channel, mainTail uint64) (uint64, error) {
	first, last, ok := ch.log.PendingBounds()
	if !ok {
		return 0, nil
	}
	for idx := last; idx >= first; idx-- {
		f, err := ch.log.Read(idx)
		if err != nil {
			return 0, err
		}
		m, err := decodeMainLT(f)
		if err != nil {
			return 0, err
		}
		if m <= mainTail {
			return idx, nil
		}
		if idx == first {
			break
		}
	}
	return 0, nil
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
	// The main channel is identity (main-LT == channel-LT), so it needs no fk
	// index: read directly at mainLT, treating out-of-range as "not found".
	if channelName == x.main {
		first, last := ch.log.FirstIndex(), ch.log.LastIndex()
		if first == 0 || mainLT < first || mainLT > last {
			return Record{}, false, nil
		}
		r, err := x.ReadAt(channelName, mainLT)
		if err != nil {
			return Record{}, false, err
		}
		return r, true, nil
	}
	lt, ok, err := ch.lookup(mainLT)
	if err != nil {
		return Record{}, false, err
	}
	if !ok {
		return Record{}, false, nil
	}
	r, err := x.ReadAt(channelName, lt)
	if err != nil {
		return Record{}, false, err
	}
	return r, true, nil
}

// RecordsFrom returns channel records whose main timeline LT is at least
// fromMainLT, ordered by channel LT. A non-zero limit caps the returned
// prefix; zero returns every matching record. It walks the immutable channel
// snapshot backward only to locate the boundary, then reads the requested
// ascending delta without constructing a total-history foreign-key index.
func (x *XWAL) RecordsFrom(channelName string, fromMainLT uint64, limit int) ([]Record, error) {
	if limit < 0 {
		return nil, fmt.Errorf("xwal: negative record limit %d", limit)
	}
	ch := x.chans[channelName]
	if ch == nil {
		return nil, fmt.Errorf("xwal: no channel %q", channelName)
	}
	snapshot := ch.log.Snapshot()

	var first uint64
	err := snapshot.ScanFromEnd(0, func(idx uint64, frame []byte) error {
		mainLT, err := decodeMainLT(frame)
		if err != nil {
			return err
		}
		if mainLT < fromMainLT {
			return errStopRange
		}
		first = idx
		return nil
	})
	if err != nil && !errors.Is(err, errStopRange) {
		return nil, err
	}
	if first == 0 {
		return nil, nil
	}

	records := make([]Record, 0)
	err = snapshot.Range(first, func(idx uint64, frame []byte) error {
		record, err := decodeRecord(idx, frame)
		if err != nil {
			return err
		}
		if record.MainLT < fromMainLT {
			return nil
		}
		records = append(records, record)
		if limit > 0 && len(records) == limit {
			return errStopRange
		}
		return nil
	})
	if err != nil && !errors.Is(err, errStopRange) {
		return nil, err
	}
	return records, nil
}

// LatestChannelRecord returns the newest record from one immutable channel
// snapshot when its main LT meets the requested recovery watermark.
func (x *XWAL) LatestChannelRecord(channelName string, minMainLT uint64) (Record, bool, error) {
	ch := x.chans[channelName]
	if ch == nil {
		return Record{}, false, fmt.Errorf("xwal: no channel %q", channelName)
	}
	var latest Record
	found := false
	err := ch.log.Snapshot().ScanFromEnd(0, func(idx uint64, frame []byte) error {
		record, err := decodeRecord(idx, frame)
		if err != nil {
			return err
		}
		if record.MainLT >= minMainLT {
			latest = record
			found = true
		}
		return errStopRange
	})
	if err != nil && !errors.Is(err, errStopRange) {
		return Record{}, false, err
	}
	return latest, found, nil
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
	m, err := decodeMainLT(f)
	return m, true, err
}

// ChannelInfo is a read-only snapshot of a channel's bounds.
type ChannelInfo struct {
	Name     string
	Kind     Kind
	Reducer  string
	Opaque   bool
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
			Opaque:   ch.opaque,
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

func (x *XWAL) sharedView(release func() error, releaseRoot func(), retire func()) *XWAL {
	return &XWAL{
		root:        x.root,
		branch:      append([]string(nil), x.branch...),
		main:        x.main,
		order:       x.order,
		chans:       x.chans,
		cfg:         x.cfg,
		codec:       x.codec,
		shared:      true,
		release:     release,
		releaseRoot: releaseRoot,
		retire:      retire,
	}
}

// Close closes every channel.
func (x *XWAL) Close() error {
	x.closeOnce.Do(func() {
		if x.release != nil {
			x.closeErr = x.release()
			if x.releaseRoot != nil {
				x.releaseRoot()
				x.releaseRoot = nil
			}
			if x.releaseLineage != nil {
				x.releaseLineage()
				x.releaseLineage = nil
			}
			return
		}
		if x.shared {
			return
		}
		for _, ch := range x.chans {
			if ch.log != nil {
				if err := ch.log.Close(); err != nil && x.closeErr == nil {
					x.closeErr = err
				}
			}
		}
		if x.releaseRoot != nil {
			x.releaseRoot()
			x.releaseRoot = nil
		}
		if x.releaseLineage != nil {
			x.releaseLineage()
			x.releaseLineage = nil
		}
	})
	return x.closeErr
}

func (x *XWAL) ensurePrivate() error {
	if x.retire != nil {
		x.retire()
	} else {
		retireTrunkStores(x.root)
	}
	if !x.shared {
		return nil
	}
	private, err := Open(x.root, x.cfg, x.branch...)
	if err != nil {
		return err
	}
	if x.releaseRoot != nil && x.borrowOwner != nil {
		transferRootBorrow(x.borrowRoot, x.borrowOwner, nil)
		root := x.borrowRoot
		x.releaseRoot = func() { endRootBorrow(root, nil) }
		x.borrowOwner = nil
	}
	var releaseErr error
	if x.release != nil {
		releaseErr = x.release()
	}
	x.main = private.main
	x.order = private.order
	x.chans = private.chans
	x.codec = private.codec
	x.shared = false
	x.release = nil
	x.retire = nil
	return releaseErr
}

// A channel entry is stored as a JSON object so it round-trips through
// either codec. Legacy channels embed JSON under p. Opaque channels put the
// original payload bytes in base64 under p64 so JSONL canonicalization cannot
// rewrite them. Meta remains a free side-channel. Reducible watermarks are
// stored as the bare state object, not wrapped.
type frameObj struct {
	M   uint64          `json:"m"`
	P   json.RawMessage `json:"p,omitempty"`
	P64 *string         `json:"p64,omitempty"`
	X   json.RawMessage `json:"x,omitempty"`
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
	return encodeChannelFrame(mainLT, payload, meta, false)
}

func encodeChannelFrame(mainLT uint64, payload, meta []byte, opaque bool) []byte {
	o := frameObj{M: mainLT}
	if opaque {
		encoded := base64.StdEncoding.EncodeToString(payload)
		o.P64 = &encoded
	} else {
		o.P = embedJSON(payload)
		if len(o.P) == 0 {
			o.P = json.RawMessage("null")
		}
	}
	o.X = embedJSON(meta)
	b, _ := json.Marshal(o)
	return b
}

// decodeFrame returns the main-LT and payload, ignoring meta. Used by the
// fold and fork-boundary paths that don't care about meta.
func decodeFrame(f []byte) (uint64, []byte, error) {
	if m, p, _, ok := fastDecodeFrame(f); ok {
		return m, p, nil
	}
	var o frameObj
	if err := json.Unmarshal(f, &o); err != nil {
		return 0, nil, fmt.Errorf("xwal: decode frame: %w", err)
	}
	payload, err := o.payload()
	if err != nil {
		return 0, nil, err
	}
	return o.M, payload, nil
}

func decodeRecord(channelLT uint64, f []byte) (Record, error) {
	if m, p, x, ok := fastDecodeFrame(f); ok {
		return Record{ChannelLT: channelLT, MainLT: m, Payload: p, Meta: x}, nil
	}
	var o frameObj
	if err := json.Unmarshal(f, &o); err != nil {
		return Record{}, fmt.Errorf("xwal: decode frame: %w", err)
	}
	payload, err := o.payload()
	if err != nil {
		return Record{}, err
	}
	return Record{ChannelLT: channelLT, MainLT: o.M, Payload: payload, Meta: o.X}, nil
}

func (o frameObj) payload() ([]byte, error) {
	if o.P64 == nil {
		return o.P, nil
	}
	payload, err := base64.StdEncoding.DecodeString(*o.P64)
	if err != nil {
		return nil, fmt.Errorf("xwal: decode opaque payload: %w", err)
	}
	return payload, nil
}

func decodeMainLT(f []byte) (uint64, error) {
	if m, ok := fastDecodeMainLT(f); ok {
		return m, nil
	}
	var o struct {
		M uint64 `json:"m"`
	}
	if err := json.Unmarshal(f, &o); err != nil {
		return 0, fmt.Errorf("xwal: decode frame: %w", err)
	}
	return o.M, nil
}

func fastDecodeMainLT(f []byte) (uint64, bool) {
	const prefix = `{"m":`
	if len(f) <= len(prefix) || string(f[:len(prefix)]) != prefix || !json.Valid(f) {
		return 0, false
	}
	i := len(prefix)
	start := i
	var mainLT uint64
	for i < len(f) && f[i] >= '0' && f[i] <= '9' {
		d := uint64(f[i] - '0')
		if mainLT > (^uint64(0)-d)/10 {
			return 0, false
		}
		mainLT = mainLT*10 + d
		i++
	}
	return mainLT, i > start && i < len(f) && f[i] == ','
}

func fastDecodeFrame(f []byte) (uint64, []byte, []byte, bool) {
	const prefix = `{"m":`
	if len(f) <= len(prefix) || string(f[:len(prefix)]) != prefix || !json.Valid(f) {
		return 0, nil, nil, false
	}
	i := len(prefix)
	var mainLT uint64
	start := i
	for i < len(f) && f[i] >= '0' && f[i] <= '9' {
		d := uint64(f[i] - '0')
		if mainLT > (^uint64(0)-d)/10 {
			return 0, nil, nil, false
		}
		mainLT = mainLT*10 + d
		i++
	}
	if i == start {
		return 0, nil, nil, false
	}
	opaque := false
	switch {
	case i+5 <= len(f) && string(f[i:i+5]) == `,"p":`:
		i += 5
	case i+7 <= len(f) && string(f[i:i+7]) == `,"p64":`:
		i += 7
		opaque = true
	default:
		return 0, nil, nil, false
	}
	end, ok := jsonValueEnd(f, i)
	if !ok {
		return 0, nil, nil, false
	}
	payload := f[i:end]
	if opaque {
		if len(payload) < 2 || payload[0] != '"' || payload[len(payload)-1] != '"' {
			return 0, nil, nil, false
		}
		decoded := make([]byte, base64.StdEncoding.DecodedLen(len(payload)-2))
		n, err := base64.StdEncoding.Decode(decoded, payload[1:len(payload)-1])
		if err != nil {
			return 0, nil, nil, false
		}
		payload = decoded[:n]
	}
	if end+1 == len(f) && f[end] == '}' {
		return mainLT, payload, nil, true
	}
	const metaPrefix = `,"x":`
	if end+len(metaPrefix) >= len(f) || string(f[end:end+len(metaPrefix)]) != metaPrefix {
		return 0, nil, nil, false
	}
	i = end + len(metaPrefix)
	end, ok = jsonValueEnd(f, i)
	if !ok || end+1 != len(f) || f[end] != '}' {
		return 0, nil, nil, false
	}
	return mainLT, payload, f[i:end], true
}

func jsonValueEnd(b []byte, start int) (int, bool) {
	if start >= len(b) {
		return 0, false
	}
	switch b[start] {
	case '"':
		for i := start + 1; i < len(b); i++ {
			switch b[i] {
			case '\\':
				i++
			case '"':
				return i + 1, true
			}
		}
		return 0, false
	case '{', '[':
		depth := 0
		inString := false
		for i := start; i < len(b); i++ {
			if inString {
				switch b[i] {
				case '\\':
					i++
				case '"':
					inString = false
				}
				continue
			}
			switch b[i] {
			case '"':
				inString = true
			case '{', '[':
				depth++
			case '}', ']':
				depth--
				if depth == 0 {
					return i + 1, true
				}
			}
		}
		return 0, false
	default:
		for i := start; i < len(b); i++ {
			if b[i] == ',' || b[i] == '}' {
				return i, i > start
			}
		}
		return 0, false
	}
}
