package xwal

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jack-work/figwal/disk"
	"github.com/jack-work/figwal/log"
)

// Trunks is the trunk-addressed view of a joint xwal. A trunk is one
// continuation chain across all channels (the triune forked as a unit);
// it is the stable handle a caller addresses, while the per-fork node is
// plumbing.
//
// DISK IS THE SOLE SOURCE OF TRUTH. The node tree is the main channel's
// directory tree (dirs + .fork). The ONLY datum not derivable from that
// tree is a node's trunk id, so that — and only that — is persisted, in a
// `.trunk` file in each node's main-channel dir (alongside .fork). This
// in-memory structure is a derived cache, rebuilt by one walk on open; it
// cannot diverge from disk because it is read from disk.
//
// Identity rules:
//   - A fork freezes the node; the continuation INHERITS the trunk (same
//     id, head advances), the alternative FOUNDS a new trunk.
//   - An empty node has no logical time of its own — it IS its parent's
//     tail position. So forking an empty head redirects to an N-ary fork
//     of the parent; an empty node never becomes a parent.
type Trunks struct {
	root string
	cfg  Config
	main string

	registryRoot string

	mu       sync.RWMutex
	nodes    map[string]*tnode // key = branch joined by "/" ("" = root)
	heads    map[string]string // trunk id -> head node key (the one live leaf)
	nodeSeq  int               // next "n<N>" dir name
	trunkSeq int               // next "t<N>" trunk id

	// version is the local rebuild cookie; rootEpoch tracks mutations made
	// through any in-process peer for the same canonical root.
	version   atomic.Uint64
	rootEpoch atomic.Uint64

	hotMu   sync.Mutex
	hot     *trunkStore
	retired map[*trunkStore]struct{}

	validationMu             sync.Mutex
	validationGeneration     uint64
	validatedTopologyVersion uint64
	validatedForkBranches    map[string]uint64

	testAfterReadLock func()
	testDeepRepair    func()
}

type trunkStore struct {
	store   *log.Store
	heads   map[string]*hotHead
	refs    int
	retired bool
}

type hotHead struct {
	ready chan struct{}
	x     *XWAL
	err   error
	refs  int
}

var trunkRegistry = struct {
	sync.Mutex
	roots map[string]map[*Trunks]struct{}
}{roots: map[string]map[*Trunks]struct{}{}}

type rootTopologyState struct {
	mu        sync.Mutex
	ready     *sync.Cond
	mutating  bool
	borrowers int
	epoch     uint64
	owners    map[*Trunks]int
	lineages  map[string]*rootLineageState
}

type rootLineageState struct {
	mu      sync.Mutex
	ready   *sync.Cond
	writing bool
	owner   *Trunks
	heads   map[*Trunks]int
}

var rootTopologyRegistry = struct {
	sync.Mutex
	states map[string]*rootTopologyState
}{states: map[string]*rootTopologyState{}}

// topologyWaitTimeout bounds how long topology mutations wait for
// pending flushes and open heads before giving up.
var topologyWaitTimeout = 3 * time.Second

// NodeID and TrunkID are string ids (a node id is a branch dir name; a
// trunk id is "t<N>").
type (
	NodeID  = string
	TrunkID = string
)

type tnode struct {
	branch   []string
	trunk    string
	frozen   bool     // has child forks
	children []string // child node keys
	parent   string   // parent node key ("" = root's absent parent; root key is also "")
	isRoot   bool
	isStump  bool // a markerless depth-1 child of the root (cauterization boundary)
}

// stumpName is the stump's identity: its (single) branch dir name. Empty
// for non-stumps.
func (n *tnode) stumpName() string {
	if n.isStump && len(n.branch) == 1 {
		return n.branch[0]
	}
	return ""
}

const trunkMarker = ".trunk"

// ErrAtStump is returned by Promote when a trunk cannot climb further: the
// node above it is a stump (the cauterization boundary). Callers map this
// to a domain message (figaro: "cannot promote into a loadout").
var ErrAtStump = errors.New("xwal: trunk is rooted at a stump; cannot promote further")

// genesisMarker is the root node's first main entry, so the trunk is
// immediately forkable and its prefix is never empty.
const genesisMarker = `{"genesis":true}`

// mainTail returns the main channel's last index for an opened branch.
func mainTail(x *XWAL) uint64 { return x.chans[x.main].log.LastIndex() }

// CreateTrunks initializes a fresh trunk store at dir (creating the xwal
// from cfg) and seeds the genesis at the root. The root is the channel
// directory itself: it carries NO .trunk marker (it is addressed by being
// the root, not by an id) and holds the genesis every trunk inherits.
// Stumps (CreateStump) and trunks live below it.
func createTrunks(dir string, cfg Config) (*Trunks, error) {
	if cfg.Main == "" {
		return nil, fmt.Errorf("xwal: create needs cfg.Main")
	}
	endMutation, root, err := beginRootTopologyMutation(dir)
	if err != nil {
		return nil, err
	}
	defer endMutation()
	retireTrunkStores(dir)
	x, err := Open(dir, cfg)
	if err != nil {
		return nil, err
	}
	if mainTail(x) > 0 {
		x.Close()
		return nil, fmt.Errorf("xwal: trunks already initialized at %s", dir)
	}
	gen := cfg.Genesis
	if len(gen) == 0 {
		gen = []byte(genesisMarker)
	}
	glt, err := x.AppendMain(gen, nil)
	if err != nil {
		x.Close()
		return nil, err
	}
	for _, c := range x.Channels() {
		if c.Kind == ChannelReducible {
			if _, err := x.Append(c.Name, glt, []byte("{}"), nil); err != nil {
				x.Close()
				return nil, err
			}
		}
	}
	x.Close()

	t := &Trunks{root: dir, registryRoot: root, cfg: cfg, main: cfg.Main}
	if err := t.rebuild(); err != nil {
		return nil, err
	}
	t.markTopologyValidated()
	if err := t.register(); err != nil {
		return nil, err
	}
	return t, nil
}

// OpenTrunks opens an existing trunk store, rebuilding its cache from disk.
func openTrunks(dir string, cfg Config) (*Trunks, error) {
	endMutation, root, err := beginRootTopologyMutation(dir)
	if err != nil {
		return nil, err
	}
	defer endMutation()
	retireTrunkStores(dir)
	man, err := loadOrCreateManifest(dir, cfg)
	if err != nil {
		return nil, err
	}
	man, err = recoverChannelPending(dir, cfg, man)
	if err != nil {
		return nil, err
	}
	// Complete any interrupted joint fork FIRST: later repair passes open
	// channel dirs and must never trip on mid-fork state.
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
	if _, err := recoverManifestTopology(dir, cfg, man); err != nil {
		return nil, err
	}
	x, err := Open(dir, cfg)
	if err != nil {
		return nil, err
	}
	if err := x.Close(); err != nil {
		return nil, err
	}
	main, err := mainChannelName(dir)
	if err != nil {
		return nil, err
	}
	t := &Trunks{root: dir, registryRoot: root, cfg: cfg, main: main}
	if err := t.rebuild(); err != nil {
		return nil, err
	}
	t.markTopologyValidated()
	if err := t.register(); err != nil {
		return nil, err
	}
	return t, nil
}

// rebuild walks the main channel's directory tree and reconstructs the
// node + trunk cache from disk (dirs + .trunk markers). Source of truth.
// Every completed rebuild bumps the version counter so external
// consumers can probe for topology changes without a walk of their own.
func (t *Trunks) rebuild() error {
	oldVersion := t.Version()
	t.retireRootHotPreservingValidation()
	t.nodes = map[string]*tnode{}
	t.heads = map[string]string{}
	t.nodeSeq, t.trunkSeq = 0, 0
	base := filepath.Join(t.root, t.main)
	if err := t.walk(base, nil, "", true); err != nil {
		return err
	}
	newVersion := t.version.Add(1)
	t.validationMu.Lock()
	if t.validatedTopologyVersion != 0 && t.validatedTopologyVersion == oldVersion {
		t.validatedTopologyVersion = newVersion
	}
	t.validationMu.Unlock()
	return nil
}

// Version returns the current topology version. It increases (never
// resets) every time the in-memory index is rebuilt from disk, which
// happens after every mutating public call (Fork, ForkAt, Promote,
// Remove, SpawnChild, CreateStump…) and at Open / Refresh time.
//
// Consumers cache derived state (e.g. head-of-trunk lookups, node
// listings) against the version they last observed; if Version()
// changes, the cache is stale. Cheap: one atomic load, no lock.
func (t *Trunks) Version() uint64 { return t.version.Load() }

// Refresh re-scans the on-disk trunk marker layout and re-derives the
// in-memory index. In single-process land it is redundant — every
// mutating call already rebuilds — but it is the escape hatch for a
// future cross-process story where another writer may have relabeled
// markers under our feet. Bumps Version() by one if anything changes
// (and even if nothing did, because rebuild is unconditional).
func (t *Trunks) Refresh() error {
	endMutation, err := t.beginTopologyMutation()
	if err != nil {
		return err
	}
	defer endMutation()
	t.invalidateTopologyValidation()
	return t.rebuild()
}

func (t *Trunks) walk(dir string, branch []string, parentKey string, isRoot bool) error {
	key := strings.Join(branch, "/")
	trunkID, _ := readTrunkID(dir)
	ents, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	var kids []string
	for _, e := range ents {
		if e.IsDir() && !strings.HasPrefix(e.Name(), ".") {
			kids = append(kids, e.Name())
		}
	}
	// Node class is positional + marker-based: the root is the channel dir
	// itself (depth 0, no marker); a stump is a markerless depth-1 child of
	// the root (named <loadout>@<hash>); everything with a .trunk marker is a
	// trunk. A markerless node deeper than depth-1 is treated as part of its
	// parent's trunk's plumbing (shouldn't arise in the new layout).
	n := &tnode{
		branch: append([]string(nil), branch...),
		trunk:  trunkID, frozen: len(kids) > 0, parent: parentKey, isRoot: isRoot,
		isStump: !isRoot && trunkID == "" && len(branch) == 1,
	}
	for _, k := range kids {
		n.children = append(n.children, joinKey(branch, k))
	}
	t.nodes[key] = n
	t.bumpSeqs(branch, trunkID)
	if !n.frozen && trunkID != "" {
		if previous, exists := t.heads[trunkID]; exists {
			return fmt.Errorf("xwal: trunk %q has multiple live heads %q and %q", trunkID, previous, key)
		}
		t.heads[trunkID] = key
	}
	for _, k := range kids {
		if err := t.walk(filepath.Join(dir, k), append(append([]string(nil), branch...), k), key, false); err != nil {
			return err
		}
	}
	return nil
}

func (t *Trunks) bumpSeqs(branch []string, trunkID string) {
	if len(branch) > 0 {
		if n := numSuffix(branch[len(branch)-1], 'n'); n+1 > t.nodeSeq {
			t.nodeSeq = n + 1
		}
	}
	if n := numSuffix(trunkID, 't'); n+1 > t.trunkSeq {
		t.trunkSeq = n + 1
	}
}

// --- accessors ---

func (t *Trunks) headBranch(trunk string) ([]string, error) {
	key, ok := t.heads[trunk]
	if !ok {
		return nil, fmt.Errorf("xwal: unknown trunk %q", trunk)
	}
	return t.nodes[key].branch, nil
}

func (t *Trunks) rootLineage(trunk string) *rootLineageState {
	state := rootTopologyStateFor(t.registryRoot)
	state.mu.Lock()
	defer state.mu.Unlock()
	lineage := state.lineages[trunk]
	if lineage == nil {
		lineage = &rootLineageState{heads: map[*Trunks]int{}}
		lineage.ready = sync.NewCond(&lineage.mu)
		state.lineages[trunk] = lineage
	}
	return lineage
}

func (t *Trunks) adoptLineage(lineage *rootLineageState) {
	if lineage.owner == nil {
		lineage.owner = t
	} else if lineage.owner != t {
		// The previous owner's buffered appends must reach disk before this
		// peer opens the lineage from disk.
		_ = lineage.owner.flushHot()
		t.retireHot()
		lineage.owner = t
	}
}

func foreignLineageHeads(lineage *rootLineageState, owner *Trunks) bool {
	for peer, count := range lineage.heads {
		if peer != owner && count != 0 {
			return true
		}
	}
	return false
}

func (t *Trunks) lockLineage(trunk string) func() {
	lineage := t.rootLineage(trunk)
	lineage.mu.Lock()
	for lineage.writing || foreignLineageHeads(lineage, t) {
		lineage.ready.Wait()
	}
	lineage.writing = true
	t.adoptLineage(lineage)
	lineage.mu.Unlock()
	return func() {
		lineage.mu.Lock()
		lineage.writing = false
		lineage.ready.Broadcast()
		lineage.mu.Unlock()
	}
}

func (t *Trunks) holdLineageHead(trunk string) func() {
	lineage := t.rootLineage(trunk)
	lineage.mu.Lock()
	for lineage.writing || foreignLineageHeads(lineage, t) {
		lineage.ready.Wait()
	}
	t.adoptLineage(lineage)
	lineage.heads[t]++
	lineage.mu.Unlock()
	return func() {
		lineage.mu.Lock()
		lineage.heads[t]--
		if lineage.heads[t] == 0 {
			delete(lineage.heads, t)
		}
		lineage.ready.Broadcast()
		lineage.mu.Unlock()
	}
}

func (t *Trunks) openHeadRefs() int {
	t.hotMu.Lock()
	defer t.hotMu.Unlock()
	refs := 0
	if t.hot != nil {
		refs += t.hot.refs
	}
	for h := range t.retired {
		refs += h.refs
	}
	return refs
}

func (t *Trunks) ensureNoOpenHeads() error {
	deadline := time.Now().Add(topologyWaitTimeout)
	for {
		refs := t.openHeadRefs()
		if refs == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("xwal: topology mutation timed out waiting for %d open head(s)", refs)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func (t *Trunks) borrowHotUntracked(branch []string) (*XWAL, func() error, error) {
	t.hotMu.Lock()
	h := t.hot
	if h == nil {
		h = &trunkStore{store: log.NewStore(), heads: map[string]*hotHead{}}
		t.hot = h
	}
	h.refs++
	key := strings.Join(branch, "\x00")
	head := h.heads[key]
	creator := head == nil
	if creator {
		head = &hotHead{ready: make(chan struct{})}
		h.heads[key] = head
	}
	head.refs++
	t.hotMu.Unlock()

	if creator {
		head.x, head.err = open(t.root, t.cfg, h.store, branch...)
		t.hotMu.Lock()
		if head.err != nil && h.heads[key] == head {
			delete(h.heads, key)
		}
		close(head.ready)
		t.hotMu.Unlock()
	} else {
		<-head.ready
	}
	if head.err != nil {
		t.hotMu.Lock()
		head.refs--
		t.hotMu.Unlock()
		_ = t.releaseHot(h)
		return nil, nil, head.err
	}

	var once sync.Once
	var releaseErr error
	release := func() error {
		once.Do(func() {
			t.hotMu.Lock()
			head.refs--
			t.hotMu.Unlock()
			releaseErr = t.releaseHot(h)
		})
		return releaseErr
	}
	return head.x, release, nil
}

// evictLineage unloads a trunk's hot head: buffered entries are flushed
// under the lineage lock (appenders excluded), then the head and its
// channel snapshots are dropped; the next touch reloads from disk. A
// head still borrowed is skipped. Returns whether the lineage is now
// unloaded.
func (t *Trunks) evictLineage(trunk string) (bool, error) {
	unlock := t.lockLineage(trunk)
	defer unlock()
	t.mu.RLock()
	headKey, ok := t.heads[trunk]
	var branch []string
	if ok {
		branch = t.nodes[headKey].branch
	}
	t.mu.RUnlock()
	if !ok {
		return true, nil
	}
	key := strings.Join(branch, "\x00")

	t.hotMu.Lock()
	h := t.hot
	if h == nil {
		t.hotMu.Unlock()
		return true, nil
	}
	head := h.heads[key]
	if head == nil {
		t.hotMu.Unlock()
		return true, nil
	}
	if head.refs != 0 || head.x == nil || head.err != nil {
		t.hotMu.Unlock()
		return false, nil
	}
	t.hotMu.Unlock()

	for _, name := range head.x.order {
		if err := head.x.chans[name].log.Flush(); err != nil {
			return false, err
		}
	}

	t.hotMu.Lock()
	if t.hot != h || h.heads[key] != head || head.refs != 0 {
		t.hotMu.Unlock()
		return false, nil
	}
	delete(h.heads, key)
	inUse := map[string]bool{}
	for _, other := range h.heads {
		if other.x == nil {
			continue
		}
		for _, ch := range other.x.chans {
			inUse[ch.dir] = true
		}
	}
	t.hotMu.Unlock()

	for _, name := range head.x.order {
		ch := head.x.chans[name]
		if inUse[ch.dir] {
			continue
		}
		if err := h.store.Evict(ch.dir); err != nil {
			return true, err
		}
	}
	return true, nil
}

func (t *Trunks) openHotTopology(branch []string) (*XWAL, error) {
	x, release, err := t.borrowHotUntracked(branch)
	if err != nil {
		return nil, err
	}
	return x.sharedView(release, nil, t.retireRootHotPreservingValidation), nil
}

func rootTopologyStateFor(root string) *rootTopologyState {
	rootTopologyRegistry.Lock()
	defer rootTopologyRegistry.Unlock()
	state := rootTopologyRegistry.states[root]
	if state == nil {
		state = &rootTopologyState{
			owners:   map[*Trunks]int{},
			lineages: map[string]*rootLineageState{},
		}
		state.ready = sync.NewCond(&state.mu)
		rootTopologyRegistry.states[root] = state
	}
	return state
}

func canonicalRoot(root string) (string, error) {
	abs, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	return filepath.Clean(abs), nil
}

func beginRootTopologyMutation(root string) (func(), string, error) {
	return beginRootTopologyMutationFor(root, nil)
}

var errLocalTopologyBorrowers = errors.New("xwal: local topology borrowers")

func beginRootTopologyMutationFor(root string, owner *Trunks) (func(), string, error) {
	root, err := canonicalRoot(root)
	if err != nil {
		return nil, "", err
	}
	state := rootTopologyStateFor(root)
	deadline := time.Now().Add(topologyWaitTimeout)
	state.mu.Lock()
	for {
		for state.mutating {
			state.ready.Wait()
		}
		if state.borrowers == 0 {
			break
		}
		borrowers := state.borrowers
		local := state.owners[owner]
		if owner != nil && local == borrowers {
			state.mu.Unlock()
			return nil, "", errLocalTopologyBorrowers
		}
		if time.Now().After(deadline) {
			state.mu.Unlock()
			return nil, "", fmt.Errorf("xwal: topology mutation timed out waiting for %d open head(s)", borrowers)
		}
		state.mu.Unlock()
		time.Sleep(2 * time.Millisecond)
		state.mu.Lock()
	}
	state.mutating = true
	state.mu.Unlock()
	return func() {
		state.mu.Lock()
		state.mutating = false
		state.epoch++
		if owner != nil {
			owner.rootEpoch.Store(state.epoch)
		}
		state.ready.Broadcast()
		state.mu.Unlock()
	}, root, nil
}

func beginRootAdditiveMutation(root string) (func(), error) {
	root, err := canonicalRoot(root)
	if err != nil {
		return nil, err
	}
	state := rootTopologyStateFor(root)
	state.mu.Lock()
	for state.mutating {
		state.ready.Wait()
	}
	state.mutating = true
	state.mu.Unlock()
	return func() {
		state.mu.Lock()
		state.mutating = false
		state.ready.Broadcast()
		state.mu.Unlock()
	}, nil
}

func beginRootBorrow(root string, owner *Trunks) error {
	state := rootTopologyStateFor(root)
	state.mu.Lock()
	defer state.mu.Unlock()
	for state.mutating {
		state.ready.Wait()
	}
	state.borrowers++
	state.owners[owner]++
	return nil
}

func endRootBorrow(root string, owner *Trunks) {
	state := rootTopologyStateFor(root)
	state.mu.Lock()
	state.borrowers--
	state.owners[owner]--
	if state.owners[owner] == 0 {
		delete(state.owners, owner)
	}
	state.ready.Broadcast()
	state.mu.Unlock()
}

func transferRootBorrow(root string, from, to *Trunks) {
	state := rootTopologyStateFor(root)
	state.mu.Lock()
	state.owners[from]--
	if state.owners[from] == 0 {
		delete(state.owners, from)
	}
	state.owners[to]++
	state.ready.Broadcast()
	state.mu.Unlock()
}

func waitRootBorrowers(root string, owner *Trunks) error {
	state := rootTopologyStateFor(root)
	deadline := time.Now().Add(topologyWaitTimeout)
	state.mu.Lock()
	for state.owners[owner] != 0 {
		if time.Now().After(deadline) {
			n := state.owners[owner]
			state.mu.Unlock()
			return fmt.Errorf("xwal: topology mutation timed out waiting for %d open head(s)", n)
		}
		state.mu.Unlock()
		time.Sleep(2 * time.Millisecond)
		state.mu.Lock()
	}
	state.mu.Unlock()
	return nil
}

func rootTopologyEpoch(root string) uint64 {
	state := rootTopologyStateFor(root)
	state.mu.Lock()
	defer state.mu.Unlock()
	return state.epoch
}

func (t *Trunks) ensureCurrentTopology() error {
	epoch := rootTopologyEpoch(t.registryRoot)
	if t.rootEpoch.Load() == epoch {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	epoch = rootTopologyEpoch(t.registryRoot)
	if t.rootEpoch.Load() == epoch {
		return nil
	}
	if err := t.rebuild(); err != nil {
		return err
	}
	t.rootEpoch.Store(epoch)
	return nil
}

func (t *Trunks) beginTrackedRead() (func(), error) {
	if err := beginRootBorrow(t.registryRoot, t); err != nil {
		return nil, err
	}
	if err := t.ensureCurrentTopology(); err != nil {
		endRootBorrow(t.registryRoot, t)
		return nil, err
	}
	t.mu.RLock()
	if t.testAfterReadLock != nil {
		t.testAfterReadLock()
	}
	return func() {
		endRootBorrow(t.registryRoot, t)
		t.mu.RUnlock()
	}, nil
}

func (t *Trunks) beginTopologyMutation() (func(), error) {
	for {
		t.mu.Lock()
		end, _, err := beginRootTopologyMutationFor(t.registryRoot, t)
		if errors.Is(err, errLocalTopologyBorrowers) {
			if openErr := t.ensureNoOpenHeads(); openErr != nil {
				t.mu.Unlock()
				return nil, openErr
			}
			t.mu.Unlock()
			if waitErr := waitRootBorrowers(t.registryRoot, t); waitErr != nil {
				return nil, waitErr
			}
			continue
		}
		if err != nil {
			t.mu.Unlock()
			return nil, err
		}
		if err := t.rebuild(); err != nil {
			end()
			t.mu.Unlock()
			return nil, err
		}
		return func() {
			end()
			t.mu.Unlock()
		}, nil
	}
}

func (t *Trunks) releaseHot(h *trunkStore) error {
	t.hotMu.Lock()
	h.refs--
	closeStore := h.retired && h.refs == 0
	if closeStore {
		delete(t.retired, h)
	}
	t.hotMu.Unlock()
	if closeStore {
		return h.store.Close()
	}
	return nil
}

func (t *Trunks) flushHot() error {
	t.hotMu.Lock()
	h := t.hot
	if h != nil {
		h.refs++
	}
	t.hotMu.Unlock()
	if h == nil {
		return nil
	}
	err := h.store.FlushAll()
	if rerr := t.releaseHot(h); rerr != nil && err == nil {
		err = rerr
	}
	return err
}

func (t *Trunks) retireHot() {
	t.hotMu.Lock()
	h := t.hot
	t.hot = nil
	if h != nil {
		h.retired = true
		if h.refs != 0 {
			if t.retired == nil {
				t.retired = map[*trunkStore]struct{}{}
			}
			t.retired[h] = struct{}{}
		}
	}
	closeStore := h != nil && h.refs == 0
	t.hotMu.Unlock()
	if closeStore {
		_ = h.store.Close()
	}
}

func (t *Trunks) register() error {
	if t.registryRoot == "" {
		root, err := canonicalRoot(t.root)
		if err != nil {
			return err
		}
		t.registryRoot = root
	}
	trunkRegistry.Lock()
	peers := trunkRegistry.roots[t.registryRoot]
	if peers == nil {
		peers = map[*Trunks]struct{}{}
		trunkRegistry.roots[t.registryRoot] = peers
	}
	peers[t] = struct{}{}
	trunkRegistry.Unlock()
	t.rootEpoch.Store(rootTopologyEpoch(t.registryRoot))
	return nil
}

func (t *Trunks) retireRootHot() {
	invalidateRootTopologyValidation(t.registryRoot)
	retireTrunkStores(t.root)
}

func (t *Trunks) retireRootHotPreservingValidation() {
	retireTrunkStores(t.root)
}

func invalidateRootTopologyValidation(root string) {
	trunkRegistry.Lock()
	peers := make([]*Trunks, 0, len(trunkRegistry.roots[root]))
	for peer := range trunkRegistry.roots[root] {
		peers = append(peers, peer)
	}
	trunkRegistry.Unlock()
	for _, peer := range peers {
		peer.invalidateTopologyValidation()
	}
}

func (t *Trunks) invalidateTopologyValidation() {
	t.validationMu.Lock()
	t.validationGeneration++
	t.validatedTopologyVersion = 0
	t.validatedForkBranches = nil
	t.validationMu.Unlock()
}

func (t *Trunks) markTopologyValidated() {
	t.validationMu.Lock()
	if t.validationGeneration == 0 {
		t.validationGeneration = 1
	}
	t.validatedTopologyVersion = t.Version()
	t.validatedForkBranches = nil
	t.validationMu.Unlock()
}

func (t *Trunks) forkPreflightValidated(branch []string) bool {
	key := strings.Join(branch, "/")
	t.validationMu.Lock()
	defer t.validationMu.Unlock()
	if t.validatedTopologyVersion == t.Version() {
		return true
	}
	return t.validatedForkBranches[key] == t.validationGeneration
}

func (t *Trunks) markForkPreflightValidated(branch []string) {
	key := strings.Join(branch, "/")
	t.validationMu.Lock()
	if t.validationGeneration == 0 {
		t.validationGeneration = 1
	}
	if t.validatedForkBranches == nil {
		t.validatedForkBranches = make(map[string]uint64)
	}
	t.validatedForkBranches[key] = t.validationGeneration
	t.validationMu.Unlock()
}

func (t *Trunks) markForkResultValidated(parent []string, children ...string) {
	t.markForkPreflightValidated(parent)
	for _, child := range children {
		if child == "" {
			continue
		}
		branch := append(append([]string(nil), parent...), child)
		t.markForkPreflightValidated(branch)
	}
}

func (t *Trunks) openForkSource(branch []string) (*XWAL, error) {
	t.retireRootHotPreservingValidation()
	validated := t.forkPreflightValidated(branch)
	if validated {
		complete, err := forkTopologyStructurallyComplete(t.root, t.cfg, branch)
		if err != nil {
			return nil, err
		}
		validated = complete
	}
	if !validated {
		if t.testDeepRepair != nil {
			t.testDeepRepair()
		}
		if err := repairBranchChannels(t.root, t.cfg, branch); err != nil {
			return nil, err
		}
		if err := repairRehomeDescendants(t.root, t.cfg, branch); err != nil {
			return nil, err
		}
		t.markForkPreflightValidated(branch)
	}
	return Open(t.root, t.cfg, branch...)
}

func retireTrunkStores(root string) {
	abs, err := filepath.Abs(root)
	if err != nil {
		return
	}
	registryRoot := filepath.Clean(abs)
	trunkRegistry.Lock()
	peers := make([]*Trunks, 0, len(trunkRegistry.roots[registryRoot]))
	for peer := range trunkRegistry.roots[registryRoot] {
		peers = append(peers, peer)
	}
	trunkRegistry.Unlock()
	if len(peers) == 0 {
		return
	}
	for _, peer := range peers {
		peer.retireHot()
	}
}

// Close releases cached segment handles. Any Head or StumpHead handles must
// be closed first. The topology cache remains usable and opens a fresh
// segment generation on the next disk-backed operation.
func (t *Trunks) Close() error {
	t.hotMu.Lock()
	refs := 0
	if t.hot != nil {
		refs += t.hot.refs
	}
	for h := range t.retired {
		refs += h.refs
	}
	if refs != 0 {
		t.hotMu.Unlock()
		return fmt.Errorf("xwal: close trunks with %d open head(s)", refs)
	}
	stores := make([]*trunkStore, 0, 1+len(t.retired))
	if t.hot != nil {
		t.hot.retired = true
		stores = append(stores, t.hot)
	}
	for h := range t.retired {
		stores = append(stores, h)
	}
	t.hot = nil
	t.retired = nil
	t.hotMu.Unlock()

	trunkRegistry.Lock()
	if peers := trunkRegistry.roots[t.registryRoot]; peers != nil {
		delete(peers, t)
		if len(peers) == 0 {
			delete(trunkRegistry.roots, t.registryRoot)
		}
	}
	trunkRegistry.Unlock()
	var errs []error
	for _, h := range stores {
		if err := h.store.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// EnsureChannel adds and backfills a channel if needed, and installs its
// runtime policy for subsequent hot heads and topology operations.
func (t *Trunks) ensureChannel(spec ChannelSpec) error {
	endMutation, err := t.beginTopologyMutation()
	if err != nil {
		return err
	}
	defer endMutation()

	man, err := loadOrCreateManifest(t.root, t.cfg)
	if err != nil {
		return err
	}
	man, err = recoverChannelPending(t.root, t.cfg, man)
	if err != nil {
		return err
	}
	if err := validateChannelSpec(t.root, t.cfg, man, spec); err != nil {
		return err
	}
	if err := t.ensureNoOpenHeads(); err != nil {
		return err
	}

	cfg := withChannelSpec(t.cfg, spec)
	t.retireRootHotPreservingValidation()
	pending := channelPendingPlan{Channel: manifestChannel{
		Name: spec.Name, Kind: spec.Kind.String(), Reducer: spec.Reducer, Opaque: spec.Opaque,
	}}
	if err := writeChannelPending(t.root, pending); err != nil {
		return err
	}
	if _, err := recoverChannelPending(t.root, cfg, man); err != nil {
		return err
	}

	t.cfg = cfg
	return t.rebuild()
}

// Head opens the live head node of a trunk. Caller closes it.
func (t *Trunks) Head(trunk string) (*XWAL, error) {
	releaseLineage := t.holdLineageHead(trunk)
	if err := beginRootBorrow(t.registryRoot, t); err != nil {
		releaseLineage()
		return nil, err
	}
	if err := t.ensureCurrentTopology(); err != nil {
		endRootBorrow(t.registryRoot, t)
		releaseLineage()
		return nil, err
	}
	t.mu.RLock()
	branch, err := t.headBranch(trunk)
	if err != nil {
		t.mu.RUnlock()
		endRootBorrow(t.registryRoot, t)
		releaseLineage()
		return nil, err
	}
	x, release, err := t.borrowHotUntracked(branch)
	t.mu.RUnlock()
	if err != nil {
		endRootBorrow(t.registryRoot, t)
		releaseLineage()
		return nil, err
	}
	view := x.sharedView(release, func() {
		endRootBorrow(t.registryRoot, t)
	}, t.retireRootHotPreservingValidation)
	view.releaseLineage = releaseLineage
	view.borrowRoot = t.registryRoot
	view.borrowOwner = t
	return view, nil
}

// Append adds a main-timeline entry at the trunk's tail. atMainLT is
// ignored — appends never fork; ForkAt is the only forking path.
func (t *Trunks) Append(trunk string, atMainLT uint64, payload, meta []byte) (string, uint64, error) {
	_ = atMainLT
	unlockLineage := t.lockLineage(trunk)
	defer unlockLineage()

	endRead, err := t.beginTrackedRead()
	if err != nil {
		return "", 0, err
	}
	defer endRead()
	branch, err := t.headBranch(trunk)
	if err != nil {
		return "", 0, err
	}
	x, release, err := t.borrowHotUntracked(branch)
	if err != nil {
		return "", 0, err
	}
	lt, appendErr := x.AppendMain(payload, meta)
	_ = release()
	if appendErr != nil {
		return "", 0, appendErr
	}
	return trunk, lt, nil
}

// resplitBelow forks the ancestor node along `branch` that OWNS atMainLT (a
// frozen branch point) at atMainLT+1, minting a new alternative trunk that
// shares [1..atMainLT]. The owner's original timeline beyond atMainLT — its
// suffix and ALL its child forks (including the caller's own trunk) — re-homes
// into the continuation (which keeps the owner's trunk id, the normal
// continuation-chain behavior). Caller holds t.mu. This is how a fork at an
// inherited LT (a turn shared with ancestors) produces a sibling branch.
func (t *Trunks) resplitBelow(branch []string, atMainLT uint64) (string, error) {
	ownerBranch, err := t.ownerOf(branch, atMainLT)
	if err != nil {
		return "", err
	}
	altDir := t.mintNode()
	contDir := t.mintNode()
	altTrunk := t.mintTrunk()
	owner := t.nodes[strings.Join(ownerBranch, "/")]
	sourceTrunk := ""
	if owner != nil {
		sourceTrunk = owner.trunk
	}
	ox, err := t.openForkSource(ownerBranch)
	if err != nil {
		return "", err
	}
	// child = alt; old-future = cont (re-homes children).
	child, ferr := ox.forkJoint(atMainLT+1, altDir, contDir,
		&forkCommit{SourceTrunk: sourceTrunk, ChildTrunk: altTrunk})
	ox.Close()
	if ferr != nil {
		return "", fmt.Errorf("re-split-below: %w", ferr)
	}
	t.markForkResultValidated(ownerBranch, altDir, contDir)
	child.Close()
	return altTrunk, t.rebuild()
}

// ownerOf returns the branch of the deepest node along `branch` whose own
// segments contain atMainLT (the deepest with forkBase <= atMainLT). For an
// atMainLT below the head's own range this is a strict ancestor — the
// re-split-below target.
func (t *Trunks) ownerOf(branch []string, atMainLT uint64) ([]string, error) {
	owner := []string(nil) // root owns [1..]
	for i := 1; i <= len(branch); i++ {
		sub := branch[:i]
		fb, err := t.readForkBase(sub)
		if err != nil {
			return nil, err
		}
		if fb <= atMainLT {
			owner = append([]string(nil), sub...)
		} else {
			break
		}
	}
	return owner, nil
}

func (t *Trunks) readForkBase(branch []string) (uint64, error) {
	b, err := os.ReadFile(filepath.Join(t.irDir(branch), ".fork"))
	if err != nil {
		return 0, err
	}
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		if v, ok := strings.CutPrefix(strings.TrimSpace(line), "base="); ok {
			return strconv.ParseUint(strings.TrimSpace(v), 10, 64)
		}
	}
	return 0, fmt.Errorf("xwal: malformed fork marker for %q", strings.Join(branch, "/"))
}

// ForkTail bisects a trunk's present.
//   - head has content: freeze it; the continuation keeps the trunk (new
//     empty head) and a new alternative trunk is founded.
//   - head is empty: it has no LT of its own, so redirect — N-ary fork the
//     PARENT, adding one new alternative sibling trunk; the trunk keeps its
//     empty head untouched.
func (t *Trunks) ForkTail(trunk string) (string, error) {
	unlockLineage := t.lockLineage(trunk)
	defer unlockLineage()
	endMutation, err := t.beginTopologyMutation()
	if err != nil {
		return "", err
	}
	defer endMutation()
	return t.forkTailLocked(trunk)
}

func (t *Trunks) forkTailLocked(trunk string) (string, error) {
	if err := t.ensureNoOpenHeads(); err != nil {
		return "", err
	}
	headKey, ok := t.heads[trunk]
	if !ok {
		return "", fmt.Errorf("xwal: unknown trunk %q", trunk)
	}
	head := t.nodes[headKey]
	x, err := t.openHotTopology(head.branch)
	if err != nil {
		return "", err
	}
	tail := mainTail(x)
	empty := isEmptyHead(x)
	fb := mainForkBase(x)
	x.Close()
	t.retireRootHotPreservingValidation()

	if empty {
		// Redirect to an N-ary fork of the parent at the head's fork point.
		if head.isRoot {
			return "", fmt.Errorf("xwal: cannot fork an empty root trunk")
		}
		pbranch := t.nodes[head.parent].branch
		altDir := t.mintNode()
		altTrunk := t.mintTrunk()
		px, err := t.openForkSource(pbranch)
		if err != nil {
			return "", err
		}
		// N-ary add-one at the parent's fork point (no old-future).
		altX, ferr := px.forkJoint(fb, altDir, "", &forkCommit{ChildTrunk: altTrunk})
		px.Close()
		if ferr != nil {
			return "", fmt.Errorf("fork-tail (empty head, via parent): %w", ferr)
		}
		t.markForkResultValidated(pbranch, altDir)
		altX.Close()
		return altTrunk, t.rebuild()
	}

	// Head has content: freeze it; one fork creates both children at tail+1
	// (alt = child, cont = old-future) — both empty, inheriting the full
	// prefix in every channel (always-materialize → write isolation).
	altDir := t.mintNode()
	contDir := t.mintNode()
	altTrunk := t.mintTrunk()
	fx, err := t.openForkSource(head.branch)
	if err != nil {
		return "", err
	}
	child, ferr := fx.forkJoint(tail+1, altDir, contDir,
		&forkCommit{SourceTrunk: head.trunk, ChildTrunk: altTrunk})
	fx.Close()
	if ferr != nil {
		return "", fmt.Errorf("fork-tail: %w", ferr)
	}
	t.markForkResultValidated(head.branch, altDir, contDir)
	child.Close()
	return altTrunk, t.rebuild()
}

// ForkAt forks a trunk at an interior main-LT WITHOUT appending: it shares
// [1..atMainLT] and creates an EMPTY alternative trunk diverging at
// atMainLT+1; the original trunk keeps its id and its suffix. At or past the
// tail it degenerates to a tail fork (ForkTail). Returns the new alternative
// trunk. (Append does fork+send in one; ForkAt is the imperative-only fork.)
func (t *Trunks) ForkAt(trunk string, atMainLT uint64) (string, error) {
	unlockLineage := t.lockLineage(trunk)
	defer unlockLineage()
	endMutation, err := t.beginTopologyMutation()
	if err != nil {
		return "", err
	}
	defer endMutation()
	if err := t.ensureNoOpenHeads(); err != nil {
		return "", err
	}
	branch, err := t.headBranch(trunk)
	if err != nil {
		return "", err
	}
	x, err := t.openHotTopology(branch)
	if err != nil {
		return "", err
	}
	tail := mainTail(x)
	ownFirst := ownFirstIdx(x)
	x.Close()

	if atMainLT == 0 || atMainLT >= tail {
		return t.forkTailLocked(trunk)
	}
	if atMainLT < ownFirst {
		// Re-split-below: fork the ancestor that owns atMainLT.
		t.retireRootHotPreservingValidation()
		return t.resplitBelow(branch, atMainLT)
	}

	altDir := t.mintNode()
	contDir := t.mintNode()
	altTrunk := t.mintTrunk()
	fx, err := t.openForkSource(branch)
	if err != nil {
		return "", err
	}
	// child = alt; old-future = cont.
	child, ferr := fx.forkJoint(atMainLT+1, altDir, contDir,
		&forkCommit{SourceTrunk: trunk, ChildTrunk: altTrunk})
	fx.Close()
	if ferr != nil {
		return "", ferr
	}
	t.markForkResultValidated(branch, altDir, contDir)
	child.Close()
	return altTrunk, t.rebuild()
}

// Remove deletes a trunk: its founding node's entire subtree, in every
// channel. Trunk-addressed (a node is plumbing). Refuses the root trunk, and
// refuses a trunk that has live branches (descendant trunks branched off it)
// unless recursive — in which case those branches go too.
func (t *Trunks) Remove(trunk string, recursive bool) error {
	endMutation, err := t.beginTopologyMutation()
	if err != nil {
		return err
	}
	defer endMutation()
	if err := t.ensureNoOpenHeads(); err != nil {
		return err
	}
	t.retireRootHotPreservingValidation()

	// The founding node is the shallowest node carrying this trunk (its
	// parent is in another trunk, or it is the root).
	foundKey, ok := "", false
	for key, n := range t.nodes {
		if n.trunk != trunk {
			continue
		}
		if n.isRoot {
			return fmt.Errorf("xwal: cannot remove the root trunk %q", trunk)
		}
		if p := t.nodes[n.parent]; p == nil || p.trunk != trunk {
			foundKey, ok = key, true
			break
		}
	}
	if !ok {
		return fmt.Errorf("xwal: unknown trunk %q", trunk)
	}

	// Collect the trunks living in the founding node's subtree.
	sub := map[string]bool{}
	var walk func(string)
	walk = func(key string) {
		n := t.nodes[key]
		if n == nil {
			return
		}
		if n.trunk != "" {
			sub[n.trunk] = true
		}
		for _, c := range n.children {
			walk(c)
		}
	}
	walk(foundKey)
	delete(sub, trunk)
	if len(sub) > 0 && !recursive {
		return fmt.Errorf("xwal: trunk %q has %d live branch(es); remove recursively to take them too", trunk, len(sub))
	}

	// Delete the founding node's subtree dir in every channel.
	branch := t.nodes[foundKey].branch
	names, err := channelNames(t.root)
	if err != nil {
		return err
	}
	for _, ch := range names {
		if err := os.RemoveAll(filepath.Join(append([]string{t.root, ch}, branch...)...)); err != nil {
			return err
		}
	}
	return t.rebuild()
}

// SpawnChild adds a new child trunk under a "ceremonial" parent trunk
// WITHOUT a continuation: the parent's node becomes (or stays) a frozen
// branch point that only hosts children. (ForkTail, by contrast, gives the
// parent a continuation.) Returns the new child trunk id.
//
// In the root/stumps layout the ceremonial parents are the root and the
// stumps, addressed with SpawnUnderRoot / SpawnUnderStump. SpawnChild
// remains for spawning a fresh child under an existing trunk.
func (t *Trunks) SpawnChild(parent TrunkID) (TrunkID, error) {
	endMutation, err := t.beginTopologyMutation()
	if err != nil {
		return "", err
	}
	defer endMutation()
	if err := t.ensureNoOpenHeads(); err != nil {
		return "", err
	}
	nodeKey, ok := t.anchorOf(parent)
	if !ok {
		return "", fmt.Errorf("xwal: unknown trunk %q", parent)
	}
	return t.spawnTrunkAt(t.nodes[nodeKey].branch)
}

// CreateStump mints a markerless, named depth-1 child of the root — the
// cauterization boundary. A stump holds its own birth content (write it via
// StumpHead) and hosts top-level trunks as children (SpawnUnderStump). It
// carries NO .trunk marker: its name + depth-1 position IS its identity
// (figaro names them <loadout>@<hash>). Idempotent callers should check
// Stumps() first; a duplicate name is an error.
func (t *Trunks) CreateStump(name string) error {
	endMutation, err := t.beginTopologyMutation()
	if err != nil {
		return err
	}
	defer endMutation()
	if err := t.ensureNoOpenHeads(); err != nil {
		return err
	}
	if name == "" || name == "." || name == ".." || strings.ContainsAny(name, `/\`) || strings.HasPrefix(name, ".") {
		return fmt.Errorf("xwal: invalid stump name %q", name)
	}
	if n := t.nodes[name]; n != nil {
		return fmt.Errorf("xwal: stump %q already exists", name)
	}
	// Fork the root at its tail (N-ary add-one), naming the child `name`,
	// with no continuation and no trunk marker.
	if _, err := t.forkChild(nil, name, nil); err != nil {
		return fmt.Errorf("xwal: create-stump %q: %w", name, err)
	}
	return t.rebuild()
}

// StumpHead opens a stump's branch for appending its birth content (IR +
// related channels), before it gains trunk children. Caller closes it.
func (t *Trunks) StumpHead(name string) (*XWAL, error) {
	releaseLineage := t.holdLineageHead("stump:" + name)
	if err := beginRootBorrow(t.registryRoot, t); err != nil {
		releaseLineage()
		return nil, err
	}
	if err := t.ensureCurrentTopology(); err != nil {
		endRootBorrow(t.registryRoot, t)
		releaseLineage()
		return nil, err
	}
	t.mu.RLock()
	n := t.nodes[name]
	if n == nil || !n.isStump {
		t.mu.RUnlock()
		endRootBorrow(t.registryRoot, t)
		releaseLineage()
		return nil, fmt.Errorf("xwal: no stump %q", name)
	}
	x, release, err := t.borrowHotUntracked([]string{name})
	t.mu.RUnlock()
	if err != nil {
		endRootBorrow(t.registryRoot, t)
		releaseLineage()
		return nil, err
	}
	view := x.sharedView(release, func() {
		endRootBorrow(t.registryRoot, t)
	}, t.retireRootHotPreservingValidation)
	view.releaseLineage = releaseLineage
	view.borrowRoot = t.registryRoot
	view.borrowOwner = t
	return view, nil
}

// SpawnUnderStump mints a new trunk (a top-level aria) as a child of a stump.
func (t *Trunks) SpawnUnderStump(name string) (TrunkID, error) {
	endMutation, err := t.beginTopologyMutation()
	if err != nil {
		return "", err
	}
	defer endMutation()
	if err := t.ensureNoOpenHeads(); err != nil {
		return "", err
	}
	n := t.nodes[name]
	if n == nil || !n.isStump {
		return "", fmt.Errorf("xwal: no stump %q", name)
	}
	return t.spawnTrunkAt(n.branch)
}

// SpawnUnderRoot mints a new trunk directly under the root (a top-level
// trunk with no stump — e.g. a loadoutless conversation).
func (t *Trunks) SpawnUnderRoot() (TrunkID, error) {
	endMutation, err := t.beginTopologyMutation()
	if err != nil {
		return "", err
	}
	defer endMutation()
	if err := t.ensureNoOpenHeads(); err != nil {
		return "", err
	}
	return t.spawnTrunkAt(nil)
}

// spawnTrunkAt forks the node at parentBranch at its tail (N-ary add-one),
// mints a fresh trunk id for the child, and writes its .trunk marker. Caller
// holds t.mu.
func (t *Trunks) spawnTrunkAt(parentBranch []string) (TrunkID, error) {
	childDir := t.mintNode()
	childTrunk := t.mintTrunk()
	if _, err := t.forkChild(parentBranch, childDir, &forkCommit{ChildTrunk: childTrunk}); err != nil {
		return "", fmt.Errorf("xwal: spawn: %w", err)
	}
	return childTrunk, t.rebuild()
}

// forkChild forks the node at parentBranch at its tail (N-ary add-one, no
// continuation), creating an empty child dir named childDir in every channel.
// Returns the child's branch. Caller holds t.mu and must rebuild afterwards.
func (t *Trunks) forkChild(parentBranch []string, childDir string, commit *forkCommit) ([]string, error) {
	x, err := t.openHotTopology(parentBranch)
	if err != nil {
		return nil, err
	}
	tail := mainTail(x)
	x.Close()
	fx, err := t.openForkSource(parentBranch)
	if err != nil {
		return nil, err
	}
	c, ferr := fx.forkJoint(tail+1, childDir, "", commit) // N-ary add-one at the tail; no continuation
	fx.Close()
	if ferr != nil {
		return nil, ferr
	}
	t.markForkResultValidated(parentBranch, childDir)
	c.Close()
	return append(append([]string(nil), parentBranch...), childDir), nil
}

// OwnerTrunk returns the trunk id of the node that OWNS atMainLT along the
// given trunk's lineage — the deepest ancestor whose own segments contain it.
// Callers layer policy on this: e.g. an LT owned by a "ceremonial" trunk (a
// null root or loadout) can be redirected to SpawnChild instead of a re-split.
func (t *Trunks) OwnerTrunk(trunk string, atMainLT uint64) (string, error) {
	endRead, err := t.beginTrackedRead()
	if err != nil {
		return "", err
	}
	defer endRead()
	branch, err := t.headBranch(trunk)
	if err != nil {
		return "", err
	}
	ob, err := t.ownerOf(branch, atMainLT)
	if err != nil {
		return "", err
	}
	n := t.nodes[strings.Join(ob, "/")]
	if n == nil {
		return "", fmt.Errorf("xwal: no owner node for main-LT %d on trunk %q", atMainLT, trunk)
	}
	return n.trunk, nil
}

// Owner describes which node owns a main-LT along a trunk's lineage: a
// trunk (Trunk set), a stump (Stump set), or the root (IsRoot). Callers
// layer policy on this — e.g. an LT owned by the root or a stump is
// ceremonial, so a "fork there" spawns a fresh child rather than re-splitting.
type Owner struct {
	Trunk  TrunkID // "" if the owner is the root or a stump
	Stump  string  // the stump name, if the owner is a stump
	IsRoot bool
}

// Owner resolves which node owns atMainLT along the given trunk's lineage.
func (t *Trunks) Owner(trunk TrunkID, atMainLT uint64) (Owner, error) {
	endRead, err := t.beginTrackedRead()
	if err != nil {
		return Owner{}, err
	}
	defer endRead()
	branch, err := t.headBranch(trunk)
	if err != nil {
		return Owner{}, err
	}
	ob, err := t.ownerOf(branch, atMainLT)
	if err != nil {
		return Owner{}, err
	}
	n := t.nodes[strings.Join(ob, "/")]
	if n == nil {
		return Owner{}, fmt.Errorf("xwal: no owner node for main-LT %d on trunk %q", atMainLT, trunk)
	}
	return Owner{Trunk: n.trunk, Stump: n.stumpName(), IsRoot: n.isRoot}, nil
}

// Promote climbs a trunk up `levels` stump-bounded levels by relabeling the
// main channel's .trunk markers: the trunk absorbs its parent trunk's run
// (the consecutive same-id ancestors above its divergence point), repeated
// once per level. No data moves — only markers are rewritten, so the other
// channels follow the unchanged node tree. The climb stops at a stump (the
// cauterization boundary): if it cannot move at all, Promote returns
// ErrAtStump; excess levels past the stump are a no-op. Returns the number
// of levels actually climbed.
func (t *Trunks) Promote(trunk TrunkID, levels int) (int, error) {
	endMutation, err := t.beginTopologyMutation()
	if err != nil {
		return 0, err
	}
	defer endMutation()
	if err := t.ensureNoOpenHeads(); err != nil {
		return 0, err
	}
	t.retireRootHotPreservingValidation()
	if levels <= 0 {
		levels = 1
	}
	foundKey, ok := t.foundingNode(trunk)
	if !ok {
		return 0, fmt.Errorf("xwal: unknown trunk %q", trunk)
	}
	climbed := 0
	for climbed < levels {
		f := t.nodes[foundKey]
		p := t.nodes[f.parent]
		if f.isRoot || p == nil || p.isRoot || p.isStump {
			break // ceremonial boundary above — cannot climb further
		}
		parentID := p.trunk
		// Walk up from p (toward the root) through the consecutive same-id run,
		// stopping when the node above has a different id (or is a stump/root).
		runTop := f.parent
		for {
			up := t.nodes[t.nodes[runTop].parent]
			if up == nil || up.isRoot || up.isStump || up.trunk != parentID {
				break
			}
			runTop = t.nodes[runTop].parent
		}
		// Relabel the run [runTop .. p] to the promoted id on disk.
		for cur := f.parent; ; cur = t.nodes[cur].parent {
			if err := writeTrunkID(t.irDir(t.nodes[cur].branch), trunk); err != nil {
				return climbed, err
			}
			if cur == runTop {
				break
			}
		}
		foundKey = runTop
		climbed++
	}
	if climbed == 0 {
		return 0, ErrAtStump
	}
	return climbed, t.rebuild()
}

// foundingNode returns the shallowest node carrying a trunk id (its parent is
// in another trunk, a stump, or the root). One exists per live trunk.
func (t *Trunks) foundingNode(trunk TrunkID) (string, bool) {
	for key, n := range t.nodes {
		if n.trunk != trunk {
			continue
		}
		if p := t.nodes[n.parent]; p == nil || p.trunk != trunk {
			return key, true
		}
	}
	return "", false
}

// StumpInfo is a read-only view of a stump.
type StumpInfo struct {
	Name     string
	Children []TrunkID // trunk ids of its immediate trunk children
}

// Stumps returns every stump, sorted by name.
func (t *Trunks) Stumps() []StumpInfo {
	endRead, err := t.beginTrackedRead()
	if err != nil {
		return nil
	}
	defer endRead()
	var out []StumpInfo
	for _, n := range t.nodes {
		if !n.isStump {
			continue
		}
		si := StumpInfo{Name: n.stumpName()}
		for _, ck := range n.children {
			if c := t.nodes[ck]; c != nil && c.trunk != "" {
				si.Children = append(si.Children, c.trunk)
			}
		}
		sort.Strings(si.Children)
		out = append(out, si)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// anchorOf returns the node where children of a trunk attach: its live
// head if it has one, else its (single, frozen) ceremonial node.
func (t *Trunks) anchorOf(trunk TrunkID) (string, bool) {
	if k, ok := t.heads[trunk]; ok {
		return k, true
	}
	for k, n := range t.nodes {
		if n.trunk == trunk {
			return k, true
		}
	}
	return "", false
}

// AppendChannel appends to a related channel of a trunk's head.
func (t *Trunks) AppendChannel(trunk, channel string, mainLT uint64, payload, meta []byte) (uint64, error) {
	unlockLineage := t.lockLineage(trunk)
	defer unlockLineage()
	endRead, err := t.beginTrackedRead()
	if err != nil {
		return 0, err
	}
	defer endRead()
	branch, err := t.headBranch(trunk)
	if err != nil {
		return 0, err
	}
	x, release, err := t.borrowHotUntracked(branch)
	if err != nil {
		return 0, err
	}
	defer release()
	if mainLT == 0 {
		mainLT = mainTail(x) + 1 // reducible default: one ahead
	}
	return x.Append(channel, mainLT, payload, meta)
}

// LatestChannelRecord reads the newest channel checkpoint from the hot
// immutable snapshot when it is at or beyond minMainLT.
func (t *Trunks) LatestChannelRecord(trunk, channel string, minMainLT uint64) (Record, bool, error) {
	unlockLineage := t.lockLineage(trunk)
	defer unlockLineage()
	endRead, err := t.beginTrackedRead()
	if err != nil {
		return Record{}, false, err
	}
	defer endRead()
	branch, err := t.headBranch(trunk)
	if err != nil {
		return Record{}, false, err
	}
	x, release, err := t.borrowHotUntracked(branch)
	if err != nil {
		return Record{}, false, err
	}
	defer release()
	return x.LatestChannelRecord(channel, minMainLT)
}

// --- listing ---

// TrunkInfo is a read-only view of a trunk.
type TrunkInfo struct {
	ID         string
	Head       []string // head node branch
	Parent     string   // parent trunk id ("" if rooted at a stump or the root)
	Stump      string   // stump name, if the trunk is rooted directly at a stump
	BranchedLT uint64
	Tip        uint64
}

// List returns every trunk, in id order.
func (t *Trunks) List() []TrunkInfo {
	endRead, err := t.beginTrackedRead()
	if err != nil {
		return nil
	}
	defer endRead()
	ids := t.orderedTrunkIDsLocked()
	out := make([]TrunkInfo, 0, len(ids))
	for _, id := range ids {
		unlockLineage := t.lockLineage(id)
		key := t.heads[id]
		ti := TrunkInfo{ID: id, Head: t.nodes[key].branch}
		ti.Parent, ti.Stump, ti.BranchedLT = t.lineage(id)
		if x, err := t.openHotTopology(t.nodes[key].branch); err == nil {
			ti.Tip = mainTail(x)
			x.Close()
		}
		unlockLineage()
		out = append(out, ti)
	}
	return out
}

// lineage finds a trunk's founding node (shallowest node carrying the
// trunk), returning its parent trunk id, the stump it is rooted at (if its
// parent is a stump), and the LT it branched at.
func (t *Trunks) lineage(trunk string) (parent, stump string, bl uint64) {
	n := t.nodes[t.heads[trunk]]
	for {
		if n.isRoot {
			return "", "", 0
		}
		p := t.nodes[n.parent]
		if p == nil {
			return "", "", 0
		}
		if p.trunk != trunk {
			// n is the founding node; p is its parent (trunk, stump, or root).
			// BranchedLT is n's fork base — read the tiny .fork marker directly
			// instead of opening the log (which would scan the segment). Equal
			// to mainForkBase(open(n)), but O(1) file read, not O(entries).
			return p.trunk, p.stumpName(), t.forkBaseOf(n.branch)
		}
		n = p
	}
}

// forkBaseOf reads a node's main-channel .fork base (the LT it forked at)
// directly from the marker file — cheap, no log open / segment scan.
func (t *Trunks) forkBaseOf(branch []string) uint64 {
	b, err := os.ReadFile(filepath.Join(t.irDir(branch), ".fork"))
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		if v, ok := strings.CutPrefix(strings.TrimSpace(line), "base="); ok {
			n, _ := strconv.ParseUint(strings.TrimSpace(v), 10, 64)
			return n
		}
	}
	return 0
}

// ListLight is List without Tip (the head's tail index). Tip requires opening
// the head's log (a segment scan); most callers (figaro's aria listing) never
// use it. ListLight opens no logs — ids, parent/stump lineage, and BranchedLT
// all come from the in-memory node tree + the cheap .fork marker read — so it
// is O(trunks) with no per-trunk disk scan. Tip is left zero; use Head/List
// when you actually need it.
func (t *Trunks) ListLight() []TrunkInfo {
	endRead, err := t.beginTrackedRead()
	if err != nil {
		return nil
	}
	defer endRead()
	out := make([]TrunkInfo, 0, len(t.heads))
	for _, id := range t.orderedTrunkIDsLocked() {
		key := t.heads[id]
		ti := TrunkInfo{ID: id, Head: t.nodes[key].branch}
		ti.Parent, ti.Stump, ti.BranchedLT = t.lineage(id)
		out = append(out, ti)
	}
	return out
}

// orderedTrunkIDsLocked returns live trunk ids in stable display order.
// Caller holds t.mu.
func (t *Trunks) orderedTrunkIDsLocked() []string {
	var ids []string
	if t.cfg.MintTrunkID != nil {
		for id := range t.heads {
			ids = append(ids, id)
		}
		sort.Strings(ids)
	} else {
		for i := 0; i < t.trunkSeq; i++ {
			if id := "t" + strconv.Itoa(i); t.heads[id] != "" {
				ids = append(ids, id)
			}
		}
	}
	return ids
}

// NodeInfo is a read-only view of a node (debug).
type NodeInfo struct {
	Branch   []string
	Trunk    string
	Frozen   bool
	Children []string
}

// Nodes returns every node (debug), root first.
func (t *Trunks) Nodes() []NodeInfo {
	endRead, err := t.beginTrackedRead()
	if err != nil {
		return nil
	}
	defer endRead()
	out := make([]NodeInfo, 0, len(t.nodes))
	for _, n := range t.nodes {
		out = append(out, NodeInfo{Branch: n.branch, Trunk: n.trunk, Frozen: n.frozen, Children: n.children})
	}
	return out
}

// --- helpers ---

func (t *Trunks) mintNode() string {
	id := "n" + strconv.Itoa(t.nodeSeq)
	t.nodeSeq++
	return id
}

func (t *Trunks) mintTrunk() string {
	if t.cfg.MintTrunkID != nil {
		for {
			id := t.cfg.MintTrunkID()
			if id != "" && !t.trunkExists(id) {
				return id
			}
		}
	}
	id := "t" + strconv.Itoa(t.trunkSeq)
	t.trunkSeq++
	return id
}

// trunkExists reports whether any cached node already carries this trunk id
// (collision check for a custom minter). Caller holds t.mu.
func (t *Trunks) trunkExists(id string) bool {
	for _, n := range t.nodes {
		if n.trunk == id {
			return true
		}
	}
	return false
}

func (t *Trunks) irDir(branch []string) string {
	return filepath.Join(append([]string{t.root, t.main}, branch...)...)
}

func joinKey(branch []string, child string) string {
	return strings.Join(append(append([]string(nil), branch...), child), "/")
}

func writeTrunkID(nodeDir, trunkID string) error {
	if err := writeSyncedFile(filepath.Join(nodeDir, trunkMarker), []byte(trunkID+"\n")); err != nil {
		return err
	}
	return disk.SyncDir(nodeDir)
}

func readTrunkID(nodeDir string) (string, bool) {
	b, err := os.ReadFile(filepath.Join(nodeDir, trunkMarker))
	if err != nil {
		return "", false
	}
	return strings.TrimSpace(string(b)), true
}

// numSuffix parses the numeric suffix of e.g. "n12" / "t3" given the
// expected prefix rune; returns -1 if it doesn't match.
func numSuffix(s string, prefix byte) int {
	if len(s) < 2 || s[0] != prefix {
		return -1
	}
	n, err := strconv.Atoi(s[1:])
	if err != nil {
		return -1
	}
	return n
}

// channelNames reads the manifest and returns every channel's name (each
// maps directly to a dir under the root: "ir", "chalkboard",
// "translations/<provider>", …).
func channelNames(dir string) ([]string, error) {
	data, err := os.ReadFile(filepath.Join(dir, manifestName))
	if err != nil {
		return nil, fmt.Errorf("xwal: read manifest: %w", err)
	}
	var m manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("xwal: parse manifest: %w", err)
	}
	out := make([]string, 0, len(m.Channels))
	for _, c := range m.Channels {
		out = append(out, c.Name)
	}
	return out, nil
}

// mainChannelName reads the manifest to find the main channel name.
func mainChannelName(dir string) (string, error) {
	data, err := os.ReadFile(filepath.Join(dir, manifestName))
	if err != nil {
		return "", fmt.Errorf("xwal: read manifest: %w", err)
	}
	var m manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return "", fmt.Errorf("xwal: parse manifest: %w", err)
	}
	return m.Main, nil
}

// ownFirstIdx is the head's own first main index (forkBase, or 1 at root).
func ownFirstIdx(x *XWAL) uint64 {
	if fb := mainForkBase(x); fb > 0 {
		return fb
	}
	return 1
}

func mainForkBase(x *XWAL) uint64 { return x.chans[x.main].log.ForkBase() }

// isEmptyHead reports whether a forked head has no own entries yet (its
// content is wholly inherited).
func isEmptyHead(x *XWAL) bool {
	fb := mainForkBase(x)
	return fb > 0 && x.chans[x.main].log.LastIndex() < fb
}
