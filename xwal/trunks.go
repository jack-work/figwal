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

	"github.com/jack-work/figwal/disk"
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

	lineageMu sync.Mutex
	lineages  map[string]*sync.Mutex

	// version is bumped every time rebuild() runs, giving consumers a
	// cheap probe for "has the trunk topology changed since I last
	// looked?" without a directory walk. Modeled on SQLite's schema
	// cookie: mutations bump it internally, readers can compare against
	// their last-seen value, and Refresh() reconciles on demand for the
	// cross-process case. In-process this is invariably in sync with
	// disk because every mutating public method ends in rebuild().
	version atomic.Uint64

	hotMu   sync.Mutex
	hot     *trunkStore
	retired map[*trunkStore]struct{}
}

type trunkStore struct {
	store   *disk.Store
	heads   map[string]*hotHead
	refs    int
	retired bool
}

type hotHead struct {
	ready chan struct{}
	x     *XWAL
	err   error
}

var trunkRegistry = struct {
	sync.Mutex
	roots map[string]map[*Trunks]struct{}
}{roots: map[string]map[*Trunks]struct{}{}}

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
func CreateTrunks(dir string, cfg Config) (*Trunks, error) {
	if cfg.Main == "" {
		return nil, fmt.Errorf("xwal: CreateTrunks needs cfg.Main")
	}
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

	t := &Trunks{root: dir, cfg: cfg, main: cfg.Main}
	if err := t.rebuild(); err != nil {
		return nil, err
	}
	if err := t.register(); err != nil {
		return nil, err
	}
	return t, nil
}

// OpenTrunks opens an existing trunk store, rebuilding its cache from disk.
func OpenTrunks(dir string, cfg Config) (*Trunks, error) {
	main, err := mainChannelName(dir)
	if err != nil {
		return nil, err
	}
	t := &Trunks{root: dir, cfg: cfg, main: main}
	if err := t.rebuild(); err != nil {
		return nil, err
	}
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
	t.retireRootHot()
	t.nodes = map[string]*tnode{}
	t.heads = map[string]string{}
	t.nodeSeq, t.trunkSeq = 0, 0
	base := filepath.Join(t.root, t.main)
	if err := t.walk(base, nil, "", true); err != nil {
		return err
	}
	t.version.Add(1)
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
	t.mu.Lock()
	defer t.mu.Unlock()
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

func (t *Trunks) lockLineage(trunk string) func() {
	t.lineageMu.Lock()
	if t.lineages == nil {
		t.lineages = map[string]*sync.Mutex{}
	}
	lock := t.lineages[trunk]
	if lock == nil {
		lock = &sync.Mutex{}
		t.lineages[trunk] = lock
	}
	t.lineageMu.Unlock()
	lock.Lock()
	return lock.Unlock
}

func (t *Trunks) ensureNoOpenHeads() error {
	t.hotMu.Lock()
	defer t.hotMu.Unlock()
	refs := 0
	if t.hot != nil {
		refs += t.hot.refs
	}
	for h := range t.retired {
		refs += h.refs
	}
	if refs != 0 {
		return fmt.Errorf("xwal: topology mutation with %d open head(s)", refs)
	}
	return nil
}

func (t *Trunks) borrowHot(branch []string) (*XWAL, func() error, error) {
	t.hotMu.Lock()
	h := t.hot
	if h == nil {
		h = &trunkStore{store: disk.NewStore(), heads: map[string]*hotHead{}}
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
		_ = t.releaseHot(h)
		return nil, nil, head.err
	}

	var once sync.Once
	var releaseErr error
	release := func() error {
		once.Do(func() {
			releaseErr = t.releaseHot(h)
		})
		return releaseErr
	}
	return head.x, release, nil
}

func (t *Trunks) openHot(branch []string) (*XWAL, error) {
	x, release, err := t.borrowHot(branch)
	if err != nil {
		return nil, err
	}
	return x.sharedView(release, t.retireRootHot), nil
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
	root, err := filepath.Abs(t.root)
	if err != nil {
		return err
	}
	t.registryRoot = filepath.Clean(root)
	trunkRegistry.Lock()
	peers := trunkRegistry.roots[t.registryRoot]
	if peers == nil {
		peers = map[*Trunks]struct{}{}
		trunkRegistry.roots[t.registryRoot] = peers
	}
	peers[t] = struct{}{}
	trunkRegistry.Unlock()
	return nil
}

func (t *Trunks) retireRootHot() {
	retireTrunkStores(t.root)
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

// Head opens the live head node of a trunk. Caller closes it.
func (t *Trunks) Head(trunk string) (*XWAL, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	branch, err := t.headBranch(trunk)
	if err != nil {
		return nil, err
	}
	return t.openHot(branch)
}

// Append adds a main-timeline entry to a trunk.
//   - atMainLT == 0 or >= the head's tail: append (no fork). Returns the
//     same trunk.
//   - 0 < atMainLT < tail and within the head's own range: interior fork —
//     a NEW trunk shares [1..atMainLT] and diverges at atMainLT+1; the
//     existing trunk keeps its full history. Returns the new trunk.
//   - atMainLT below the head's own range (in a frozen ancestor): re-split-below
//     — fork the owning ancestor, minting a sibling trunk; the original
//     timeline (and the caller's trunk) continues unchanged.
func (t *Trunks) Append(trunk string, atMainLT uint64, payload, meta []byte) (string, uint64, error) {
	unlockLineage := t.lockLineage(trunk)
	defer unlockLineage()

	t.mu.RLock()
	branch, err := t.headBranch(trunk)
	if err != nil {
		t.mu.RUnlock()
		return "", 0, err
	}
	x, release, err := t.borrowHot(branch)
	if err != nil {
		t.mu.RUnlock()
		return "", 0, err
	}
	tail := mainTail(x)
	if atMainLT == 0 || atMainLT >= tail {
		lt, appendErr := x.AppendMain(payload, meta)
		_ = release()
		t.mu.RUnlock()
		if appendErr != nil {
			return "", 0, appendErr
		}
		return trunk, lt, nil
	}
	_ = release()
	t.mu.RUnlock()

	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.ensureNoOpenHeads(); err != nil {
		return "", 0, err
	}
	return t.appendForkLocked(trunk, atMainLT, payload, meta)
}

func (t *Trunks) appendForkLocked(trunk string, atMainLT uint64, payload, meta []byte) (string, uint64, error) {
	branch, err := t.headBranch(trunk)
	if err != nil {
		return "", 0, err
	}
	x, err := t.openHot(branch)
	if err != nil {
		return "", 0, err
	}
	tail := mainTail(x)
	ownFirst := ownFirstIdx(x)

	// Topology can change between the optimistic read and the exclusive
	// lock, so re-check whether this became a plain append.
	if atMainLT == 0 || atMainLT >= tail {
		lt, aerr := x.AppendMain(payload, meta)
		x.Close()
		if aerr != nil {
			return "", 0, aerr
		}
		return trunk, lt, nil
	}
	if atMainLT < ownFirst {
		// Re-split-below: atMainLT lives in a frozen ancestor. Fork the
		// ancestor node that owns it (re-homing its suffix + children into
		// the continuation) and append to the new alternative.
		x.Close()
		t.retireRootHot()
		return t.resplitBelow(branch, atMainLT, payload, meta, true)
	}
	x.Close()
	t.retireRootHot()
	// Interior fork: share [1..atMainLT], diverge at atMainLT+1.
	altDir := t.mintNode()
	contDir := t.mintNode()
	fx, err := Open(t.root, t.cfg, branch...)
	if err != nil {
		return "", 0, err
	}
	child, ferr := fx.Fork(atMainLT+1, altDir, contDir) // child = alt; old-future = cont
	fx.Close()
	if ferr != nil {
		return "", 0, ferr
	}
	altLT, aerr := child.AppendMain(payload, meta)
	child.Close()
	if aerr != nil {
		return "", 0, aerr
	}
	altTrunk, err := t.commitFork(branch, contDir, altDir)
	if err != nil {
		return "", 0, err
	}
	return altTrunk, altLT, nil
}

// resplitBelow forks the ancestor node along `branch` that OWNS atMainLT (a
// frozen branch point) at atMainLT+1, minting a new alternative trunk that
// shares [1..atMainLT]. The owner's original timeline beyond atMainLT — its
// suffix and ALL its child forks (including the caller's own trunk) — re-homes
// into the continuation (which keeps the owner's trunk id, the normal
// continuation-chain behavior). If doAppend, payload is written to the new
// alternative. Caller holds t.mu. This is how a fork at an inherited LT (a
// turn shared with ancestors) produces a sibling branch.
func (t *Trunks) resplitBelow(branch []string, atMainLT uint64, payload, meta []byte, doAppend bool) (string, uint64, error) {
	t.retireRootHot()
	ownerBranch, err := t.ownerOf(branch, atMainLT)
	if err != nil {
		return "", 0, err
	}
	altDir := t.mintNode()
	contDir := t.mintNode()
	ox, err := Open(t.root, t.cfg, ownerBranch...)
	if err != nil {
		return "", 0, err
	}
	child, ferr := ox.Fork(atMainLT+1, altDir, contDir) // child = alt; old-future = cont (re-homes children)
	ox.Close()
	if ferr != nil {
		return "", 0, fmt.Errorf("re-split-below: %w", ferr)
	}
	var altLT uint64
	if doAppend {
		altLT, ferr = child.AppendMain(payload, meta)
	}
	child.Close()
	if ferr != nil {
		return "", 0, ferr
	}
	altTrunk, cerr := t.commitFork(ownerBranch, contDir, altDir)
	if cerr != nil {
		return "", 0, cerr
	}
	return altTrunk, altLT, nil
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
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.ensureNoOpenHeads(); err != nil {
		return "", err
	}
	headKey, ok := t.heads[trunk]
	if !ok {
		return "", fmt.Errorf("xwal: unknown trunk %q", trunk)
	}
	head := t.nodes[headKey]
	x, err := t.openHot(head.branch)
	if err != nil {
		return "", err
	}
	tail := mainTail(x)
	empty := isEmptyHead(x)
	fb := mainForkBase(x)
	x.Close()
	t.retireRootHot()

	if empty {
		// Redirect to an N-ary fork of the parent at the head's fork point.
		if head.isRoot {
			return "", fmt.Errorf("xwal: cannot fork an empty root trunk")
		}
		pbranch := t.nodes[head.parent].branch
		altDir := t.mintNode()
		px, err := Open(t.root, t.cfg, pbranch...)
		if err != nil {
			return "", err
		}
		altX, ferr := px.Fork(fb, altDir, "") // N-ary add-one at the parent's fork point (no old-future)
		px.Close()
		if ferr != nil {
			return "", fmt.Errorf("fork-tail (empty head, via parent): %w", ferr)
		}
		altX.Close()
		altTrunk := t.mintTrunk()
		if err := writeTrunkID(t.irDir(append(append([]string(nil), pbranch...), altDir)), altTrunk); err != nil {
			return "", err
		}
		return altTrunk, t.rebuild()
	}

	// Head has content: freeze it; one fork creates both children at tail+1
	// (alt = child, cont = old-future) — both empty, inheriting the full
	// prefix in every channel (always-materialize → write isolation).
	altDir := t.mintNode()
	contDir := t.mintNode()
	fx, err := Open(t.root, t.cfg, head.branch...)
	if err != nil {
		return "", err
	}
	child, ferr := fx.Fork(tail+1, altDir, contDir)
	fx.Close()
	if ferr != nil {
		return "", fmt.Errorf("fork-tail: %w", ferr)
	}
	child.Close()
	return t.commitFork(head.branch, contDir, altDir)
}

// ForkAt forks a trunk at an interior main-LT WITHOUT appending: it shares
// [1..atMainLT] and creates an EMPTY alternative trunk diverging at
// atMainLT+1; the original trunk keeps its id and its suffix. At or past the
// tail it degenerates to a tail fork (ForkTail). Returns the new alternative
// trunk. (Append does fork+send in one; ForkAt is the imperative-only fork.)
func (t *Trunks) ForkAt(trunk string, atMainLT uint64) (string, error) {
	t.mu.Lock()
	if err := t.ensureNoOpenHeads(); err != nil {
		t.mu.Unlock()
		return "", err
	}
	branch, err := t.headBranch(trunk)
	if err != nil {
		t.mu.Unlock()
		return "", err
	}
	x, err := t.openHot(branch)
	if err != nil {
		t.mu.Unlock()
		return "", err
	}
	tail := mainTail(x)
	ownFirst := ownFirstIdx(x)
	x.Close()

	if atMainLT == 0 || atMainLT >= tail {
		t.mu.Unlock()
		return t.ForkTail(trunk) // ForkTail re-acquires the lock
	}
	if atMainLT < ownFirst {
		// Re-split-below: fork the ancestor that owns atMainLT (no append).
		t.retireRootHot()
		alt, _, rerr := t.resplitBelow(branch, atMainLT, nil, nil, false)
		t.mu.Unlock()
		return alt, rerr
	}

	t.retireRootHot()
	altDir := t.mintNode()
	contDir := t.mintNode()
	fx, err := Open(t.root, t.cfg, branch...)
	if err != nil {
		t.mu.Unlock()
		return "", err
	}
	child, ferr := fx.Fork(atMainLT+1, altDir, contDir) // child = alt; old-future = cont
	fx.Close()
	if ferr != nil {
		t.mu.Unlock()
		return "", ferr
	}
	child.Close()
	alt, cerr := t.commitFork(branch, contDir, altDir)
	t.mu.Unlock()
	return alt, cerr
}

// Remove deletes a trunk: its founding node's entire subtree, in every
// channel. Trunk-addressed (a node is plumbing). Refuses the root trunk, and
// refuses a trunk that has live branches (descendant trunks branched off it)
// unless recursive — in which case those branches go too.
func (t *Trunks) Remove(trunk string, recursive bool) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.ensureNoOpenHeads(); err != nil {
		return err
	}
	t.retireRootHot()

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
	t.mu.Lock()
	defer t.mu.Unlock()
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
	t.mu.Lock()
	defer t.mu.Unlock()
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
	if _, err := t.forkChild(nil, name); err != nil {
		return fmt.Errorf("xwal: create-stump %q: %w", name, err)
	}
	return t.rebuild()
}

// StumpHead opens a stump's branch for appending its birth content (IR +
// related channels), before it gains trunk children. Caller closes it.
func (t *Trunks) StumpHead(name string) (*XWAL, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	n := t.nodes[name]
	if n == nil || !n.isStump {
		return nil, fmt.Errorf("xwal: no stump %q", name)
	}
	return t.openHot([]string{name})
}

// SpawnUnderStump mints a new trunk (a top-level aria) as a child of a stump.
func (t *Trunks) SpawnUnderStump(name string) (TrunkID, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
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
	t.mu.Lock()
	defer t.mu.Unlock()
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
	childBranch, err := t.forkChild(parentBranch, childDir)
	if err != nil {
		return "", fmt.Errorf("xwal: spawn: %w", err)
	}
	childTrunk := t.mintTrunk()
	if err := writeTrunkID(t.irDir(childBranch), childTrunk); err != nil {
		return "", err
	}
	return childTrunk, t.rebuild()
}

// forkChild forks the node at parentBranch at its tail (N-ary add-one, no
// continuation), creating an empty child dir named childDir in every channel.
// Returns the child's branch. Caller holds t.mu and must rebuild afterwards.
func (t *Trunks) forkChild(parentBranch []string, childDir string) ([]string, error) {
	x, err := t.openHot(parentBranch)
	if err != nil {
		return nil, err
	}
	tail := mainTail(x)
	x.Close()
	t.retireRootHot()
	fx, err := Open(t.root, t.cfg, parentBranch...)
	if err != nil {
		return nil, err
	}
	c, ferr := fx.Fork(tail+1, childDir, "") // N-ary add-one at the tail; no continuation
	fx.Close()
	if ferr != nil {
		return nil, ferr
	}
	c.Close()
	return append(append([]string(nil), parentBranch...), childDir), nil
}

// OwnerTrunk returns the trunk id of the node that OWNS atMainLT along the
// given trunk's lineage — the deepest ancestor whose own segments contain it.
// Callers layer policy on this: e.g. an LT owned by a "ceremonial" trunk (a
// null root or loadout) can be redirected to SpawnChild instead of a re-split.
func (t *Trunks) OwnerTrunk(trunk string, atMainLT uint64) (string, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
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
	t.mu.RLock()
	defer t.mu.RUnlock()
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
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.ensureNoOpenHeads(); err != nil {
		return 0, err
	}
	t.retireRootHot()
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
	t.mu.RLock()
	defer t.mu.RUnlock()
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

// commitFork writes the trunk markers for a freeze+two-children fork: the
// continuation inherits the (frozen) head's trunk, the alternative founds
// a new one. Then it rebuilds the cache from disk. Returns the new alt
// trunk id.
func (t *Trunks) commitFork(headBranch []string, contDir, altDir string) (string, error) {
	headKey := strings.Join(headBranch, "/")
	headTrunk := t.nodes[headKey].trunk
	if err := writeTrunkID(t.irDir(append(append([]string(nil), headBranch...), contDir)), headTrunk); err != nil {
		return "", err
	}
	altTrunk := t.mintTrunk()
	if err := writeTrunkID(t.irDir(append(append([]string(nil), headBranch...), altDir)), altTrunk); err != nil {
		return "", err
	}
	return altTrunk, t.rebuild()
}

// AppendChannel appends to a related channel of a trunk's head.
func (t *Trunks) AppendChannel(trunk, channel string, mainLT uint64, payload, meta []byte) (uint64, error) {
	unlockLineage := t.lockLineage(trunk)
	defer unlockLineage()
	t.mu.RLock()
	defer t.mu.RUnlock()
	branch, err := t.headBranch(trunk)
	if err != nil {
		return 0, err
	}
	x, release, err := t.borrowHot(branch)
	if err != nil {
		return 0, err
	}
	defer release()
	if mainLT == 0 {
		mainLT = mainTail(x) + 1 // reducible default: one ahead
	}
	return x.Append(channel, mainLT, payload, meta)
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
	t.mu.RLock()
	defer t.mu.RUnlock()
	ids := t.orderedTrunkIDsLocked()
	out := make([]TrunkInfo, 0, len(ids))
	for _, id := range ids {
		key := t.heads[id]
		ti := TrunkInfo{ID: id, Head: t.nodes[key].branch}
		ti.Parent, ti.Stump, ti.BranchedLT = t.lineage(id)
		if x, err := t.openHot(t.nodes[key].branch); err == nil {
			ti.Tip = mainTail(x)
			x.Close()
		}
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
	t.mu.RLock()
	defer t.mu.RUnlock()
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
	t.mu.RLock()
	defer t.mu.RUnlock()
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
	return os.WriteFile(filepath.Join(nodeDir, trunkMarker), []byte(trunkID+"\n"), 0o644)
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
