package xwal

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
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

	mu       sync.Mutex
	nodes    map[string]*tnode // key = branch joined by "/" ("" = root)
	heads    map[string]string // trunk id -> head node key (the one live leaf)
	nodeSeq  int               // next "n<N>" dir name
	trunkSeq int               // next "t<N>" trunk id
}

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
}

const trunkMarker = ".trunk"

// genesisMarker is the root node's first main entry, so the trunk is
// immediately forkable and its prefix is never empty.
const genesisMarker = `{"genesis":true}`

// mainTail returns the main channel's last index for an opened branch.
func mainTail(x *XWAL) uint64 { return x.chans[x.main].log.LastIndex() }

// CreateTrunks initializes a fresh trunk store at dir (creating the xwal
// from cfg), seeds the genesis root trunk, and returns it plus the root
// trunk id.
func CreateTrunks(dir string, cfg Config) (*Trunks, string, error) {
	if cfg.Main == "" {
		return nil, "", fmt.Errorf("xwal: CreateTrunks needs cfg.Main")
	}
	if _, err := os.Stat(filepath.Join(dir, cfg.Main, trunkMarker)); err == nil {
		return nil, "", fmt.Errorf("xwal: trunks already initialized at %s", dir)
	}
	x, err := Open(dir, cfg)
	if err != nil {
		return nil, "", err
	}
	glt, err := x.AppendMain([]byte(genesisMarker), nil)
	if err != nil {
		x.Close()
		return nil, "", err
	}
	for _, c := range x.Channels() {
		if c.Kind == ChannelReducible {
			if _, err := x.Append(c.Name, glt, []byte("{}"), nil); err != nil {
				x.Close()
				return nil, "", err
			}
		}
	}
	x.Close()

	rootTrunk := "t0"
	if err := writeTrunkID(filepath.Join(dir, cfg.Main), rootTrunk); err != nil {
		return nil, "", err
	}
	t := &Trunks{root: dir, cfg: cfg, main: cfg.Main}
	if err := t.rebuild(); err != nil {
		return nil, "", err
	}
	return t, rootTrunk, nil
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
	return t, nil
}

// rebuild walks the main channel's directory tree and reconstructs the
// node + trunk cache from disk (dirs + .trunk markers). Source of truth.
func (t *Trunks) rebuild() error {
	t.nodes = map[string]*tnode{}
	t.heads = map[string]string{}
	t.nodeSeq, t.trunkSeq = 0, 0
	base := filepath.Join(t.root, t.main)
	return t.walk(base, nil, "", true)
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
	n := &tnode{
		branch: append([]string(nil), branch...),
		trunk:  trunkID, frozen: len(kids) > 0, parent: parentKey, isRoot: isRoot,
	}
	for _, k := range kids {
		n.children = append(n.children, joinKey(branch, k))
	}
	t.nodes[key] = n
	t.bumpSeqs(branch, trunkID)
	if !n.frozen && trunkID != "" {
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

// Head opens the live head node of a trunk. Caller closes it.
func (t *Trunks) Head(trunk string) (*XWAL, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	branch, err := t.headBranch(trunk)
	if err != nil {
		return nil, err
	}
	return Open(t.root, t.cfg, branch...)
}

// Append adds a main-timeline entry to a trunk.
//   - atMainLT == 0 or >= the head's tail: append (no fork). Returns the
//     same trunk.
//   - 0 < atMainLT < tail and within the head's own range: interior fork —
//     a NEW trunk shares [1..atMainLT] and diverges at atMainLT+1; the
//     existing trunk keeps its full history. Returns the new trunk.
//   - atMainLT below the head's own range (frozen ancestor): re-split-below,
//     not yet supported.
func (t *Trunks) Append(trunk string, atMainLT uint64, payload, meta []byte) (string, uint64, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	branch, err := t.headBranch(trunk)
	if err != nil {
		return "", 0, err
	}
	x, err := Open(t.root, t.cfg, branch...)
	if err != nil {
		return "", 0, err
	}
	tail := mainTail(x)
	ownFirst := ownFirstIdx(x)

	if atMainLT == 0 || atMainLT >= tail {
		lt, aerr := x.AppendMain(payload, meta)
		x.Close()
		if aerr != nil {
			return "", 0, aerr
		}
		return trunk, lt, nil
	}
	if atMainLT < ownFirst {
		x.Close()
		return "", 0, fmt.Errorf("xwal: main-LT %d is in frozen history of trunk %q (re-split-below not yet supported)", atMainLT, trunk)
	}
	// Interior fork: share [1..atMainLT], diverge at atMainLT+1.
	altDir := t.mintNode()
	contDir := t.mintNode()
	child, ferr := x.Fork(atMainLT+1, altDir, contDir) // child = alt; old-future = cont
	x.Close()
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

// ForkTail bisects a trunk's present.
//   - head has content: freeze it; the continuation keeps the trunk (new
//     empty head) and a new alternative trunk is founded.
//   - head is empty: it has no LT of its own, so redirect — N-ary fork the
//     PARENT, adding one new alternative sibling trunk; the trunk keeps its
//     empty head untouched.
func (t *Trunks) ForkTail(trunk string) (string, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	headKey, ok := t.heads[trunk]
	if !ok {
		return "", fmt.Errorf("xwal: unknown trunk %q", trunk)
	}
	head := t.nodes[headKey]
	x, err := Open(t.root, t.cfg, head.branch...)
	if err != nil {
		return "", err
	}
	tail := mainTail(x)
	empty := isEmptyHead(x)
	fb := mainForkBase(x)
	x.Close()

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
		altX, ferr := px.Fork(fb, altDir, altDir+"_of") // N-ary at the parent's fork point
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

	// Head has content: freeze it, two empty children at tail+1.
	altDir := t.mintNode()
	contDir := t.mintNode()
	x1, err := Open(t.root, t.cfg, head.branch...)
	if err != nil {
		return "", err
	}
	a, ferr := x1.Fork(tail+1, altDir, altDir+"_of")
	x1.Close()
	if ferr != nil {
		return "", fmt.Errorf("fork-tail alternative: %w", ferr)
	}
	a.Close()
	x2, err := Open(t.root, t.cfg, head.branch...)
	if err != nil {
		return "", err
	}
	c, ferr := x2.Fork(tail+1, contDir, contDir+"_of")
	x2.Close()
	if ferr != nil {
		return "", fmt.Errorf("fork-tail continuation: %w", ferr)
	}
	c.Close()
	return t.commitFork(head.branch, contDir, altDir)
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
	t.mu.Lock()
	defer t.mu.Unlock()
	branch, err := t.headBranch(trunk)
	if err != nil {
		return 0, err
	}
	x, err := Open(t.root, t.cfg, branch...)
	if err != nil {
		return 0, err
	}
	defer x.Close()
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
	Parent     string   // parent trunk id
	BranchedLT uint64
	Tip        uint64
}

// List returns every trunk, in id order.
func (t *Trunks) List() []TrunkInfo {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]TrunkInfo, 0, len(t.heads))
	for i := 0; i < t.trunkSeq; i++ {
		id := "t" + strconv.Itoa(i)
		key, ok := t.heads[id]
		if !ok {
			continue
		}
		ti := TrunkInfo{ID: id, Head: t.nodes[key].branch}
		ti.Parent, ti.BranchedLT = t.lineage(id)
		if x, err := Open(t.root, t.cfg, t.nodes[key].branch...); err == nil {
			ti.Tip = mainTail(x)
			x.Close()
		}
		out = append(out, ti)
	}
	return out
}

// lineage finds a trunk's founding node (shallowest node carrying the
// trunk), returning its parent trunk id and the LT it branched at.
func (t *Trunks) lineage(trunk string) (string, uint64) {
	n := t.nodes[t.heads[trunk]]
	for {
		if n.isRoot {
			return "", 0 // root trunk has no parent
		}
		p := t.nodes[n.parent]
		if p == nil {
			return "", 0
		}
		if p.trunk != trunk {
			// n is the founding node; p is in the parent trunk.
			var bl uint64
			if x, err := Open(t.root, t.cfg, n.branch...); err == nil {
				bl = mainForkBase(x)
				x.Close()
			}
			return p.trunk, bl
		}
		n = p
	}
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
	t.mu.Lock()
	defer t.mu.Unlock()
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
	id := "t" + strconv.Itoa(t.trunkSeq)
	t.trunkSeq++
	return id
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
