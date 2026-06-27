package xwal

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
)

// Forest is a joint forest over an xwal: the tree of branch nodes plus
// the trunk identities that flow down the continuation side of every
// fork. One trunk is one continuation chain across ALL channels (the
// triune is forked as a unit), so a trunk is the stable handle a caller
// addresses; the per-fork node id is plumbing.
//
// Identity rules (the settled trunk model):
//   - A fork freezes the node and mints two children. The continuation
//     (figwal's old-future) INHERITS the parent's trunk — the trunk's head
//     just advances to it. The alternative (figwal's child) FOUNDS a new
//     trunk.
//   - Append at a trunk's tail extends it (no fork). Append at an interior
//     main-LT forks a NEW trunk there and leaves the existing trunk intact.
//   - Trunks are append-only/immutable in identity; internally they fork.
//
// The forest persists a node+trunk index (forest.json) beside the xwal
// manifest. It mints readable counter ids (n0,n1… nodes; t0,t1… trunks).
type Forest struct {
	root string
	cfg  Config
	idx  *forestIndex
}

// NodeID and TrunkID are string ids minted by the forest.
type (
	NodeID  = string
	TrunkID = string
)

const (
	forestName    = "forest.json"
	genesisMarker = `{"genesis":true}`
)

type forestIndex struct {
	Nodes    map[string]*forestNode `json:"nodes"`
	Trunks   map[string]*trunkRec   `json:"trunks"`
	Roots    []string               `json:"roots,omitempty"` // root trunk ids, in creation order
	NodeSeq  int                    `json:"node_seq"`
	TrunkSeq int                    `json:"trunk_seq"`
}

type forestNode struct {
	ID       string   `json:"id"`
	Branch   []string `json:"branch,omitempty"` // fork-name chain; the xwal branch ([] = root)
	Parent   string   `json:"parent,omitempty"`
	Children []string `json:"children,omitempty"`
	Frozen   bool     `json:"frozen,omitempty"`
	Trunk    string   `json:"trunk"`
	Vector   []int    `json:"vector,omitempty"`
}

type trunkRec struct {
	ID         string `json:"id"`
	Head       string `json:"head"` // node id of the live writable head
	Parent     string `json:"parent,omitempty"`
	BranchedLT uint64 `json:"branched_lt,omitempty"`
	Tip        uint64 `json:"tip,omitempty"` // head node's main tail (cached for listing)
	Frozen     bool   `json:"frozen,omitempty"`
}

// CreateForest initializes a brand-new forest at dir (and the underlying
// xwal manifest from cfg), seeding a genesis root trunk. The root node's
// main channel gets one genesis entry so the trunk is immediately
// forkable, and each reducible channel gets an empty `{}` patch so its
// prefix is never empty (mirrors figaro's genesis seed). Returns the
// forest and the root trunk id.
func CreateForest(dir string, cfg Config) (*Forest, TrunkID, error) {
	if _, err := os.Stat(filepath.Join(dir, forestName)); err == nil {
		return nil, "", fmt.Errorf("xwal: forest already exists at %s", dir)
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
	tip := mainTail(x)
	x.Close()

	idx := &forestIndex{Nodes: map[string]*forestNode{}, Trunks: map[string]*trunkRec{}}
	nodeID := "n" + strconv.Itoa(idx.NodeSeq)
	idx.NodeSeq++
	trunkID := "t" + strconv.Itoa(idx.TrunkSeq)
	idx.TrunkSeq++
	idx.Nodes[nodeID] = &forestNode{ID: nodeID, Trunk: trunkID, Vector: []int{0}}
	idx.Trunks[trunkID] = &trunkRec{ID: trunkID, Head: nodeID, Tip: tip}
	idx.Roots = []string{trunkID}

	f := &Forest{root: dir, cfg: cfg, idx: idx}
	if err := f.save(); err != nil {
		return nil, "", err
	}
	return f, trunkID, nil
}

// OpenForest opens an existing forest at dir.
func OpenForest(dir string, cfg Config) (*Forest, error) {
	idx, err := loadForestIndex(dir)
	if err != nil {
		return nil, err
	}
	return &Forest{root: dir, cfg: cfg, idx: idx}, nil
}

func loadForestIndex(dir string) (*forestIndex, error) {
	data, err := os.ReadFile(filepath.Join(dir, forestName))
	if err != nil {
		return nil, err
	}
	var idx forestIndex
	if err := json.Unmarshal(data, &idx); err != nil {
		return nil, fmt.Errorf("xwal: parse forest index: %w", err)
	}
	if idx.Nodes == nil {
		idx.Nodes = map[string]*forestNode{}
	}
	if idx.Trunks == nil {
		idx.Trunks = map[string]*trunkRec{}
	}
	return &idx, nil
}

func (f *Forest) save() error {
	body, err := json.MarshalIndent(f.idx, "", "  ")
	if err != nil {
		return err
	}
	path := filepath.Join(f.root, forestName)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, body, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func (f *Forest) mintNode() string {
	id := "n" + strconv.Itoa(f.idx.NodeSeq)
	f.idx.NodeSeq++
	return id
}

func (f *Forest) mintTrunk() string {
	id := "t" + strconv.Itoa(f.idx.TrunkSeq)
	f.idx.TrunkSeq++
	return id
}

// headNode resolves a trunk to its live head node record.
func (f *Forest) headNode(trunk TrunkID) (*forestNode, error) {
	t := f.idx.Trunks[trunk]
	if t == nil {
		return nil, fmt.Errorf("xwal: unknown trunk %q", trunk)
	}
	n := f.idx.Nodes[t.Head]
	if n == nil {
		return nil, fmt.Errorf("xwal: trunk %q head node %q missing", trunk, t.Head)
	}
	return n, nil
}

// Head opens the live head node of a trunk for reading/appending.
// The caller closes the returned XWAL.
func (f *Forest) Head(trunk TrunkID) (*XWAL, NodeID, error) {
	n, err := f.headNode(trunk)
	if err != nil {
		return nil, "", err
	}
	x, err := Open(f.root, f.cfg, n.Branch...)
	if err != nil {
		return nil, "", err
	}
	return x, n.ID, nil
}

// Append adds a main-timeline entry to a trunk.
//   - atMainLT == 0, or >= the trunk's current tail: append at the head
//     (no fork). Returns the same trunk.
//   - 0 < atMainLT < tail: fork a NEW trunk at that point and append there;
//     the existing trunk is left intact (its continuation keeps the trunk).
//     Returns the new trunk. (Interior forks are only supported within the
//     head node's own LT range this pass; an LT in a frozen ancestor errors
//     — re-split-below is deferred.)
func (f *Forest) Append(trunk TrunkID, atMainLT uint64, payload, meta []byte) (TrunkID, uint64, error) {
	head, err := f.headNode(trunk)
	if err != nil {
		return "", 0, err
	}
	x, err := Open(f.root, f.cfg, head.Branch...)
	if err != nil {
		return "", 0, err
	}
	tail := mainTail(x)

	if atMainLT == 0 || atMainLT >= tail {
		lt, aerr := x.AppendMain(payload, meta)
		x.Close()
		if aerr != nil {
			return "", 0, aerr
		}
		f.idx.Trunks[trunk].Tip = lt
		if err := f.save(); err != nil {
			return "", 0, err
		}
		return trunk, lt, nil
	}

	// Interior fork: share [1..atMainLT], diverge at atMainLT+1 (the new
	// content lands there). The existing trunk keeps the original suffix.
	mainCh := x.chans[x.main]
	if fb := mainCh.log.ForkBase(); fb > 0 && atMainLT < fb {
		x.Close()
		return "", 0, fmt.Errorf("xwal: main-LT %d is in frozen history of trunk %q (re-split-below not yet supported)", atMainLT, trunk)
	}
	altNode := f.mintNode()
	contNode := f.mintNode()
	child, ferr := x.Fork(atMainLT+1, altNode, contNode) // child = alternative; old-future = continuation
	x.Close()
	if ferr != nil {
		return "", 0, ferr
	}
	altLT, aerr := child.AppendMain(payload, meta)
	child.Close()
	if aerr != nil {
		return "", 0, aerr
	}
	altTrunk := f.registerFork(head, contNode, altNode, atMainLT+1, tail)
	f.idx.Trunks[altTrunk].Tip = altLT
	if err := f.save(); err != nil {
		return "", 0, err
	}
	return altTrunk, altLT, nil
}

// ForkTail bisects a trunk's present: the head freezes and two empty
// children are minted from the full prefix — the continuation (the trunk
// keeps going, head advances) and a new alternative trunk. Returns the
// alternative trunk id. (figwal makes a forked node read-only, so the
// trunk needs a fresh continuation leaf; that's why two children appear.)
func (f *Forest) ForkTail(trunk TrunkID) (TrunkID, error) {
	head, err := f.headNode(trunk)
	if err != nil {
		return "", err
	}
	tail, err := f.trunkTail(head)
	if err != nil {
		return "", err
	}
	altNode := f.mintNode()
	contNode := f.mintNode()
	// Two N-ary tail forks at tail+1. A tail fork creates no old-future,
	// so the old-future name is unused — pass a distinct throwaway.
	x, err := Open(f.root, f.cfg, head.Branch...)
	if err != nil {
		return "", err
	}
	altX, ferr := x.Fork(tail+1, altNode, altNode+"_of")
	x.Close()
	if ferr != nil {
		return "", fmt.Errorf("fork-tail alternative: %w", ferr)
	}
	altX.Close()
	x2, err := Open(f.root, f.cfg, head.Branch...)
	if err != nil {
		return "", err
	}
	contX, ferr := x2.Fork(tail+1, contNode, contNode+"_of")
	x2.Close()
	if ferr != nil {
		return "", fmt.Errorf("fork-tail continuation: %w", ferr)
	}
	contX.Close()
	altTrunk := f.registerFork(head, contNode, altNode, tail+1, tail)
	if err := f.save(); err != nil {
		return "", err
	}
	return altTrunk, nil
}

// registerFork records a freeze+two-children fork: the head freezes, the
// continuation inherits the head's trunk (the trunk's head advances), and
// the alternative founds a new trunk. Returns the new alt trunk id.
func (f *Forest) registerFork(head *forestNode, contNode, altNode string, branchedLT, contTip uint64) TrunkID {
	head.Frozen = true
	head.Children = append(head.Children, contNode, altNode)

	contBranch := append(append([]string(nil), head.Branch...), contNode)
	altBranch := append(append([]string(nil), head.Branch...), altNode)
	f.idx.Nodes[contNode] = &forestNode{
		ID: contNode, Branch: contBranch, Parent: head.ID,
		Trunk: head.Trunk, Vector: append(append([]int(nil), head.Vector...), 0),
	}
	altTrunk := f.mintTrunk()
	f.idx.Nodes[altNode] = &forestNode{
		ID: altNode, Branch: altBranch, Parent: head.ID,
		Trunk: altTrunk, Vector: append(append([]int(nil), head.Vector...), 1),
	}
	// The existing trunk's head advances to the continuation.
	f.idx.Trunks[head.Trunk].Head = contNode
	f.idx.Trunks[head.Trunk].Tip = contTip
	f.idx.Trunks[altTrunk] = &trunkRec{ID: altTrunk, Head: altNode, Parent: head.Trunk, BranchedLT: branchedLT}
	return altTrunk
}

// trunkTail returns the head node's current main tail.
func (f *Forest) trunkTail(head *forestNode) (uint64, error) {
	x, err := Open(f.root, f.cfg, head.Branch...)
	if err != nil {
		return 0, err
	}
	defer x.Close()
	return mainTail(x), nil
}

// AppendChannel appends to a related channel of a trunk's head, tagged
// with the main LT it belongs to.
func (f *Forest) AppendChannel(trunk TrunkID, channel string, mainLT uint64, payload, meta []byte) (uint64, error) {
	head, err := f.headNode(trunk)
	if err != nil {
		return 0, err
	}
	x, err := Open(f.root, f.cfg, head.Branch...)
	if err != nil {
		return 0, err
	}
	defer x.Close()
	if mainLT == 0 {
		mainLT = mainTail(x) + 1 // reducible default: one ahead (the upcoming turn)
	}
	return x.Append(channel, mainLT, payload, meta)
}

// TrunkInfo is a read-only view of a trunk for listing.
type TrunkInfo struct {
	ID         string
	Head       string // head node id
	HeadBranch []string
	Parent     string
	BranchedLT uint64
	Tip        uint64
	Vector     []int
	Frozen     bool
}

// Trunks returns every trunk in the forest (creation order by id).
func (f *Forest) Trunks() []TrunkInfo {
	out := make([]TrunkInfo, 0, len(f.idx.Trunks))
	for i := 0; i < f.idx.TrunkSeq; i++ {
		id := "t" + strconv.Itoa(i)
		t := f.idx.Trunks[id]
		if t == nil {
			continue
		}
		ti := TrunkInfo{ID: t.ID, Head: t.Head, Parent: t.Parent, BranchedLT: t.BranchedLT, Tip: t.Tip}
		if n := f.idx.Nodes[t.Head]; n != nil {
			ti.HeadBranch = n.Branch
			ti.Vector = n.Vector
			ti.Frozen = n.Frozen
		}
		out = append(out, ti)
	}
	return out
}

// NodeInfo is a read-only view of a node.
type NodeInfo struct {
	ID       string
	Branch   []string
	Parent   string
	Children []string
	Frozen   bool
	Trunk    string
	Vector   []int
}

// Nodes returns every node in the forest (creation order by id).
func (f *Forest) Nodes() []NodeInfo {
	out := make([]NodeInfo, 0, len(f.idx.Nodes))
	for i := 0; i < f.idx.NodeSeq; i++ {
		id := "n" + strconv.Itoa(i)
		n := f.idx.Nodes[id]
		if n == nil {
			continue
		}
		out = append(out, NodeInfo{
			ID: n.ID, Branch: n.Branch, Parent: n.Parent, Children: n.Children,
			Frozen: n.Frozen, Trunk: n.Trunk, Vector: n.Vector,
		})
	}
	return out
}

// mainTail returns the main channel's last index for an opened branch.
func mainTail(x *XWAL) uint64 {
	return x.chans[x.main].log.LastIndex()
}
