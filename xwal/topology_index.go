package xwal

import (
	"fmt"
	"sort"
	"strconv"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
)

// NodeInfo is one node of the fork forest. Treat it as immutable: readers may
// hold it while a mutation runs.
//
// Frozen is derived (len(Children) > 0), never stored, so it cannot disagree
// with the forest.
type NodeInfo struct {
	Branch   []string // path segments; key is strings.Join(Branch, "/")
	Trunk    string   // owning trunk id; "" for the root and for stumps
	Parent   string   // parent key; "" for the root
	Children []string // child keys
	IsRoot   bool
	IsStump  bool // markerless depth-1 child of the root (cauterization boundary)
}

func (n *NodeInfo) Frozen() bool { return len(n.Children) > 0 }

func (n *NodeInfo) stumpName() string {
	if n.IsStump && len(n.Branch) == 1 {
		return n.Branch[0]
	}
	return ""
}

// TopologyIndex answers "where does trunk T live" and "what is the forest
// shape". Trunks derives this today by walking every node directory and
// reading a .trunk marker; a consumer may supply a maintained index instead,
// which is what takes the walk off the spawn path.
//
// GROUND TRUTH DOES NOT MOVE. The .trunk and .fork markers stay authoritative.
// This is a cache of them, and RebuildFrom recovers it. That is why a failed
// index write is survivable and why it needs no write-ahead log.
//
// ORDERING CONTRACT, which is the whole safety argument: Trunks calls the
// mutators AFTER the filesystem mutation has committed, never before.
//
//	fs ok, index fails -> stale index, RebuildFrom repairs, nothing lost
//	fs fails           -> index untouched, still correct
//
// Reversed, a crash leaves the index naming a node that does not exist, which
// is unrecoverable because the thing you would recover from was never written.
// The index may lag the disk. It may never lead it.
//
// CONCURRENCY: mutators run under the topology lock and need not be safe
// against each other. Node, Head, Walk and Version are read from other
// goroutines during mutation and must be lock free.
type TopologyIndex interface {
	Node(key string) (*NodeInfo, bool)
	Head(trunk string) (key string, ok bool)
	Walk(func(key string, n *NodeInfo) bool)
	// All is a point-in-time copy, for the cold paths that iterate the forest
	// (promote, remove, lineage). Hot paths use Node/Head.
	All() map[string]*NodeInfo
	// LiveTrunks is every trunk with a head, in stable display order.
	LiveTrunks() []string
	Version() uint64

	Spawn(parent, child, trunk string, isStump bool) error
	Reassign(trunkByNodeKey map[string]string) error
	Drop(nodeKeys, trunkIDs []string) error
	RebuildFrom(mainDir string) error

	// MintNode and MintTrunk do not persist their counters and cannot fail.
	//
	// The alternative is a durable sequence, so that a crash between minting
	// an id and creating its directory cannot leak it. We are not doing that.
	// The counters are recovered at open by RebuildFrom, which reads the
	// "n<N>" and "t<N>" suffixes off directory names. A leaked id costs one
	// skipped integer per crash and is corrected on the next open.
	//
	// The trade is deliberate: a durable counter puts a synchronous write on
	// the create path, which is the path this exists to make cheap, to close a
	// gap in a sequence nobody reads for meaning. Ids are opaque, gaps are not
	// a defect. That is also why neither returns an error: nothing can fail.
	MintNode() string
	MintTrunk() string
}

// pickIndex returns the caller's index, or the default marker-walking one.
// A nil Config.Index keeps pre-interface behaviour exactly.
func pickIndex(cfg Config) TopologyIndex {
	if cfg.Index != nil {
		return cfg.Index
	}
	return newMemIndex(cfg.MintTrunkID)
}

// memIndex is the default: the maps Trunks used to own, plus the walk that
// filled them. Behaviour is identical to the pre-interface code.
type memIndex struct {
	mu       sync.RWMutex
	nodes    map[string]*NodeInfo
	heads    map[string]string
	nodeSeq  int
	trunkSeq int
	version  atomic.Uint64
	mintID   func() string // Config.MintTrunkID, or nil for sequential t<N>
}

func newMemIndex(mintID func() string) *memIndex {
	return &memIndex{nodes: map[string]*NodeInfo{}, heads: map[string]string{}, mintID: mintID}
}

func (m *memIndex) Node(key string) (*NodeInfo, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	n, ok := m.nodes[key]
	return n, ok
}

func (m *memIndex) Head(trunk string) (string, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	k, ok := m.heads[trunk]
	return k, ok
}

func (m *memIndex) Walk(fn func(string, *NodeInfo) bool) {
	m.mu.RLock()
	snapshot := make(map[string]*NodeInfo, len(m.nodes))
	for k, v := range m.nodes {
		snapshot[k] = v
	}
	m.mu.RUnlock()
	for k, v := range snapshot {
		if !fn(k, v) {
			return
		}
	}
}

func (m *memIndex) All() map[string]*NodeInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make(map[string]*NodeInfo, len(m.nodes))
	for k, v := range m.nodes {
		out[k] = v
	}
	return out
}

// LiveTrunks keeps the original ordering rule: numeric for the sequential
// t<N> ids (so t2 precedes t10), lexical for caller-minted opaque ids.
func (m *memIndex) LiveTrunks() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var ids []string
	if m.mintID != nil {
		for id := range m.heads {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		return ids
	}
	for i := 0; i < m.trunkSeq; i++ {
		if id := "t" + strconv.Itoa(i); m.heads[id] != "" {
			ids = append(ids, id)
		}
	}
	return ids
}

func (m *memIndex) Version() uint64 { return m.version.Load() }

func (m *memIndex) Spawn(parent, child, trunk string, isStump bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if p := m.nodes[parent]; p != nil && !contains(p.Children, child) {
		p.Children = append(p.Children, child)
		if p.Trunk != "" {
			delete(m.heads, p.Trunk) // a node with children is no longer a head
			if p.Trunk != trunk {
				m.reheadLocked(p.Trunk)
			}
		}
	}
	m.nodes[child] = &NodeInfo{
		Branch: strings.Split(child, "/"), Trunk: trunk, Parent: parent, IsStump: isStump,
	}
	if trunk != "" {
		m.heads[trunk] = child
	}
	m.bumpSeqsLocked(child, trunk)
	m.version.Add(1)
	return nil
}

func (m *memIndex) Reassign(trunkByNodeKey map[string]string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	touched := map[string]bool{}
	for key, trunk := range trunkByNodeKey {
		n := m.nodes[key]
		if n == nil {
			return fmt.Errorf("xwal: reassign unknown node %q", key)
		}
		touched[n.Trunk], touched[trunk] = true, true
		n.Trunk = trunk
	}
	for trunk := range touched {
		if trunk != "" {
			m.reheadLocked(trunk)
		}
	}
	m.version.Add(1)
	return nil
}

func (m *memIndex) Drop(nodeKeys, trunkIDs []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, key := range nodeKeys {
		if n := m.nodes[key]; n != nil {
			if p := m.nodes[n.Parent]; p != nil {
				p.Children = remove(p.Children, key)
			}
		}
		delete(m.nodes, key)
	}
	for _, id := range trunkIDs {
		delete(m.heads, id)
	}
	for _, n := range m.nodes {
		if n.Trunk != "" && !n.Frozen() {
			m.heads[n.Trunk] = strings.Join(n.Branch, "/")
		}
	}
	m.version.Add(1)
	return nil
}

// reheadLocked restores heads[trunk] to that trunk's one live leaf, or drops
// the entry when it has none.
func (m *memIndex) reheadLocked(trunk string) {
	delete(m.heads, trunk)
	for key, n := range m.nodes {
		if n.Trunk == trunk && !n.Frozen() {
			m.heads[trunk] = key
			return
		}
	}
}

func (m *memIndex) MintNode() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	name := fmt.Sprintf("n%d", m.nodeSeq)
	m.nodeSeq++
	return name
}

func (m *memIndex) MintTrunk() string {
	if m.mintID != nil {
		return m.mintID()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	id := fmt.Sprintf("t%d", m.trunkSeq)
	m.trunkSeq++
	return id
}

// RebuildFrom re-derives everything from the markers. The only path that walks.
func (m *memIndex) RebuildFrom(mainDir string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nodes, m.heads = map[string]*NodeInfo{}, map[string]string{}
	m.nodeSeq, m.trunkSeq = 0, 0
	if err := m.walkLocked(mainDir, nil, "", true); err != nil {
		return err
	}
	m.version.Add(1)
	return nil
}

func (m *memIndex) walkLocked(dir string, branch []string, parentKey string, isRoot bool) error {
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
	// Node class is positional plus marker-based: the root is the channel dir
	// itself; a stump is a markerless depth-1 child of it; anything with a
	// .trunk marker is a trunk.
	n := &NodeInfo{
		Branch: append([]string(nil), branch...), Trunk: trunkID, Parent: parentKey,
		IsRoot: isRoot, IsStump: !isRoot && trunkID == "" && len(branch) == 1,
	}
	for _, k := range kids {
		n.Children = append(n.Children, joinKey(branch, k))
	}
	m.nodes[key] = n
	m.bumpSeqsLocked(key, trunkID)
	if !n.Frozen() && trunkID != "" {
		if previous, exists := m.heads[trunkID]; exists {
			return fmt.Errorf("xwal: trunk %q has multiple live heads %q and %q", trunkID, previous, key)
		}
		m.heads[trunkID] = key
	}
	for _, k := range kids {
		if err := m.walkLocked(filepath.Join(dir, k), append(append([]string(nil), branch...), k), key, false); err != nil {
			return err
		}
	}
	return nil
}

func (m *memIndex) bumpSeqsLocked(key, trunkID string) {
	if key != "" {
		seg := key
		if i := strings.LastIndex(key, "/"); i >= 0 {
			seg = key[i+1:]
		}
		if n := numSuffix(seg, 'n'); n+1 > m.nodeSeq {
			m.nodeSeq = n + 1
		}
	}
	if n := numSuffix(trunkID, 't'); n+1 > m.trunkSeq {
		m.trunkSeq = n + 1
	}
}

func contains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

func remove(s []string, v string) []string {
	out := s[:0]
	for _, x := range s {
		if x != v {
			out = append(out, x)
		}
	}
	return out
}
