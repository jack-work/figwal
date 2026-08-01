// index.go: the node/trunk index.
//
// Trunks used to derive this by walking every node directory and reading a
// .trunk marker, twice per spawn, at roughly 0.9ms per existing node on a cold
// filesystem. Minting node 400 was a function of nodes 1 through 399. Now a
// spawn patches the index with the delta it already knows and never walks.
package xwal

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// Index is the node/trunk index: the forest shape, and where each trunk's
// live head is.
//
// Readers (Node, Head, All, LiveTrunks, Version) run on RPC goroutines while
// a mutation is in flight, so they take the read lock. Mutators run under the
// topology lock and need not be safe against each other.
//
// The .trunk markers on disk stay ground truth; this is a derived cache and
// RebuildFrom recovers it.
type Index struct {
	mu       sync.RWMutex
	nodes    map[string]*NodeInfo
	heads    map[string]string // trunk id -> its one live leaf
	nodeSeq  int
	trunkSeq int
	version  atomic.Uint64
	// rebuilds counts full marker walks. A spawn must cause none; the test
	// asserts on the count because the cost is syscalls, which a wall-clock
	// assertion only catches on a slow filesystem.
	rebuilds atomic.Uint64
	mintID   func() string
}

func newIndex(mintTrunkID func() string) *Index {
	return &Index{nodes: map[string]*NodeInfo{}, heads: map[string]string{}, mintID: mintTrunkID}
}

func (x *Index) Node(key string) (*NodeInfo, bool) {
	x.mu.RLock()
	defer x.mu.RUnlock()
	n, ok := x.nodes[key]
	return n, ok
}

func (x *Index) Head(trunk string) (string, bool) {
	x.mu.RLock()
	defer x.mu.RUnlock()
	k, ok := x.heads[trunk]
	return k, ok
}

// All is a point-in-time copy, for the cold paths that iterate the forest
// (promote, remove, lineage). Hot paths use Node/Head.
func (x *Index) All() map[string]*NodeInfo {
	x.mu.RLock()
	defer x.mu.RUnlock()
	out := make(map[string]*NodeInfo, len(x.nodes))
	for k, v := range x.nodes {
		out[k] = v
	}
	return out
}

// LiveTrunks is every trunk with a head, in stable display order: numeric for
// sequential t<N> ids so t2 precedes t10, lexical for caller-minted ids.
func (x *Index) LiveTrunks() []string {
	x.mu.RLock()
	defer x.mu.RUnlock()
	var ids []string
	if x.mintID != nil {
		for id := range x.heads {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		return ids
	}
	for i := 0; i < x.trunkSeq; i++ {
		if id := "t" + strconv.Itoa(i); x.heads[id] != "" {
			ids = append(ids, id)
		}
	}
	return ids
}

// Version increases on every change AND on every rebuild. Consumers cache
// derived state against it; Refresh means "I re-read the disk", so it must
// invalidate even when the forest came back identical.
func (x *Index) Version() uint64 { return x.version.Load() }

// SpawnFlat records a sibling node forked from parent at LT. The parent is
// not touched: no child list, no freeze, no continuation.
func (x *Index) SpawnFlat(parent, child, trunk, kind string) {
	x.mu.Lock()
	defer x.mu.Unlock()
	x.nodes[child] = &NodeInfo{Trunk: trunk, From: parent, Kind: kind}
	if trunk != "" {
		x.heads[trunk] = child
	}
	x.bumpSeqsLocked(child, trunk)
	x.version.Add(1)
}

// ChildrenOf is every node forked from key. Derived, not stored: a flat
// fork writes only the child's own .from, so the parent has no list.
//
// Calling this in a loop over the forest is O(n^2); use ChildIndex once.
func (x *Index) ChildrenOf(key string) []string {
	x.mu.RLock()
	defer x.mu.RUnlock()
	var out []string
	for k, n := range x.nodes {
		if n.From == key {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

// ChildIndex is the whole from -> children adjacency in one pass under one
// lock, for callers that would otherwise ask per node.
func (x *Index) ChildIndex() map[string][]string {
	x.mu.RLock()
	defer x.mu.RUnlock()
	out := make(map[string][]string, len(x.nodes))
	for k, n := range x.nodes {
		out[n.From] = append(out[n.From], k)
	}
	for _, v := range out {
		sort.Strings(v)
	}
	return out
}

// Len is the node count, a bound for lineage climbs.
func (x *Index) Len() int {
	x.mu.RLock()
	defer x.mu.RUnlock()
	return len(x.nodes)
}

// ParentOf is the flat lineage link, for xwal.Config.ParentOf.
func (x *Index) ParentOf(node string) string {
	x.mu.RLock()
	defer x.mu.RUnlock()
	if n := x.nodes[node]; n != nil {
		return n.From
	}
	return ""
}

// MintNode and MintTrunk do not persist their counters and cannot fail.
// RebuildFrom recovers them from the n<N>/t<N> suffixes of directory names, so
// a crash between minting an id and creating its directory leaks one integer
// and self-corrects. Ids are opaque; gaps are not a defect.
func (x *Index) MintNode() string {
	x.mu.Lock()
	defer x.mu.Unlock()
	id := "n" + strconv.Itoa(x.nodeSeq)
	x.nodeSeq++
	return id
}

func (x *Index) MintTrunk() string {
	if x.mintID != nil {
		return x.mintID()
	}
	x.mu.Lock()
	defer x.mu.Unlock()
	id := "t" + strconv.Itoa(x.trunkSeq)
	x.trunkSeq++
	return id
}

// RebuildFrom re-derives everything from the markers. The only path that walks,
// and it walks one directory: every node is depth-1.
func (x *Index) RebuildFrom(mainDir string) error {
	x.rebuilds.Add(1)
	x.mu.Lock()
	defer x.mu.Unlock()
	x.nodes, x.heads = map[string]*NodeInfo{}, map[string]string{}
	x.nodeSeq, x.trunkSeq = 0, 0
	if err := x.addLocked(mainDir, "", true); err != nil {
		return err
	}
	ents, err := os.ReadDir(mainDir)
	if err != nil {
		return err
	}
	for _, e := range ents {
		if !e.IsDir() || strings.HasPrefix(e.Name(), ".") {
			continue
		}
		if err := x.addLocked(filepath.Join(mainDir, e.Name()), e.Name(), false); err != nil {
			return err
		}
	}
	x.version.Add(1)
	return nil
}

// addLocked records one node from its markers. Lineage is .from; the
// directory says nothing. A fork does not freeze its parent, so a node
// with a trunk id is that trunk's live head, forks or no forks.
func (x *Index) addLocked(dir, key string, isRoot bool) error {
	from, kind, flat := readFlatMarker(dir)
	if !flat && !isRoot {
		return nil // an unfinished fork: .from is the commit point
	}
	if isRoot {
		kind = "null"
	}
	trunkID, _ := readTrunkID(dir)
	x.nodes[key] = &NodeInfo{Trunk: trunkID, IsRoot: isRoot, From: from, Kind: kind}
	x.bumpSeqsLocked(key, trunkID)
	if trunkID == "" {
		return nil
	}
	if previous, exists := x.heads[trunkID]; exists {
		return fmt.Errorf("xwal: trunk %q has multiple live heads %q and %q", trunkID, previous, key)
	}
	x.heads[trunkID] = key
	return nil
}

func (x *Index) bumpSeqsLocked(key, trunkID string) {
	if key != "" {
		seg := key
		if i := strings.LastIndex(key, "/"); i >= 0 {
			seg = key[i+1:]
		}
		if n := numSuffix(seg, 'n'); n+1 > x.nodeSeq {
			x.nodeSeq = n + 1
		}
	}
	// Recovering the TRUNK counter matters as much as the node one: dropping
	// it reissued t0 after a restart, two nodes claimed one trunk, and every
	// open failed with "multiple live heads". The crash harness caught it.
	if n := numSuffix(trunkID, 't'); n+1 > x.trunkSeq {
		x.trunkSeq = n + 1
	}
}

// .from is "<parent>\n<kind>\n". Its absence marks the null root.
// At is not stored: it is the channel's own .fork base minus one.
const flatMarkerName = ".from"

func writeFlatMarker(dir, parent, kind string) error {
	return writeSyncedFile(filepath.Join(dir, flatMarkerName), fmt.Appendf(nil, "%s\n%s\n", parent, kind))
}

func readFlatMarker(dir string) (string, string, bool) {
	b, err := os.ReadFile(filepath.Join(dir, flatMarkerName))
	if err != nil {
		return "", "", false
	}
	// Split before trimming: an empty parent leaves a leading newline that
	// TrimSpace would eat, collapsing two fields into one.
	parts := strings.SplitN(string(b), "\n", 2)
	if len(parts) != 2 {
		return "", "", false
	}
	return strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1]), true
}
