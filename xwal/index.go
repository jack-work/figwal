// index.go: the node/trunk index, kept in a persistent tree.
//
// Trunks used to derive this by walking every node directory and reading a
// .trunk marker, twice per spawn, at roughly 0.9ms per existing node on a
// cold filesystem. Minting node 400 was a function of nodes 1 through 399.
//
// pstate gives O(log n) apply by path copying, lock-free readers, and a
// background writer that never blocks a caller. The .trunk markers stay
// ground truth; this is a maintained cache of them and RebuildFrom recovers
// it, which is why a failed index write is survivable and why it needs no
// write-ahead log.
package xwal

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/jack-work/pstate"
)

// Two namespaces in one tree, so a single Apply keeps them consistent. Parent
// and child are written together and therefore cannot disagree.
const (
	nodePfx  = "node/"
	trunkPfx = "trunk/"
)

// Index is the node/trunk index over a pstate model.
//
// seqMu guards only the mint counters. The tree itself needs no lock: readers
// take a root pointer, writers CAS a new one.
type Index struct {
	m *pstate.Model
	// rebuilds counts full marker walks. A spawn must not cause one; the
	// test asserts on it because the cost is syscalls, which a wall-clock
	// assertion only catches on a slow filesystem.
	rebuilds atomic.Uint64
	seqMu    sync.Mutex
	nodeSeq  int
	trunkSeq int
	mintID   func() string
}

// newIndex returns an empty index. Open fills it by walking the markers.
//
// It is deliberately memory-only. pstate can persist a Model, and an earlier
// pass did, but nothing read the file back: trusting a loaded index needs a
// validity marker so a reader can tell "lagging after a crash" from "current",
// and without one every reader would have to answer that for itself. The file
// was write-only, so it went. Re-add it with the marker, not before.
func newIndex(mintTrunkID func() string) *Index {
	return &Index{m: pstate.NewModel(pstate.Tree{}, nil), mintID: mintTrunkID}
}

func (x *Index) Close() error { return x.m.Close() }

// Version increases on every published change AND on every rebuild.
//
// pstate does not advance its version when a patch changes nothing, which is
// right for a data store and wrong for a topology cookie: Refresh means "I
// re-read the disk", and a consumer caching against this must invalidate even
// when the re-read found the same forest.
func (x *Index) Version() uint64 { return x.m.Snapshot().Version() + x.rebuilds.Load() }

func (x *Index) Node(key string) (*NodeInfo, bool) {
	v, ok := x.m.Get(nodePfx + key)
	if !ok {
		return nil, false
	}
	return toInfo(v)
}

func (x *Index) Head(trunk string) (string, bool) {
	v, ok := x.m.Get(trunkPfx + trunk)
	if !ok {
		return "", false
	}
	var head string
	if json.Unmarshal(v.Raw(), &head) != nil {
		return "", false
	}
	return head, true
}

func (x *Index) All() map[string]*NodeInfo {
	out := map[string]*NodeInfo{}
	x.m.Snapshot().Range(func(k string, v pstate.Value) bool {
		if strings.HasPrefix(k, nodePfx) {
			if n, ok := toInfo(v); ok {
				out[strings.TrimPrefix(k, nodePfx)] = n
			}
		}
		return true
	})
	return out
}

// LiveTrunks is every trunk with a live head, in stable (lexical) order.
// figaro mints opaque hex ids, so there is no numeric ordering to preserve.
func (x *Index) LiveTrunks() []string {
	var ids []string
	x.m.Snapshot().Range(func(k string, _ pstate.Value) bool {
		if strings.HasPrefix(k, trunkPfx) {
			ids = append(ids, strings.TrimPrefix(k, trunkPfx))
		}
		return true
	})
	sort.Strings(ids)
	return ids
}

// Spawn adds one leaf: the child node, the parent gaining it, and the child
// trunk's head. One Apply, so a reader never sees half of it.
func (x *Index) Spawn(parent, child, trunk string, isStump bool) error {
	snap := x.m.Snapshot()
	p := pstate.Patch{}
	if pn, ok := getNode(snap, parent); ok && !contains(pn.Children, child) {
		pn.Children = append(pn.Children, child)
		p = setNode(p, parent, pn)
		if pn.Trunk != "" && pn.Trunk != trunk {
			p = p.Delete(trunkPfx + pn.Trunk) // no longer a live head
		}
	}
	p = setNode(p, child, NodeInfo{Branch: strings.Split(child, "/"), Trunk: trunk, Parent: parent, IsStump: isStump})
	if trunk != "" {
		p = p.Set(trunkPfx+trunk, val(child))
	}
	x.bumpSeqs(child, trunk)
	_, err := x.m.Apply(p)
	return err
}

func (x *Index) Drop(nodeKeys, trunkIDs []string) error {
	snap := x.m.Snapshot()
	p := pstate.Patch{}
	for _, key := range nodeKeys {
		if n, ok := getNode(snap, key); ok {
			if pn, ok := getNode(snap, n.Parent); ok {
				pn.Children = remove(pn.Children, key)
				p = setNode(p, n.Parent, pn)
			}
		}
		p = p.Delete(nodePfx + key)
	}
	for _, id := range trunkIDs {
		p = p.Delete(trunkPfx + id)
	}
	next := snap.Tree().Apply(p)
	seen := map[string]bool{}
	rangeNodes(next, func(_ string, n NodeInfo) bool {
		if n.Trunk != "" && !seen[n.Trunk] {
			seen[n.Trunk] = true
			p = rehead(p, next, n.Trunk)
		}
		return true
	})
	_, err := x.m.Apply(p)
	return err
}

// RebuildFrom re-derives everything from the markers. The only path that walks.
func (x *Index) RebuildFrom(mainDir string) error {
	x.rebuilds.Add(1)
	fresh := pstate.Tree{}
	heads := map[string]string{}
	var walk func(dir string, branch []string, parentKey string, isRoot bool) error
	walk = func(dir string, branch []string, parentKey string, isRoot bool) error {
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
		n := NodeInfo{
			Branch: append([]string(nil), branch...), Trunk: trunkID, Parent: parentKey,
			IsRoot: isRoot, IsStump: !isRoot && trunkID == "" && len(branch) == 1,
		}
		for _, k := range kids {
			n.Children = append(n.Children, joinKey(branch, k))
		}
		fresh = fresh.Set(nodePfx+key, val(n))
		x.bumpSeqs(key, trunkID)
		if len(n.Children) == 0 && trunkID != "" {
			if prev, dup := heads[trunkID]; dup {
				return fmt.Errorf("xwal: trunk %q has multiple live heads %q and %q", trunkID, prev, key)
			}
			heads[trunkID] = key
			fresh = fresh.Set(trunkPfx+trunkID, val(key))
		}
		for _, k := range kids {
			if err := walk(filepath.Join(dir, k), append(append([]string(nil), branch...), k), key, false); err != nil {
				return err
			}
		}
		return nil
	}
	x.resetSeqs()
	if err := walk(mainDir, nil, "", true); err != nil {
		return err
	}
	// Replace wholesale: a rebuild is a repair, so anything not on disk is gone.
	p := pstate.Patch{}
	x.m.Snapshot().Range(func(k string, _ pstate.Value) bool { p = p.Delete(k); return true })
	fresh.Range(func(k string, v pstate.Value) bool { p = p.Set(k, v); return true })
	_, err := x.m.Apply(p)
	return err
}

func (x *Index) MintNode() string {
	x.seqMu.Lock()
	defer x.seqMu.Unlock()
	id := "n" + strconv.Itoa(x.nodeSeq)
	x.nodeSeq++
	return id
}

// MintNode and MintTrunk do not persist their counters and cannot fail.
//
// The alternative is a durable sequence, so a crash between minting an id and
// creating its directory cannot leak it. We are not doing that. MintNode's
// counter is recovered at open by RebuildFrom, which reads the n<N> suffixes
// off directory names; a leaked id costs one skipped integer per crash and is
// corrected on the next open. A durable counter would put a synchronous write
// on the create path, which is the path this exists to make cheap, to close a
// gap in a sequence nobody reads for meaning. Ids are opaque; gaps are not a
// defect. That is also why neither returns an error.
func (x *Index) MintTrunk() string {
	if x.mintID != nil {
		return x.mintID()
	}
	x.seqMu.Lock()
	defer x.seqMu.Unlock()
	id := "t" + strconv.Itoa(x.trunkSeq)
	x.trunkSeq++
	return id
}

func (x *Index) resetSeqs() {
	x.seqMu.Lock()
	x.nodeSeq, x.trunkSeq = 0, 0
	x.seqMu.Unlock()
}

func (x *Index) bumpSeqs(key, trunkID string) {
	x.seqMu.Lock()
	defer x.seqMu.Unlock()
	if key != "" {
		seg := key
		if i := strings.LastIndex(key, "/"); i >= 0 {
			seg = key[i+1:]
		}
		if n := suffixNum(seg, 'n'); n+1 > x.nodeSeq {
			x.nodeSeq = n + 1
		}
	}
	// Recover the trunk counter too. Dropping this reissued t0 after a
	// restart, so two nodes claimed one trunk and every open failed with
	// "multiple live heads" — which is what the crash harness caught.
	if n := suffixNum(trunkID, 't'); n+1 > x.trunkSeq {
		x.trunkSeq = n + 1
	}
}

// --- helpers ---

func val(v any) pstate.Value { pv, _ := pstate.EncodeValue(v); return pv }

func setNode(p pstate.Patch, key string, n NodeInfo) pstate.Patch {
	return p.Set(nodePfx+key, val(n))
}

func getNode(s pstate.Snapshot, key string) (NodeInfo, bool) {
	v, ok := s.Get(nodePfx + key)
	if !ok {
		return NodeInfo{}, false
	}
	var n NodeInfo
	return n, json.Unmarshal(v.Raw(), &n) == nil
}

func rangeNodes(t pstate.Tree, fn func(string, NodeInfo) bool) {
	t.Range(func(k string, v pstate.Value) bool {
		if !strings.HasPrefix(k, nodePfx) {
			return true
		}
		var n NodeInfo
		if json.Unmarshal(v.Raw(), &n) != nil {
			return true
		}
		return fn(strings.TrimPrefix(k, nodePfx), n)
	})
}

// rehead points trunk at its one live leaf, or drops it when it has none.
func rehead(p pstate.Patch, next pstate.Tree, trunk string) pstate.Patch {
	found := ""
	rangeNodes(next, func(key string, n NodeInfo) bool {
		if n.Trunk == trunk && len(n.Children) == 0 {
			found = key
			return false
		}
		return true
	})
	if found == "" {
		return p.Delete(trunkPfx + trunk)
	}
	return p.Set(trunkPfx+trunk, val(found))
}

func toInfo(v pstate.Value) (*NodeInfo, bool) {
	var n NodeInfo
	if json.Unmarshal(v.Raw(), &n) != nil {
		return nil, false
	}
	return &n, true
}

func suffixNum(s string, p byte) int {
	if len(s) < 2 || s[0] != p {
		return -1
	}
	n, err := strconv.Atoi(s[1:])
	if err != nil {
		return -1
	}
	return n
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
	out := make([]string, 0, len(s))
	for _, x := range s {
		if x != v {
			out = append(out, x)
		}
	}
	return out
}
