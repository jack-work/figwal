// flatten.go: the v3 -> v4 layout migration.
//
// A v3 store nested a node inside its parent's directory, so lineage WAS the
// path: ir/config@6bd4/n4/n6 read n6 as a child of n4. A v4 store puts every
// node at depth 1 and records lineage in the node's own .node marker.
//
// The flat build cannot open a nested store, and does not say so: Index
// RebuildFrom reads ONE directory, and a v3 node carries neither .node nor
// .from, so it is skipped as an unfinished fork. A real store opens
// "successfully" with its loadout stumps and none of its conversations.
// That silence is why this migration must run before anything walks.
//
// It moves directories and writes markers. No record is parsed, no log is
// opened, and NO FORK BASE IS EVER READ OR WRITTEN. An LT does not change
// when a directory moves: a base is relative to the parent's numbering, the
// parent keeps its own segments, and .node names the same parent the nesting
// did. Recomputing a base here would be a misunderstanding, not a repair.
package xwal

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/jack-work/figwal/disk"
	"github.com/jack-work/figwal/segment"
)

// FlattenPlan is every move and every marker, computed before anything is
// touched. The whole plan is built and validated first because a refusal
// discovered halfway through a directory move is worse than no refusal at
// all: the store would be part flat, part nested, and the caller would have
// an error message instead of a store.
type FlattenPlan struct {
	Root     string
	Main     string
	Channels []flattenChannelPlan

	// Nodes is the main channel's node count, Nested how many of them sit
	// below depth 1. Nested == 0 with Markers == 0 means the store is
	// already v4 and Apply is a no-op.
	Nodes   int
	Nested  int
	Moves   int
	Markers int
}

type flattenChannelPlan struct {
	Name  string
	Nodes []flattenNode
}

// flattenNode is one directory's fate. Rel is where it sits today
// (channel-relative), Flat where it will sit. Marker is non-nil only in the
// main channel, and only where a .node must be written.
type flattenNode struct {
	Rel    string
	Flat   string
	Marker *nodeMarker
}

// PlanFlatten reads the store's shape and computes the migration without
// modifying anything. It is safe to call on a live store; it takes no lock
// and writes nothing.
func PlanFlatten(root string) (*FlattenPlan, error) {
	chanList, main, codec, err := flattenManifest(root)
	if err != nil {
		return nil, err
	}
	plan := &FlattenPlan{Root: root, Main: main}

	// Main first: its tree is the lineage authority, and every other
	// channel is checked against it.
	ordered := append([]string{main}, without(chanList, main)...)
	parentOf := map[string]string{} // leaf -> its parent's leaf, per the main channel
	var collisions, divergent []string

	for _, ch := range ordered {
		dir := filepath.Join(root, ch)
		paths, err := nodePaths(dir)
		if err != nil {
			return nil, err
		}
		// Uniqueness is checked PER CHANNEL, at migration time. That the
		// trees are identical in shape is an observation about one store,
		// not a property of the format.
		claimed := map[string]string{}
		cp := flattenChannelPlan{Name: ch}
		for _, rel := range paths {
			leaf := filepath.Base(rel)
			if owner, taken := claimed[leaf]; taken {
				collisions = append(collisions, fmt.Sprintf("%s: %q and %q both flatten to %q", ch, owner, rel, leaf))
				continue
			}
			claimed[leaf] = rel
			if ch == main {
				parentOf[leaf] = mainParentLeaf(dir, rel)
			} else if p := parentLeaf(rel); p != "" {
				// A channel node still nested under something the main
				// channel does not call its parent. Lineage comes from main,
				// so flattening would re-home this channel's data: the base
				// in its .fork is an index in ITS parent's numbering.
				//
				// Only a MISMATCH is a divergence. A node main has already
				// moved is not one -- that is what an interrupted migration
				// looks like from here, and it must resume, not refuse.
				if want, known := parentOf[leaf]; known && want != p {
					divergent = append(divergent, fmt.Sprintf(
						"%s: %q is nested under %q, but the main channel's lineage says %q", ch, rel, p, want))
				}
			}
			if err := checkForkMarker(dir, rel, codec); err != nil {
				return nil, err
			}
			node := flattenNode{Rel: rel, Flat: leaf}
			if ch == main {
				m, need, err := plannedMarker(dir, rel)
				if err != nil {
					return nil, err
				}
				if need {
					node.Marker = &m
					plan.Markers++
				}
			}
			if rel != leaf {
				plan.Moves++
			}
			cp.Nodes = append(cp.Nodes, node)
		}
		// Deepest first, so a node's descendants have already left before it
		// moves. Renaming a parent otherwise carries its children with it and
		// makes every path recorded below it stale.
		sort.Slice(cp.Nodes, func(i, j int) bool {
			di, dj := pathDepth(cp.Nodes[i].Rel), pathDepth(cp.Nodes[j].Rel)
			if di != dj {
				return di > dj
			}
			return cp.Nodes[i].Rel < cp.Nodes[j].Rel
		})
		if ch == main {
			plan.Nodes = len(cp.Nodes)
			for _, n := range cp.Nodes {
				if pathDepth(n.Rel) > 1 {
					plan.Nested++
				}
			}
		}
		plan.Channels = append(plan.Channels, cp)
	}
	if len(collisions) > 0 || len(divergent) > 0 {
		return nil, fmt.Errorf("xwal: cannot flatten %s:\n  %s\n"+
			"name mangling is deliberately not implemented; a store that needs it "+
			"must be migrated by hand",
			root, strings.Join(append(collisions, divergent...), "\n  "))
	}
	return plan, nil
}

// parentLeaf is the leaf name of a nested path's parent, empty at depth 1.
func parentLeaf(rel string) string {
	if parent := filepath.Dir(rel); parent != "." {
		return filepath.Base(parent)
	}
	return ""
}

// mainParentLeaf is a main-channel node's parent, from whichever of the two
// records it has: its nesting if it is still nested, its .node if it has
// already been moved by an interrupted run.
func mainParentLeaf(mainDir, rel string) string {
	if p := parentLeaf(rel); p != "" {
		return p
	}
	if m, ok := readNodeMarker(filepath.Join(mainDir, rel)); ok {
		return m.from
	}
	return ""
}

// readLegacyPair reads the pre-.node identity: .from holding "parent\nkind"
// and .trunk holding the trunk id. It lives here, in the migration, and
// nowhere else -- the read path no longer knows these files exist, so a
// build either understands a store or refuses it.
func readLegacyPair(dir string) (n nodeMarker, ok bool) {
	b, err := os.ReadFile(filepath.Join(dir, legacyFromName))
	if err != nil {
		return nodeMarker{}, false
	}
	// Split before trimming: an empty parent leaves a leading newline that
	// TrimSpace would eat, collapsing two fields into one.
	parts := strings.SplitN(string(b), "\n", 2)
	if len(parts) != 2 {
		return nodeMarker{}, false
	}
	n.from, n.kind = strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
	if tb, terr := os.ReadFile(filepath.Join(dir, legacyTrunkName)); terr == nil {
		n.trunk = strings.TrimSpace(string(tb))
	}
	return n, true
}

// NeedsFlatten reports whether a store must be migrated before it can be
// opened. It reads one file, so it is cheap enough for every open. A
// directory with no manifest is not a store yet and needs nothing.
func NeedsFlatten(root string) (bool, error) {
	m, err := readManifestFile(root)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	return m.Layout != layoutVersion, nil
}

// errNoForkBase reports a node directory with neither a .fork marker nor a
// segment at index 1. A missing .fork means "owns its log from 1" to this
// build (validForkNode, ensureBackfillFork), which the move preserves. With
// no segment at 1 the base is not that: it would be derived at open from the
// node's parent, and after flattening that parent is the channel root rather
// than the node it was nested under. Refused, because a silently rebased
// channel looks like data loss months later.
type errNoForkBase struct{ ch, rel string }

func (e *errNoForkBase) Error() string {
	return fmt.Sprintf("xwal: node %q in channel %q has no .fork and no segment at index 1; "+
		"its base is defined by its nesting and flattening would change it", e.rel, e.ch)
}

func checkForkMarker(chanDir, rel string, codec segment.SegmentCodec) error {
	dir := filepath.Join(chanDir, rel)
	if _, err := readForkBaseFile(filepath.Join(dir, ".fork")); err == nil {
		return nil
	}
	first, ok, err := firstSegmentBase(dir, codec)
	if err != nil {
		return err
	}
	if ok && first == 1 {
		return nil // "owns its log from 1", which the move preserves
	}
	return &errNoForkBase{ch: filepath.Base(chanDir), rel: rel}
}

// plannedMarker derives one main-channel node's .node from where it sits:
// from is its parent's flat name (empty at depth 1, whose parent is the
// root), trunk comes from the legacy .trunk, and kind follows from the
// trunk — a v3 node with a trunk id is a conversation, and the nodes without
// one are the loadout stumps. need is false when the node already has a
// .node, so a resumed migration counts no work it is redoing.
func plannedMarker(chanDir, rel string) (m nodeMarker, need bool, err error) {
	dir := filepath.Join(chanDir, rel)
	if _, ok := readNodeMarker(dir); ok {
		return nodeMarker{}, false, nil
	}
	// A store written after the flattening but before the markers merged is
	// already at depth 1 and carries the .from/.kind pair. Its identity is
	// on disk in full, so it is taken as written rather than re-derived.
	if legacy, ok := readLegacyPair(dir); ok {
		return legacy, true, nil
	}
	m.from = parentLeaf(rel)
	if b, rerr := os.ReadFile(filepath.Join(dir, legacyTrunkName)); rerr == nil {
		m.trunk = strings.TrimSpace(string(b))
	}
	m.kind = "loadout"
	if m.trunk != "" {
		// Including a depth-1 node with a trunk: a conversation forked from
		// the null root. It is already where it belongs and only wants a
		// marker.
		m.kind = "conversation"
	}
	return m, true, nil
}

// legacyLayoutVersion is the nested layout Flatten migrates FROM. A store
// that has been through it records it, and that record is the only thing
// that sanctions the migration-era cursor fallback in CursorAt.
const legacyLayoutVersion = 3

// FlattenReport is what Apply did, in counts. Counts rather than durations:
// this runs once, on stores that differ by two orders of magnitude in size
// and on filesystems 60x apart in per-file cost.
type FlattenReport struct {
	Nodes   int
	Moved   int
	Markers int
	Retired int // legacy .trunk/.from files removed
}

// Flatten plans and applies in one call, under the store lock, so it refuses
// to run against a live daemon.
//
// It is RESUMABLE from any interrupted state and needs no journal to be:
// every state it can stop in is re-derivable from the tree itself. An
// unmoved node still names its parent by nesting; a moved node carries that
// parent in its .node. The one ordering that matters is in the main channel
// — the marker is written and FSYNCED (writeSyncedFile) before the rename
// that destroys the nesting it was derived from — so no crash can leave a
// node at depth 1 with no record of where it came from.
//
// It is NOT atomic. Interrupted, it leaves a store part flat and part
// nested. Such a store must be re-flattened, not opened; NestedNodes is how
// a caller tells the difference.
func Flatten(root string) (FlattenReport, error) {
	plan, err := PlanFlatten(root)
	if err != nil {
		return FlattenReport{}, err
	}
	lock, err := lockRoot(root)
	if err != nil {
		return FlattenReport{}, err
	}
	defer unlockRoot(lock)
	// The shape can have changed between planning and locking, so plan
	// again now that we are the only writer. This is the plan that runs.
	plan, err = PlanFlatten(root)
	if err != nil {
		return FlattenReport{}, err
	}
	return plan.apply()
}

func (p *FlattenPlan) apply() (FlattenReport, error) {
	rep := FlattenReport{Nodes: p.Nodes}
	for _, cp := range p.Channels {
		dir := filepath.Join(p.Root, cp.Name)
		for _, n := range cp.Nodes {
			src := filepath.Join(dir, n.Rel)
			if n.Marker != nil {
				if err := writeNodeMarker(src, *n.Marker); err != nil {
					return rep, err
				}
				rep.Markers++
				// Only once .node is durable: one identity file per node, so
				// no later reader has two to disagree about. Best effort, as
				// on the read path in index.go — a full or read-only volume
				// must not strand a half-migrated store.
				for _, legacy := range []string{legacyTrunkName, legacyFromName} {
					if err := os.Remove(filepath.Join(src, legacy)); err == nil {
						rep.Retired++
					}
				}
			}
			if n.Rel == n.Flat {
				continue // already at depth 1 under its final name
			}
			if err := os.Rename(src, filepath.Join(dir, n.Flat)); err != nil {
				return rep, fmt.Errorf("xwal: flatten %s/%s: %w", cp.Name, n.Rel, err)
			}
			rep.Moved++
		}
		if err := disk.SyncDir(dir); err != nil {
			return rep, err
		}
	}
	// The stamp goes LAST, after every channel is moved and synced. It is
	// the store's only claim to be v4, and openTrunks refuses without it, so
	// an interrupted migration must leave it absent: a half-flat store has
	// to be re-flattened, never opened.
	m, err := readManifestFile(p.Root)
	if err != nil {
		return rep, err
	}
	if m.Layout != layoutVersion {
		m.LayoutFrom, m.Layout = legacyLayoutVersion, layoutVersion
		if err := writeManifest(p.Root, m); err != nil {
			return rep, err
		}
	}
	return rep, nil
}

// NestedNodes counts node directories below depth 1 across every channel,
// without changing anything. Zero means flat. It is both the gate a caller
// uses to decide whether to migrate and the assertion that one finished.
func NestedNodes(root string) (int, error) {
	chanList, _, _, err := flattenManifest(root)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, ch := range chanList {
		paths, err := nodePaths(filepath.Join(root, ch))
		if err != nil {
			return 0, err
		}
		for _, rel := range paths {
			if pathDepth(rel) > 1 {
				n++
			}
		}
	}
	return n, nil
}

// nodePaths is every node directory under a channel dir, as channel-relative
// paths, parents before children. A node directory is any directory whose
// name does not begin with a dot.
func nodePaths(chanDir string) ([]string, error) {
	if _, err := os.Stat(chanDir); os.IsNotExist(err) {
		return nil, nil
	}
	var out []string
	var walk func(rel string) error
	walk = func(rel string) error {
		ents, err := os.ReadDir(filepath.Join(chanDir, rel))
		if err != nil {
			return err
		}
		for _, e := range ents {
			if !e.IsDir() || strings.HasPrefix(e.Name(), ".") {
				continue
			}
			child := e.Name()
			if rel != "" {
				child = filepath.Join(rel, e.Name())
			}
			out = append(out, child)
			if err := walk(child); err != nil {
				return err
			}
		}
		return nil
	}
	if err := walk(""); err != nil {
		return nil, err
	}
	return out, nil
}

func pathDepth(rel string) int { return strings.Count(rel, string(os.PathSeparator)) + 1 }

// flattenManifest reads the channel list, the main channel's name and the
// codec. Detection and migration both go through it, so neither can invent a
// channel the store does not have.
func flattenManifest(root string) (chans []string, main string, codec segment.SegmentCodec, err error) {
	m, err := readManifestFile(root)
	if err != nil {
		return nil, "", nil, err
	}
	if codec, err = codecByName(m.Codec); err != nil {
		return nil, "", nil, err
	}
	for _, c := range m.Channels {
		chans = append(chans, c.Name)
	}
	return chans, m.Main, codec, nil
}

func without(all []string, drop string) []string {
	out := make([]string, 0, len(all))
	for _, s := range all {
		if s != drop {
			out = append(out, s)
		}
	}
	return out
}
