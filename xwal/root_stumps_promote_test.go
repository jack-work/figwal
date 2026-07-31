package xwal

import (
	"os"
	"path/filepath"
	"sort"
	"testing"
)

// relDirs returns every directory under base, relative to base, sorted —
// the node-tree shape of a channel (excluding the base itself).
func relDirs(t *testing.T, base string) []string {
	t.Helper()
	var out []string
	filepath.WalkDir(base, func(p string, d os.DirEntry, err error) error {
		if err != nil || !d.IsDir() || p == base {
			return nil
		}
		rel, _ := filepath.Rel(base, p)
		out = append(out, rel)
		return nil
	})
	sort.Strings(out)
	return out
}

// --- Task 1: channel root-and-backfill ---

func TestAddChannel_RootAndBackfill(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "f")
	f, err := createTrunks(dir, trunksCfg())
	if err != nil {
		t.Fatal(err)
	}
	cleanupTrunks(t, f)
	// A stump + 2 trunks + 1 branch.
	if err := f.CreateStump("cfg@d880"); err != nil {
		t.Fatal(err)
	}
	tA, err := f.SpawnUnderStump("cfg@d880")
	if err != nil {
		t.Fatal(err)
	}
	tB, err := f.SpawnUnderStump("cfg@d880")
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range []string{`"a1"`, `"a2"`} {
		if _, _, err := f.Append(tA, 0, []byte(m), nil); err != nil {
			t.Fatal(err)
		}
	}
	branch, err := f.ForkTail(tA) // tA continues; branch is a new alternative
	if err != nil {
		t.Fatal(err)
	}

	// The translations channel does not exist yet (lazy add).
	irDirs := relDirs(t, filepath.Join(dir, "ir"))

	// Add + backfill the channel (note the slashed name).
	const ch = "translations/anthropic"
	x, err := Open(dir, trunksCfg())
	if err != nil {
		t.Fatal(err)
	}
	if err := x.addChannel(ChannelSpec{Name: ch, Kind: ChannelLog}); err != nil {
		t.Fatalf("add channel: %v", err)
	}
	x.Close()

	// The new channel's node tree mirrors ir's exactly — no stray
	// "anthropic/anthropic" from the slash, no flattening.
	chDirs := relDirs(t, filepath.Join(dir, ch))
	if len(chDirs) != len(irDirs) {
		t.Fatalf("channel tree does not mirror ir:\n ir=%v\n ch=%v", irDirs, chDirs)
	}
	for i := range irDirs {
		if chDirs[i] != irDirs[i] {
			t.Fatalf("channel dir mismatch at %d: ir=%q ch=%q\n ir=%v\n ch=%v",
				i, irDirs[i], chDirs[i], irDirs, chDirs)
		}
	}

	// Sane forkBases: every backfilled (non-root) node has base=1 (an empty
	// channel forks at its own first index everywhere), no segments.
	filepath.WalkDir(filepath.Join(dir, ch), func(p string, d os.DirEntry, err error) error {
		if err != nil || !d.IsDir() {
			return nil
		}
		fork := filepath.Join(p, ".fork")
		if b, rerr := os.ReadFile(fork); rerr == nil {
			if string(b) != "base=1\n" {
				t.Errorf("backfilled node %q: .fork = %q, want base=1", p, b)
			}
		}
		// No segment files in backfilled log nodes (content derivable).
		ents, _ := os.ReadDir(p)
		for _, e := range ents {
			if filepath.Ext(e.Name()) == ".jsonl" {
				t.Errorf("backfilled log node %q has a segment %q (should be empty)", p, e.Name())
			}
		}
		return nil
	})

	// A write to each trunk lands in its own node and reads back (prefix
	// read-through: the empty inherited prefix resolves up the chain).
	for _, tr := range []TrunkID{tA, tB, branch} {
		xw, err := f.Head(tr)
		if err != nil {
			t.Fatalf("head %s: %v", tr, err)
		}
		lt := channelTail(xw, "ir")
		chLT, err := xw.Append(ch, lt, []byte(`["translated"]`), nil)
		if err != nil {
			xw.Close()
			t.Fatalf("append translation on %s: %v", tr, err)
		}
		// The write lands in this trunk's own node (empty backfilled prefix:
		// own range starts at channel-LT 1) and reads back through the handle.
		if chLT != 1 {
			t.Errorf("trunk %s: first translation landed at chLT %d, want 1", tr, chLT)
		}
		_, p, rerr := xw.Read(ch, chLT)
		xw.Close()
		if rerr != nil {
			t.Fatalf("read translation on %s: %v", tr, rerr)
		}
		if string(p) != `["translated"]` {
			t.Fatalf("trunk %s translation round-trip = %q", tr, p)
		}
	}
}

func channelTail(x *XWAL, name string) uint64 {
	for _, c := range x.Channels() {
		if c.Name == name {
			return c.Last
		}
	}
	return 0
}

// A reducible channel backfills each node with the Initial watermark seed.
func TestAddChannel_BackfillReducible(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "f")
	f, conv := seedTrunk(t, dir)
	f.Append(conv, 0, []byte(`"u1"`), nil)

	x, err := Open(dir, trunksCfg())
	if err != nil {
		t.Fatal(err)
	}
	if err := x.addChannel(ChannelSpec{Name: "scratch", Kind: ChannelReducible, Reducer: "jsonmerge"}); err != nil {
		t.Fatalf("add reducible: %v", err)
	}
	x.Close()

	// The conversation head can fold + append to the new reducible channel.
	xw, err := f.Head(conv)
	if err != nil {
		t.Fatal(err)
	}
	defer xw.Close()
	if _, err := xw.Append("scratch", channelTail(xw, "ir"), []byte(`{"set":{"k":"v"}}`), nil); err != nil {
		t.Fatalf("append to backfilled reducible: %v", err)
	}
	st, err := xw.StateAt("scratch", channelTail(xw, "scratch"))
	if err != nil {
		t.Fatalf("StateAt backfilled reducible: %v", err)
	}
	if string(st) != `{"k":"v"}` {
		t.Fatalf("folded reducible state = %s, want {\"k\":\"v\"}", st)
	}
}

// A fork AFTER a slashed channel was lazily added must joint-fork that
// channel too (keeping the tree mirrored, no stray "anthropic/anthropic"),
// and both children inherit the parent's content.
func TestAddChannel_ForkAfterBackfill(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "f")
	f, conv := seedTrunkBirth(t, dir, "L@h")
	f.Append(conv, 0, []byte(`"hello"`), nil)

	const ch = "translations/anthropic"
	x, err := Open(dir, trunksCfg())
	if err != nil {
		t.Fatal(err)
	}
	if err := x.addChannel(ChannelSpec{Name: ch, Kind: ChannelLog}); err != nil {
		t.Fatal(err)
	}
	x.Close()

	// Write a translation to the conversation head.
	xw, _ := f.Head(conv)
	if _, err := xw.Append(ch, channelTail(xw, "ir"), []byte(`["xlate"]`), nil); err != nil {
		t.Fatal(err)
	}
	xw.Close()

	// Fork: the joint fork must fork the slashed channel as well.
	alt, err := f.ForkTail(conv)
	if err != nil {
		t.Fatal(err)
	}

	// No stray nested dir from the slash.
	for _, p := range relDirs(t, filepath.Join(dir, ch)) {
		if filepath.Base(p) == "anthropic" {
			t.Fatalf("stray nested dir from slashed channel name: %q", p)
		}
	}
	// The channel tree mirrors ir after the fork.
	if got, want := relDirs(t, filepath.Join(dir, ch)), relDirs(t, filepath.Join(dir, "ir")); len(got) != len(want) {
		t.Fatalf("channel tree diverged from ir after fork:\n ir=%v\n ch=%v", want, got)
	}
	// Both the continuation and the alternative inherit the translation.
	for _, tr := range []TrunkID{conv, alt} {
		h, err := f.Head(tr)
		if err != nil {
			t.Fatalf("head %s: %v", tr, err)
		}
		_, p, rerr := h.Read(ch, 1) // inherited entry at channel-LT 1
		h.Close()
		if rerr != nil || string(p) != `["xlate"]` {
			t.Fatalf("trunk %s did not inherit translation: p=%q err=%v", tr, p, rerr)
		}
	}
}

// --- Task 2: root & stumps (boundary semantics) ---

func TestStumps_OwnerAndBoundary(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "f")
	f, conv := seedTrunkBirth(t, dir, "cfg@d880")
	f.Append(conv, 0, []byte(`"u1"`), nil)

	// Owner of the genesis LT is the root; the birth LT is the stump.
	if o, _ := f.Owner(conv, 1); !o.IsRoot {
		t.Fatalf("LT1 owner = %+v, want root", o)
	}
	if o, _ := f.Owner(conv, 2); o.Stump != "cfg@d880" {
		t.Fatalf("LT2 owner = %+v, want stump", o)
	}
	// A second conversation under the same stump shares the birth prefix.
	conv2, err := f.SpawnUnderStump("cfg@d880")
	if err != nil {
		t.Fatal(err)
	}
	x, _ := f.Head(conv2)
	if _, p, _ := x.Read("ir", 2); string(p) != `"loadout-birth"` {
		x.Close()
		t.Fatalf("conv2 did not inherit the stump birth: %q", p)
	}
	x.Close()
	// A loadoutless trunk directly under the root, and reopen survives.
	rootTrunk, err := f.SpawnUnderRoot()
	if err != nil {
		t.Fatal(err)
	}
	r2, _ := openTrunks(dir, trunksCfg())
	cleanupTrunks(t, r2)
	if ti := findTrunk(r2.List(), rootTrunk); ti == nil || ti.Stump != "" {
		t.Fatalf("loadoutless trunk should have no stump: %+v", ti)
	}
	if ti := findTrunk(r2.List(), conv); ti == nil || ti.Stump != "cfg@d880" {
		t.Fatalf("conv should be rooted at its stump: %+v", ti)
	}
}

func findTrunk(infos []TrunkInfo, id TrunkID) *TrunkInfo {
	for i := range infos {
		if infos[i].ID == id {
			return &infos[i]
		}
	}
	return nil
}

// --- Task 3: Promote ---

// parentRun returns the trunk id of the node directly above a trunk's
// founding node, and whether that node is a stump.
func parentRun(t *testing.T, f *Trunks, trunk TrunkID) (TrunkID, bool) {
	t.Helper()
	fk, ok := f.foundingNode(trunk)
	if !ok {
		t.Fatalf("no founding node for %s", trunk)
	}
	p := f.node(f.node(fk).Parent)
	return p.Trunk, p.Kind == "loadout"
}

func TestPromote_ClimbsAndStopsAtStump(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "f")
	f, c := seedTrunkBirth(t, dir, "cfg@d880") // t0 = C
	for _, m := range []string{`"m1"`, `"m2"`, `"m3"`} {
		f.Append(c, 0, []byte(m), nil)
	}
	// Interior fork C -> B; extend B; interior fork B -> A.
	b, _ := forkAppend(t, f, c, 4, []byte(`"fromB"`))
	f.Append(b, 0, []byte(`"b2"`), nil)
	bx, _ := f.Head(b)
	bTail := mainTail(bx)
	bx.Close()
	a, _ := forkAppend(t, f, b, bTail-1, []byte(`"fromA"`))

	// A's lineage: parent run is B, then C, then the stump.
	if id, _ := parentRun(t, f, a); id != b {
		t.Fatalf("pre-promote: A's parent run = %s, want B(%s)", id, b)
	}

	// Promote A one level: it absorbs B's above-divergence run; its parent
	// run becomes C.
	n, err := f.Promote(a, 1)
	if err != nil || n != 1 {
		t.Fatalf("promote 1: n=%d err=%v", n, err)
	}
	if id, _ := parentRun(t, f, a); id != c {
		t.Fatalf("after promote 1: A's parent run = %s, want C(%s)", id, c)
	}
	// B survives (its continuation tail), C survives, A's head still resolves.
	for _, tr := range []TrunkID{a, b, c} {
		if x, err := f.Head(tr); err != nil {
			t.Fatalf("head %s after promote: %v", tr, err)
		} else {
			x.Close()
		}
	}

	// Promote A again: absorbs C; now its parent is the stump.
	n, err = f.Promote(a, 1)
	if err != nil || n != 1 {
		t.Fatalf("promote 2: n=%d err=%v", n, err)
	}
	if _, isStump := parentRun(t, f, a); !isStump {
		t.Fatalf("after promote 2: A should be rooted at the stump")
	}

	// Promote again: nothing above but the stump -> ErrAtStump.
	if _, err := f.Promote(a, 1); err != ErrAtStump {
		t.Fatalf("promote at stump: err=%v, want ErrAtStump", err)
	}

	// Invariant: A reads end-to-end, and the reopened forest agrees.
	checkReads := func(f *Trunks) {
		x, err := f.Head(a)
		if err != nil {
			t.Fatalf("head A: %v", err)
		}
		defer x.Close()
		last := mainTail(x)
		for lt := uint64(1); lt <= last; lt++ {
			if _, _, err := x.Read("ir", lt); err != nil {
				t.Fatalf("read A ir %d: %v", lt, err)
			}
		}
	}
	checkReads(f)
	r2, _ := openTrunks(dir, trunksCfg())
	cleanupTrunks(t, r2)
	checkReads(r2)
	if _, isStump := parentRun(t, r2, a); !isStump {
		t.Fatal("after reopen: A should still be rooted at the stump")
	}
}

// Excess promote levels past the stump are a no-op once movement is possible,
// and a multi-level promote climbs as far as it can.
func TestPromote_MultiLevelAndExcess(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "f")
	f, c := seedTrunkBirth(t, dir, "cfg@d880")
	for _, m := range []string{`"m1"`, `"m2"`, `"m3"`} {
		f.Append(c, 0, []byte(m), nil)
	}
	b, _ := forkAppend(t, f, c, 4, []byte(`"fromB"`))
	f.Append(b, 0, []byte(`"b2"`), nil)
	bx, _ := f.Head(b)
	bTail := mainTail(bx)
	bx.Close()
	a, _ := forkAppend(t, f, b, bTail-1, []byte(`"fromA"`))

	// Ask for 10 levels: climbs B then C (2), stops at the stump, excess no-op.
	n, err := f.Promote(a, 10)
	if err != nil {
		t.Fatalf("multi-level promote: %v", err)
	}
	if n != 2 {
		t.Fatalf("promote climbed %d levels, want 2 (then stump)", n)
	}
	if _, isStump := parentRun(t, f, a); !isStump {
		t.Fatal("A should be rooted at the stump after climbing all the way")
	}
}
