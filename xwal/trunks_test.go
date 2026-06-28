package xwal

import (
	"path/filepath"
	"testing"
)

func trunksCfg() Config {
	return Config{
		Main: "ir",
		Channels: []ChannelSpec{
			{Name: "ir", Kind: ChannelLog},
			{Name: "translations", Kind: ChannelLog},
			{Name: "chalkboard", Kind: ChannelReducible, Reducer: "jsonmerge"},
		},
		Registry:    map[string]Reducer{"jsonmerge": {Reduce: jsonMerge, Initial: []byte("{}")}},
		SegmentSize: 4096,
	}
}

// headMainTail opens a trunk's head and returns its main tail + first
// real (post-genesis) payloads, for assertions.
func headPayloads(t *testing.T, f *Trunks, trunk TrunkID) []string {
	t.Helper()
	x, err := f.Head(trunk)
	if err != nil {
		t.Fatalf("head %s: %v", trunk, err)
	}
	defer x.Close()
	var out []string
	last := mainTail(x)
	for lt := uint64(1); lt <= last; lt++ {
		_, p, err := x.Read("ir", lt)
		if err != nil {
			t.Fatalf("read ir %d on %s: %v", lt, trunk, err)
		}
		out = append(out, string(p))
	}
	return out
}

func TestForest_TailAppendNoFork(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "f")
	f, root, err := CreateTrunks(dir, trunksCfg())
	if err != nil {
		t.Fatal(err)
	}
	// Tail appends (atMainLT 0) keep the same trunk.
	tr, _, err := f.Append(root, 0, []byte(`"u1"`), nil)
	if err != nil || tr != root {
		t.Fatalf("append1: trunk=%s err=%v (want %s)", tr, err, root)
	}
	tr, _, err = f.Append(root, 0, []byte(`"a1"`), nil)
	if err != nil || tr != root {
		t.Fatalf("append2: trunk=%s err=%v", tr, err)
	}
	// genesis + u1 + a1
	got := headPayloads(t, f, root)
	if len(got) != 3 {
		t.Fatalf("want 3 entries (genesis+2), got %d: %v", len(got), got)
	}
}

func TestForest_InteriorForkKeepsExistingTrunk(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "f")
	f, root, err := CreateTrunks(dir, trunksCfg())
	if err != nil {
		t.Fatal(err)
	}
	// Build root: genesis(1) u1(2) a1(3) u2(4)  -> tail 4
	for _, m := range []string{`"u1"`, `"a1"`, `"u2"`} {
		if _, _, err := f.Append(root, 0, []byte(m), nil); err != nil {
			t.Fatal(err)
		}
	}
	// Interior fork: `:3` shares [1..3] (genesis,u1,a1) and diverges at 4.
	alt, altLT, err := f.Append(root, 3, []byte(`"alt-from-4"`), nil)
	if err != nil {
		t.Fatalf("interior fork: %v", err)
	}
	if alt == root {
		t.Fatalf("interior fork must mint a new trunk, got root %s", root)
	}
	if altLT != 4 {
		t.Fatalf("alt new content should land at LT 4 (shares [1..3]), got %d", altLT)
	}
	// Existing trunk retained intact: genesis,u1,a1,u2 still readable.
	rootGot := headPayloads(t, f, root)
	if len(rootGot) != 4 || rootGot[3] != `"u2"` {
		t.Fatalf("existing trunk not intact: %v", rootGot)
	}
	// Alt shares the frozen prefix [genesis,u1,a1] (LT<=3) then diverges at 4.
	altGot := headPayloads(t, f, alt)
	if len(altGot) != 4 || altGot[3] != `"alt-from-4"` {
		t.Fatalf("alt content wrong: %v", altGot)
	}
	if altGot[0] != genesisMarker || altGot[1] != `"u1"` || altGot[2] != `"a1"` {
		t.Fatalf("alt did not inherit shared prefix: %v", altGot)
	}
	// Trunk listing: root + alt; alt parent = root, branched at 4.
	trunks := f.List()
	if len(trunks) != 2 {
		t.Fatalf("want 2 trunks, got %d", len(trunks))
	}
	var altInfo *TrunkInfo
	for i := range trunks {
		if trunks[i].ID == alt {
			altInfo = &trunks[i]
		}
	}
	if altInfo == nil || altInfo.Parent != root || altInfo.BranchedLT != 4 {
		t.Fatalf("alt trunk info wrong: %+v", altInfo)
	}
}

func TestForest_ChalkboardForksAlong(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "f")
	f, root, err := CreateTrunks(dir, trunksCfg())
	if err != nil {
		t.Fatal(err)
	}
	// turn 1: append + a chalkboard patch keyed to the committed LT
	_, lt1, _ := f.Append(root, 0, []byte(`"u1"`), nil)
	if _, err := f.AppendChannel(root, "chalkboard", lt1, []byte(`{"set":{"mantra":"root thread"}}`), nil); err != nil {
		t.Fatal(err)
	}
	f.Append(root, 0, []byte(`"a1"`), nil) // tail 3

	// interior fork: `:2` shares [1..2] (incl. root-thread chalkboard@2),
	// diverges at 3 -> alt inherits the chalkboard watermark.
	alt, _, err := f.Append(root, 2, []byte(`"alt"`), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.AppendChannel(alt, "chalkboard", 0, []byte(`{"set":{"mantra":"alt thread"}}`), nil); err != nil {
		t.Fatal(err)
	}
	// alt chalkboard folds to alt thread; root stays root thread.
	ax, _ := f.Head(alt)
	defer ax.Close()
	st, err := ax.StateAt("chalkboard", chalkLast(t, ax))
	if err != nil {
		t.Fatal(err)
	}
	if string(st) != `{"mantra":"alt thread"}` {
		t.Fatalf("alt chalkboard = %s", st)
	}
	rx, _ := f.Head(root)
	defer rx.Close()
	rst, _ := rx.StateAt("chalkboard", chalkLast(t, rx))
	if string(rst) != `{"mantra":"root thread"}` {
		t.Fatalf("root chalkboard = %s", rst)
	}
}

func TestForest_ForkTailBisectsPresent(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "f")
	f, root, err := CreateTrunks(dir, trunksCfg())
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range []string{`"u1"`, `"a1"`} {
		f.Append(root, 0, []byte(m), nil)
	}
	rootBefore := headPayloads(t, f, root) // genesis,u1,a1
	alt, err := f.ForkTail(root)
	if err != nil {
		t.Fatalf("fork-tail: %v", err)
	}
	if alt == root {
		t.Fatal("fork-tail must mint a new alternative trunk")
	}
	// Both share the full prefix [genesis,u1,a1]; both empty after.
	for _, tr := range []TrunkID{root, alt} {
		got := headPayloads(t, f, tr)
		if len(got) != len(rootBefore) {
			t.Fatalf("trunk %s after fork-tail: want %d shared entries, got %v", tr, len(rootBefore), got)
		}
	}
	// Diverge: append to each, confirm isolation.
	f.Append(root, 0, []byte(`"root-cont"`), nil)
	f.Append(alt, 0, []byte(`"alt-cont"`), nil)
	rg := headPayloads(t, f, root)
	ag := headPayloads(t, f, alt)
	if rg[len(rg)-1] != `"root-cont"` || ag[len(ag)-1] != `"alt-cont"` {
		t.Fatalf("divergence failed: root=%v alt=%v", rg, ag)
	}
}

func chalkLast(t *testing.T, x *XWAL) uint64 {
	t.Helper()
	for _, c := range x.Channels() {
		if c.Name == "chalkboard" {
			return c.Last
		}
	}
	t.Fatal("no chalkboard channel")
	return 0
}

func TestTrunks_SpawnChild(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "f")
	root, rootID, err := CreateTrunks(dir, trunksCfg())
	if err != nil {
		t.Fatal(err)
	}
	// rootID = the ceremonial "null". Spawn two children (loadouts) — no
	// continuation, both N-ary siblings under the frozen root.
	l1, err := root.SpawnChild(rootID)
	if err != nil {
		t.Fatalf("spawn l1: %v", err)
	}
	l2, err := root.SpawnChild(rootID)
	if err != nil {
		t.Fatalf("spawn l2 (root now frozen/closed): %v", err)
	}
	if l1 == l2 || l1 == rootID || l2 == rootID {
		t.Fatalf("spawned trunks must be distinct: root=%s l1=%s l2=%s", rootID, l1, l2)
	}
	// root is now closed (no live head): Append/ForkTail on it should fail.
	if _, _, err := root.Append(rootID, 0, []byte(`"x"`), nil); err == nil {
		t.Fatal("append to a closed ceremonial root should fail")
	}
	// A loadout can itself spawn a conversation child.
	conv, err := root.SpawnChild(l1)
	if err != nil {
		t.Fatalf("spawn conv from loadout: %v", err)
	}
	// conv is a live trunk: it can take turns and fork normally.
	if _, _, err := root.Append(conv, 0, []byte(`"hello"`), nil); err != nil {
		t.Fatalf("append to conversation: %v", err)
	}
	if _, _, err := root.Append(conv, 0, []byte(`"more"`), nil); err != nil {
		t.Fatal(err)
	}
	alt, _, err := root.Append(conv, 2, []byte(`"branch"`), nil) // interior fork at conv's first own LT (shares genesis+hello)
	if err != nil {
		t.Fatalf("fork conversation: %v", err)
	}
	if alt == conv {
		t.Fatal("interior fork should mint a new trunk")
	}
	// Reopen from disk: all trunks still resolve.
	r2, err := OpenTrunks(dir, trunksCfg())
	if err != nil {
		t.Fatal(err)
	}
	// l1 is now a closed ceremonial trunk (it spawned conv) — no live head,
	// like a loadout; resolve only the live trunks. l1 stays a SpawnChild parent.
	if _, err := r2.SpawnChild(l1); err != nil {
		t.Fatalf("closed l1 must still accept SpawnChild after reopen: %v", err)
	}
	for _, tr := range []string{l2, conv, alt} {
		if x, err := r2.Head(tr); err != nil {
			t.Fatalf("reopen head %s: %v", tr, err)
		} else {
			x.Close()
		}
	}
}

func TestTrunks_ForkAt(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "f")
	root, rootID, err := CreateTrunks(dir, trunksCfg())
	if err != nil {
		t.Fatal(err)
	}
	conv, err := root.SpawnChild(rootID)
	if err != nil {
		t.Fatal(err)
	}
	// conv owns u1,u2,u3 (LTs 2,3,4 after genesis@1 inherited).
	for _, m := range []string{`"u1"`, `"u2"`, `"u3"`} {
		if _, _, err := root.Append(conv, 0, []byte(m), nil); err != nil {
			t.Fatal(err)
		}
	}
	x, _ := root.Head(conv)
	tail := mainTail(x)
	x.Close()
	// Interior fork at tail-1 (no message) -> empty alt sharing the prefix.
	alt, err := root.ForkAt(conv, tail-1)
	if err != nil {
		t.Fatalf("forkat interior: %v", err)
	}
	if alt == conv || alt == "" {
		t.Fatalf("forkat must mint a new trunk, got %q", alt)
	}
	// alt is live and empty-own: a send appends at the divergence point.
	if _, _, err := root.Append(alt, 0, []byte(`"branch msg"`), nil); err != nil {
		t.Fatalf("send to forked alt: %v", err)
	}
	// conv keeps its id and is still re-forkable / sendable.
	if _, _, err := root.Append(conv, 0, []byte(`"u4"`), nil); err != nil {
		t.Fatalf("original trunk still live: %v", err)
	}
	// Past-tail ForkAt degenerates to a tail fork.
	alt2, err := root.ForkAt(conv, 9999)
	if err != nil || alt2 == "" || alt2 == conv {
		t.Fatalf("forkat past tail should tail-fork: alt2=%q err=%v", alt2, err)
	}
}

func TestTrunks_ReSplitBelow(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "f")
	root, rootID, err := CreateTrunks(dir, trunksCfg())
	if err != nil {
		t.Fatal(err)
	}
	// Build a conversation, then fork it -> a parent/child trunk pair sharing
	// a prefix. The alt's own range starts above the shared turns.
	conv, _ := root.SpawnChild(rootID)
	for _, m := range []string{`"a"`, `"b"`, `"c"`, `"d"`} {
		if _, _, err := root.Append(conv, 0, []byte(m), nil); err != nil {
			t.Fatal(err)
		}
	}
	// conv owns LTs 3,4,5,6 (genesis@1, loadout-birth@2 inherited). Fork at 5
	// -> alt2 shares [1..5], conv continues with its suffix.
	alt2, _, err := root.Append(conv, 5, []byte(`"alt-from-5"`), nil)
	if err != nil {
		t.Fatalf("interior fork: %v", err)
	}
	// Now RE-SPLIT-BELOW: fork alt2 at LT 3 — a turn alt2 inherited from conv
	// (below alt2's own range). Must fork the ancestor and mint a sibling.
	sib, _, err := root.Append(alt2, 3, []byte(`"resplit-at-3"`), nil)
	if err != nil {
		t.Fatalf("re-split-below at inherited LT: %v", err)
	}
	if sib == alt2 || sib == conv || sib == "" {
		t.Fatalf("re-split must mint a distinct sibling trunk, got %q", sib)
	}
	// Everyone still resolves + folds after the re-split, and from disk.
	r2, err := OpenTrunks(dir, trunksCfg())
	if err != nil {
		t.Fatal(err)
	}
	for _, tr := range []string{conv, alt2, sib} {
		x, err := r2.Head(tr)
		if err != nil {
			t.Fatalf("reopen head %s: %v", tr, err)
		}
		// each is sendable (write isolation holds through the re-split)
		x.Close()
		if _, _, err := r2.Append(tr, 0, []byte(`"more"`), nil); err != nil {
			t.Fatalf("append to %s after re-split: %v", tr, err)
		}
	}
}

func TestTrunks_OwnerTrunk(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "f")
	root, rootID, err := CreateTrunks(dir, trunksCfg())
	if err != nil {
		t.Fatal(err)
	}
	// root owns LT 1 (genesis). ceremonial child (loadout-like) owns LT 2.
	ld, _ := root.SpawnChild(rootID)
	if _, _, err := root.Append(ld, 0, []byte(`"loadout-birth"`), nil); err != nil {
		t.Fatal(err)
	}
	conv, _ := root.SpawnChild(ld) // owns [3..]
	for _, m := range []string{`"u1"`, `"u2"`} {
		if _, _, err := root.Append(conv, 0, []byte(m), nil); err != nil {
			t.Fatal(err)
		}
	}
	cases := map[uint64]string{1: rootID, 2: ld, 3: conv, 4: conv}
	for lt, want := range cases {
		got, err := root.OwnerTrunk(conv, lt)
		if err != nil {
			t.Fatalf("OwnerTrunk(%d): %v", lt, err)
		}
		if got != want {
			t.Errorf("OwnerTrunk(conv, %d) = %q, want %q", lt, got, want)
		}
	}
}

func TestTrunks_Remove(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "f")
	root, rootID, err := CreateTrunks(dir, trunksCfg())
	if err != nil {
		t.Fatal(err)
	}
	ld, _ := root.SpawnChild(rootID)
	root.Append(ld, 0, []byte(`"birth"`), nil)
	conv, _ := root.SpawnChild(ld)
	root.Append(conv, 0, []byte(`"u1"`), nil)
	root.Append(conv, 0, []byte(`"u2"`), nil)
	alt, _ := root.ForkTail(conv) // conv now has a live branch (alt)

	// Can't remove the root.
	if err := root.Remove(rootID, false); err == nil {
		t.Fatal("removing the root trunk must fail")
	}
	// conv has a live branch (alt) -> refuse without recursive.
	if err := root.Remove(conv, false); err == nil {
		t.Fatal("removing a trunk with live branches must fail without recursive")
	}
	// Remove the leaf branch (alt) directly -> ok; conv survives.
	if err := root.Remove(alt, false); err != nil {
		t.Fatalf("remove leaf branch: %v", err)
	}
	if _, err := root.Head(conv); err != nil {
		t.Fatalf("conv must survive removing its branch: %v", err)
	}
	if _, err := root.Head(alt); err == nil {
		t.Fatal("alt should be gone")
	}
	// Reopen from disk: alt stays gone, conv resolves.
	r2, _ := OpenTrunks(dir, trunksCfg())
	if _, err := r2.Head(conv); err != nil {
		t.Fatalf("conv missing after reopen: %v", err)
	}
	if _, err := r2.Head(alt); err == nil {
		t.Fatal("alt resurrected after reopen")
	}
	// Recursive remove of conv (it now has no branches, but exercise the flag).
	if err := r2.Remove(conv, true); err != nil {
		t.Fatalf("recursive remove conv: %v", err)
	}
	if _, err := r2.Head(conv); err == nil {
		t.Fatal("conv should be gone after recursive remove")
	}
}
