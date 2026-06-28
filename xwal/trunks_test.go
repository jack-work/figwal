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
