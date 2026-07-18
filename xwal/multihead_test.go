package xwal

import (
	"os"
	"path/filepath"
	"testing"
)

// liveLeaves returns the live (unfrozen) leaf node keys carrying a trunk id.
func liveLeaves(t *Trunks, trunk string) []string {
	var out []string
	for k, n := range t.nodes {
		if n.trunk == trunk && !n.frozen {
			out = append(out, k)
		}
	}
	return out
}

// TestForest_RepeatedTailForkOneHead drives the user's flow — send a turn,
// tail-fork, send, tail-fork … — and asserts the forked trunk keeps EXACTLY
// ONE live head/leaf throughout. The single-leaf invariant must hold for every
// trunk after every op.
func TestForest_RepeatedTailForkOneHead(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "f")
	f, conv := seedTrunkBirth(t, dir, "L@h")
	if _, _, err := f.Append(conv, 0, []byte(`"u1"`), nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := f.Append(conv, 0, []byte(`"a1"`), nil); err != nil {
		t.Fatal(err)
	}
	trunks := []string{conv}
	for i := 0; i < 6; i++ {
		alt, err := f.ForkTail(conv)
		if err != nil {
			t.Fatalf("tail-fork %d: %v", i, err)
		}
		trunks = append(trunks, alt)
		if _, _, err := f.Append(conv, 0, []byte(`"turn"`), nil); err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
		// Invariant: every trunk has exactly one live leaf carrying its id.
		for _, tr := range trunks {
			if ll := liveLeaves(f, tr); len(ll) != 1 {
				t.Fatalf("after op %d trunk %s has %d live leaves (want 1): %v", i, tr, len(ll), ll)
			}
		}
	}
	// And the head is the single leaf.
	if got := liveLeaves(f, conv); len(got) != 1 || f.heads[conv] != got[0] {
		t.Fatalf("conv head=%s leaves=%v", f.heads[conv], got)
	}
}

// makeMultiHead forges the legacy malformed shape on disk: a frozen branch
// point B carrying trunk T, with several same-id EMPTY sibling leaves under it
// (only one of them, the "advanced" one, carries own content). Mirrors the
// reference store's n4/{n6,n8,n10,n12} all stamped 2bdcea54. Returns the dir
// and the trunk id of the damaged trunk.
func makeMultiHead(t *testing.T) (string, string) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "f")
	f, conv := seedTrunkBirth(t, dir, "L@h")
	// conv owns LTs 3,4 (genesis@1, birth@2 inherited).
	if _, _, err := f.Append(conv, 0, []byte(`"u1"`), nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := f.Append(conv, 0, []byte(`"a1"`), nil); err != nil {
		t.Fatal(err)
	}
	// One legit tail-fork: freezes conv's head (now the branch point B), gives
	// it a continuation that keeps conv's id. Send a turn so the continuation
	// owns content (the "advanced" leaf the heal must keep).
	if _, err := f.ForkTail(conv); err != nil {
		t.Fatal(err)
	}
	if _, _, err := f.Append(conv, 0, []byte(`"advanced"`), nil); err != nil {
		t.Fatal(err)
	}
	// Find conv's head (the advanced leaf) and its parent branch point B.
	headKey := f.heads[conv]
	head := f.nodes[headKey]
	bBranch := f.nodes[head.parent].branch
	fb := mainForkBase4(t, f, head.branch) // the empties' fork base

	// Forge 3 spurious EMPTY same-id sibling leaves under B in every channel,
	// each forking at fb, each stamped conv's id — the legacy damage.
	names, err := channelNames(dir)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		sib := f.mintNode() // unique nN
		for _, ch := range names {
			d := filepath.Join(append([]string{dir, ch}, append(append([]string(nil), bBranch...), sib)...)...)
			if err := os.MkdirAll(d, 0o755); err != nil {
				t.Fatal(err)
			}
			// .fork marker so it reads as an empty-own branch at fb.
			if err := os.WriteFile(filepath.Join(d, ".fork"), []byte("base="+u64s(fb)+"\n"), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		// .trunk only in the main channel (where trunk ids live).
		md := filepath.Join(append([]string{dir, f.main}, append(append([]string(nil), bBranch...), sib)...)...)
		if err := writeTrunkID(md, conv); err != nil {
			t.Fatal(err)
		}
	}
	return dir, conv
}

func mainForkBase4(t *testing.T, f *Trunks, branch []string) uint64 {
	t.Helper()
	x, err := Open(f.root, f.cfg, branch...)
	if err != nil {
		t.Fatal(err)
	}
	defer x.Close()
	return mainForkBase(x)
}

func u64s(u uint64) string {
	if u == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for u > 0 {
		i--
		b[i] = byte('0' + u%10)
		u /= 10
	}
	return string(b[i:])
}

// TestForest_HealCollapsesMultiHead builds the legacy malformed tree (one
// trunk id on several sibling leaves), confirms it IS damaged, then opens it
// and asserts the heal collapsed the duplicates to one head — keeping the
// advanced leaf and pruning the empties — across all channels. Idempotent.
func TestForest_HealCollapsesMultiHead(t *testing.T) {
	dir, conv := makeMultiHead(t)

	// Damaged: a raw rebuild (no heal) sees several live leaves.
	raw := &Trunks{root: dir, cfg: trunksCfg(), main: "ir"}
	if err := raw.rebuild(); err != nil {
		t.Fatal(err)
	}
	if got := len(raw.leaves[conv]); got <= 1 {
		t.Fatalf("setup failed: expected a multi-head trunk, got %d leaves", got)
	}
	t.Logf("forged %d live leaves on %s", len(raw.leaves[conv]), conv)

	// Heal on open.
	f, err := OpenTrunks(dir, trunksCfg())
	if err != nil {
		t.Fatalf("open+heal: %v", err)
	}
	cleanupTrunks(t, f)
	if got := liveLeaves(f, conv); len(got) != 1 {
		t.Fatalf("after heal trunk %s has %d live leaves (want 1): %v", conv, len(got), got)
	}
	// The surviving head keeps the advanced content ("advanced" present).
	got := headPayloads(t, f, conv)
	if got[len(got)-1] != `"advanced"` {
		t.Fatalf("heal kept the wrong leaf; head payloads = %v", got)
	}
	// The pruned dirs are gone in every channel.
	names, _ := channelNames(dir)
	headBranch := f.nodes[f.heads[conv]].branch
	bBranch := f.nodes[f.nodes[f.heads[conv]].parent].branch
	for _, ch := range names {
		bd := filepath.Join(append([]string{dir, ch}, bBranch...)...)
		ents, _ := os.ReadDir(bd)
		nDirs := 0
		for _, e := range ents {
			if e.IsDir() {
				nDirs++
			}
		}
		// Under B only the legit (alt, cont=head) pair should remain.
		if nDirs > 2 {
			t.Fatalf("channel %s: branch point still has %d child dirs (want <=2)", ch, nDirs)
		}
	}
	_ = headBranch

	// Idempotent: re-open finds nothing to heal, head unchanged.
	f2, err := OpenTrunks(dir, trunksCfg())
	if err != nil {
		t.Fatal(err)
	}
	cleanupTrunks(t, f2)
	if len(liveLeaves(f2, conv)) != 1 || f2.heads[conv] != f.heads[conv] {
		t.Fatalf("heal not idempotent: head %s -> %s", f.heads[conv], f2.heads[conv])
	}
}
