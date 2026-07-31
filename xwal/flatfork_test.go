package xwal

import (
	"os"
	"path/filepath"
	"testing"
)

// A fork must not mutate the parent's topology.
func TestForkDoesNotMutateParent(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "f")
	f, trunk := seedTrunk(t, dir)
	for range 3 {
		if _, _, err := f.Append(trunk, 0, []byte(`"turn"`), nil); err != nil {
			t.Fatal(err)
		}
	}
	headKey, _ := f.idx.Head(trunk)
	parentDir := f.irDir(f.node(headKey).Branch)
	subdirs := func() []string {
		ents, err := os.ReadDir(parentDir)
		if err != nil {
			t.Fatal(err)
		}
		var out []string
		for _, e := range ents {
			if e.IsDir() {
				out = append(out, e.Name())
			}
		}
		return out
	}
	before := subdirs()

	alt, err := f.ForkTail(trunk)
	if err != nil {
		t.Fatalf("fork: %v", err)
	}

	if after := subdirs(); len(after) != len(before) {
		t.Fatalf("fork nested a child under the parent: %v -> %v", before, after)
	}

	// The parent keeps its id and its head.
	if k, ok := f.idx.Head(trunk); !ok || k != headKey {
		t.Fatalf("parent head moved: %q -> %q", headKey, k)
	}
	// The alternative is a sibling, one level deep.
	altKey, ok := f.idx.Head(alt)
	if !ok {
		t.Fatal("no head for the alternative")
	}
	n, _ := f.idx.Node(altKey)
	if len(n.Branch) != 1 {
		t.Fatalf("alternative is nested: branch=%v", n.Branch)
	}
	if n.From != headKey {
		t.Fatalf("alternative lineage = %q, want %q", n.From, headKey)
	}
}

// A flat child reads the shared prefix through the index, not the directory.
func TestFlatForkInheritsPrefix(t *testing.T) {
	f, trunk := seedTrunk(t, filepath.Join(t.TempDir(), "f"))
	for i := range 3 {
		if _, _, err := f.Append(trunk, 0, []byte(`"t`+string(rune('0'+i))+`"`), nil); err != nil {
			t.Fatal(err)
		}
	}
	alt, err := f.ForkTail(trunk)
	if err != nil {
		t.Fatal(err)
	}
	x, err := f.Head(alt)
	if err != nil {
		t.Fatalf("open flat child: %v", err)
	}
	defer x.Close()
	if got := mainTail(x); got < 3 {
		t.Fatalf("child tail = %d, want >= 3: the prefix is not visible", got)
	}
	if _, err := x.chans[f.main].log.Read(2); err != nil {
		t.Fatalf("child cannot read shared LT 2: %v", err)
	}
}
