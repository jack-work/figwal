package xwal

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// mainLTs reads every main index visible from a trunk's head.
func mainLTs(t *testing.T, f *Trunks, trunk TrunkID) []uint64 {
	t.Helper()
	x, err := f.Head(trunk)
	if err != nil {
		t.Fatalf("head %s: %v", trunk, err)
	}
	defer x.Close()
	var out []uint64
	ch := x.chans[f.main]
	if err := ch.log.Range(1, func(idx uint64, _ []byte) error {
		out = append(out, idx)
		return nil
	}); err != nil {
		t.Fatalf("range %s: %v", trunk, err)
	}
	return out
}

// A detached node reads its whole history with its ancestor DELETED. This
// is the property a delete depends on: absorb the prefix, then unlink.
func TestDetachSurvivesAncestorRemoval(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "f")
	f, parent := seedMapTrunk(t, dir)
	for i := range 5 {
		_, lt, err := f.Append(parent, 0, fmt.Appendf(nil, `"p%d"`, i), nil)
		if err != nil {
			t.Fatal(err)
		}
		patch, _ := MapSetPatch([]string{"n"}, fmt.Appendf(nil, `%d`, i))
		if _, err := f.AppendChannel(string(parent), "chalkboard", lt, patch, nil); err != nil {
			t.Fatal(err)
		}
	}
	child, err := f.ForkTail(parent)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := f.Append(child, 0, []byte(`"c0"`), nil); err != nil {
		t.Fatal(err)
	}
	before := mainLTs(t, f, child)
	if len(before) < 6 {
		t.Fatalf("child sees %d records before detach, want the inherited prefix", len(before))
	}

	if err := f.Detach(f.head(string(child))); err != nil {
		t.Fatalf("detach: %v", err)
	}
	if got := mainLTs(t, f, child); len(got) != len(before) {
		t.Fatalf("after detach child sees %v, want %v", got, before)
	}
	// The ancestor is now dispensable. Remove its directory outright, the
	// way a delete would, and the child must be unharmed.
	if err := os.RemoveAll(f.irDir(f.head(string(parent)))); err != nil {
		t.Fatal(err)
	}
	g, err := openTrunks(dir, mapTrunksCfg())
	if err != nil {
		t.Fatalf("reopen after removing the ancestor: %v", err)
	}
	cleanupTrunks(t, g)
	if got := mainLTs(t, g, child); len(got) != len(before) {
		t.Fatalf("with the ancestor gone the child sees %v, want %v", got, before)
	}
}

// Detaching must not change what the node reads, including reducible state
// folded from a watermark it used to inherit.
func TestDetachPreservesReducibleState(t *testing.T) {
	f, parent := seedMapTrunk(t, filepath.Join(t.TempDir(), "f"))
	_, lt, err := f.Append(parent, 0, []byte(`"p"`), nil)
	if err != nil {
		t.Fatal(err)
	}
	patch, _ := MapSetPatch([]string{"model"}, []byte(`"opus"`))
	if _, err := f.AppendChannel(string(parent), "chalkboard", lt, patch, nil); err != nil {
		t.Fatal(err)
	}
	child, err := f.ForkTail(parent)
	if err != nil {
		t.Fatal(err)
	}
	state := func(label string) string {
		x, err := f.Head(child)
		if err != nil {
			t.Fatalf("%s head: %v", label, err)
		}
		defer x.Close()
		var last uint64
		for _, c := range x.Channels() {
			if c.Name == "chalkboard" {
				last = c.Last
			}
		}
		st, err := x.StateAt("chalkboard", last)
		if err != nil {
			t.Fatalf("%s state: %v", label, err)
		}
		return string(st)
	}
	before := state("before")
	if !strings.Contains(before, `"opus"`) {
		t.Fatalf("child state before detach = %s, want the inherited model", before)
	}
	if err := f.Detach(f.head(string(child))); err != nil {
		t.Fatalf("detach: %v", err)
	}
	if after := state("after"); after != before {
		t.Fatalf("detach changed state: %s -> %s", before, after)
	}
}

// Detach is idempotent: running it twice is not an error and changes
// nothing, which is what makes crash recovery a re-run.
func TestDetachIsIdempotent(t *testing.T) {
	f, parent := seedMapTrunk(t, filepath.Join(t.TempDir(), "f"))
	for range 3 {
		if _, _, err := f.Append(parent, 0, []byte(`"p"`), nil); err != nil {
			t.Fatal(err)
		}
	}
	child, err := f.ForkTail(parent)
	if err != nil {
		t.Fatal(err)
	}
	key := f.head(string(child))
	if err := f.Detach(key); err != nil {
		t.Fatal(err)
	}
	first := mainLTs(t, f, child)
	if err := f.Detach(key); err != nil {
		t.Fatalf("second detach: %v", err)
	}
	if got := mainLTs(t, f, child); len(got) != len(first) {
		t.Fatalf("second detach changed the log: %v -> %v", first, got)
	}
}
