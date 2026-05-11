package log

import (
	"bytes"
	"errors"
	"figwal/segment"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// forkSetup builds a log with the given codec containing entries
// 1..count, each payload "data{i}". SegmentSize is small enough that
// `entriesPerSeg` entries fit per segment, so rotation produces several
// sealed segments.
func forkSetup(t *testing.T, codec segment.SegmentCodec, count int, entriesPerSeg int) (string, *Log) {
	t.Helper()
	dir := t.TempDir()
	// Estimate: each binary frame is 8 + payload bytes, each JSONL line
	// is at least 50 bytes for our shape. Be generous to keep segment
	// boundaries predictable. Tests don't depend on the exact size.
	approxPerEntry := 80
	segSize := int64(approxPerEntry * entriesPerSeg)
	l, err := Open(dir, Options{SegmentSize: segSize, Codec: codec})
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(1); i <= uint64(count); i++ {
		var payload []byte
		if codec == nil || codec.Name() == "binary" {
			payload = []byte(fmt.Sprintf("data%d", i))
		} else {
			payload = []byte(fmt.Sprintf(`{"i":%d}`, i))
		}
		if err := l.Write(i, payload); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	return dir, l
}

func TestForkAtTail(t *testing.T) {
	// Forking at LastIndex+1 means no entries move; only the new fork
	// subdir is created and the parent becomes a branch point.
	dir, l := forkSetup(t, segment.BinaryCodec{}, 5, 3)
	defer l.Close()
	cut := l.LastIndex() + 1
	child, err := l.Fork(cut, "branch_a")
	if err != nil {
		t.Fatal(err)
	}
	defer child.Close()
	if !l.readOnly {
		t.Fatal("parent should be readOnly after fork")
	}
	if l.LastIndex() != cut-1 {
		t.Fatalf("parent LastIndex=%d want %d", l.LastIndex(), cut-1)
	}
	// branch_a is empty but appendable from cut.
	if err := child.Write(cut, []byte("first-in-fork")); err != nil {
		t.Fatalf("write to child: %v", err)
	}
	// Old future subdir should NOT exist when forking at tail.
	oldFuture := filepath.Join(dir, filepath.Base(dir))
	if _, err := os.Stat(oldFuture); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("old-future subdir should be absent at tail fork, got: %v", err)
	}
}

func TestForkMidSegmentSplits(t *testing.T) {
	// Force the fork to land in the middle of a sealed segment so the
	// boundary segment has to be split.
	dir, l := forkSetup(t, segment.BinaryCodec{}, 9, 3) // segs of ~3 entries
	defer l.Close()
	// Pick an index that should land mid-way in the first segment.
	cut := uint64(2)
	child, err := l.Fork(cut, "alt")
	if err != nil {
		t.Fatal(err)
	}
	defer child.Close()
	if l.LastIndex() != cut-1 {
		t.Fatalf("parent LastIndex=%d want %d", l.LastIndex(), cut-1)
	}
	// Reads of the prefix still work on the parent.
	for i := uint64(1); i < cut; i++ {
		got, err := l.Read(i)
		if err != nil {
			t.Fatalf("parent read %d: %v", i, err)
		}
		want := fmt.Sprintf("data%d", i)
		if string(got) != want {
			t.Fatalf("parent[%d]=%q want %q", i, got, want)
		}
	}
	// Child reads of the prefix fall through to parent.
	for i := uint64(1); i < cut; i++ {
		got, err := child.Read(i)
		if err != nil {
			t.Fatalf("child read %d (via parent): %v", i, err)
		}
		want := fmt.Sprintf("data%d", i)
		if string(got) != want {
			t.Fatalf("child[%d]=%q want %q", i, got, want)
		}
	}
	// Child writes diverge from parent.
	if err := child.Write(cut, []byte("diverged")); err != nil {
		t.Fatal(err)
	}
	got, _ := child.Read(cut)
	if string(got) != "diverged" {
		t.Fatalf("child[cut]=%q", got)
	}
	// The "old future" subdir holds the original entries [cut, 9].
	oldFuture := filepath.Join(dir, filepath.Base(dir))
	if _, err := os.Stat(oldFuture); err != nil {
		t.Fatalf("old-future subdir missing: %v", err)
	}
	of, err := Open(oldFuture, Options{Codec: segment.BinaryCodec{}})
	if err != nil {
		t.Fatal(err)
	}
	defer of.Close()
	for i := cut; i <= 9; i++ {
		got, err := of.Read(i)
		if err != nil {
			t.Fatalf("old-future read %d: %v", i, err)
		}
		want := fmt.Sprintf("data%d", i)
		if string(got) != want {
			t.Fatalf("old-future[%d]=%q want %q", i, got, want)
		}
	}
}

func TestForkRangeAcrossParent(t *testing.T) {
	// After a mid-segment fork, child.Range from index 1 should walk
	// the parent for the prefix and the child for the suffix.
	_, l := forkSetup(t, segment.BinaryCodec{}, 6, 3)
	defer l.Close()
	cut := uint64(3)
	child, err := l.Fork(cut, "alt")
	if err != nil {
		t.Fatal(err)
	}
	defer child.Close()
	for i := cut; i <= 7; i++ {
		if err := child.Write(i, []byte(fmt.Sprintf("c%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	var seen []uint64
	err = child.Range(1, func(idx uint64, payload []byte) error {
		seen = append(seen, idx)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []uint64{1, 2, 3, 4, 5, 6, 7}
	if len(seen) != len(want) {
		t.Fatalf("seen=%v want %v", seen, want)
	}
	for i, idx := range seen {
		if idx != want[i] {
			t.Fatalf("seen[%d]=%d want %d", i, idx, want[i])
		}
	}
}

func TestForkOfFork(t *testing.T) {
	// Depth >= 2: fork A from trunk, then fork B from A. Reads on B
	// should walk B -> A -> trunk.
	_, trunk := forkSetup(t, segment.BinaryCodec{}, 4, 2)
	defer trunk.Close()
	a, err := trunk.Fork(3, "a")
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	if err := a.Write(3, []byte("a3")); err != nil {
		t.Fatal(err)
	}
	if err := a.Write(4, []byte("a4")); err != nil {
		t.Fatal(err)
	}
	b, err := a.Fork(4, "b")
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if err := b.Write(4, []byte("b4")); err != nil {
		t.Fatal(err)
	}
	// B reads:
	//   1, 2: trunk (via a, via b)
	//   3: a
	//   4: b
	checks := map[uint64]string{
		1: "data1",
		2: "data2",
		3: "a3",
		4: "b4",
	}
	for idx, want := range checks {
		got, err := b.Read(idx)
		if err != nil {
			t.Fatalf("b.Read(%d): %v", idx, err)
		}
		if string(got) != want {
			t.Fatalf("b[%d]=%q want %q", idx, got, want)
		}
	}
}

func TestForkReadOnlyParentRejectsWrite(t *testing.T) {
	_, l := forkSetup(t, segment.BinaryCodec{}, 3, 2)
	defer l.Close()
	if _, err := l.Fork(2, "alt"); err != nil {
		t.Fatal(err)
	}
	if err := l.Write(2, []byte("should-fail")); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("want ErrReadOnly, got %v", err)
	}
}

func TestForkNameConflict(t *testing.T) {
	_, l := forkSetup(t, segment.BinaryCodec{}, 3, 2)
	defer l.Close()
	if _, err := l.Fork(2, "alt"); err != nil {
		t.Fatal(err)
	}
	// Cannot fork again because parent is now readOnly.
	if _, err := l.Fork(3, "again"); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("want ErrReadOnly on second fork, got %v", err)
	}
}

func TestForkRejectsReservedName(t *testing.T) {
	dir, l := forkSetup(t, segment.BinaryCodec{}, 3, 2)
	defer l.Close()
	if _, err := l.Fork(2, filepath.Base(dir)); !errors.Is(err, ErrForkConflict) {
		t.Fatalf("want ErrForkConflict, got %v", err)
	}
}

func TestForkRejectsBadName(t *testing.T) {
	_, l := forkSetup(t, segment.BinaryCodec{}, 3, 2)
	defer l.Close()
	for _, bad := range []string{"", ".", "..", "a/b", `a\b`} {
		if _, err := l.Fork(2, bad); !errors.Is(err, ErrInvalidForkName) {
			t.Fatalf("name %q: want ErrInvalidForkName, got %v", bad, err)
		}
	}
}

func TestForkRejectsOutOfRangeIdx(t *testing.T) {
	_, l := forkSetup(t, segment.BinaryCodec{}, 3, 2)
	defer l.Close()
	// At or below first index.
	if _, err := l.Fork(1, "alt"); err == nil {
		t.Fatal("expected error at atIdx == FirstIndex")
	}
	// Past LastIndex+1.
	if _, err := l.Fork(99, "alt"); err == nil {
		t.Fatal("expected error past LastIndex+1")
	}
}

func TestForkPendingBlocksOpen(t *testing.T) {
	dir, l := forkSetup(t, segment.BinaryCodec{}, 3, 2)
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	// Simulate a crashed fork by dropping the sentinel file.
	if err := os.WriteFile(filepath.Join(dir, forkPendingName),
		[]byte("at=2\nchild=foo\nold=bar\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir, Options{}); !errors.Is(err, ErrForkPending) {
		t.Fatalf("want ErrForkPending, got %v", err)
	}
}

func TestStoreDedupsParent(t *testing.T) {
	_, l := forkSetup(t, segment.BinaryCodec{}, 4, 2)
	dir := l.dir
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	// Reopen via store, fork twice, then reopen children via the same
	// store and assert they share the same parent pointer.
	s := NewStore()
	defer s.Close()
	trunk, err := s.Open(dir, Options{Codec: segment.BinaryCodec{}})
	if err != nil {
		t.Fatal(err)
	}
	a, err := trunk.Fork(2, "a")
	if err != nil {
		t.Fatal(err)
	}
	// Close `a` so we can reopen it through the store and observe
	// dedup. Closing the child does NOT close the trunk segments.
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	aReopened, err := s.Open(filepath.Join(dir, "a"), Options{Codec: segment.BinaryCodec{}})
	if err != nil {
		t.Fatal(err)
	}
	if aReopened.parent != trunk {
		t.Fatalf("expected shared trunk pointer, got %p want %p",
			aReopened.parent, trunk)
	}
}

func TestForkJSONLPreservesGlobalIdx(t *testing.T) {
	// After a mid-segment split with the JSONL codec, the new prefix
	// and suffix segments should still carry the original global idx
	// in each line. Verify by reading the prefix file directly.
	dir, l := forkSetup(t, segment.JSONLCodec{}, 6, 3)
	defer l.Close()
	cut := uint64(2)
	child, err := l.Fork(cut, "alt")
	if err != nil {
		t.Fatal(err)
	}
	defer child.Close()
	// Find the prefix file (whatever name it kept).
	entries, _ := os.ReadDir(dir)
	var jsonlFile string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".jsonl") {
			jsonlFile = filepath.Join(dir, e.Name())
		}
	}
	if jsonlFile == "" {
		t.Fatal("no jsonl file found in trunk dir")
	}
	raw, _ := os.ReadFile(jsonlFile)
	// The first line in this file should still encode global index 1.
	if !bytes.Contains(raw, []byte(`"_idx":1,`)) {
		t.Fatalf("prefix segment missing _idx:1\n%s", raw)
	}
}
