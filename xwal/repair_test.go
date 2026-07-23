package xwal

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/jack-work/figwal/segment"
)

func truncateLastFrames(t *testing.T, path string, drop int) {
	t.Helper()
	spans, err := scanSegmentFrames(path, segment.JSONLCodec{})
	if err != nil {
		t.Fatal(err)
	}
	if len(spans) < drop {
		t.Fatalf("segment has %d frames, cannot drop %d", len(spans), drop)
	}
	cut := int64(0)
	if keep := len(spans) - drop; keep > 0 {
		cut = spans[keep-1].off + int64(spans[keep-1].len)
	}
	if err := truncateFileSynced(path, cut); err != nil {
		t.Fatal(err)
	}
}

func TestOpenRepairsIncoherentLineage(t *testing.T) {
	dir := t.TempDir()
	opts := testStoreOptions()
	opts.Opaque = []string{"translations"}
	s, err := OpenStore(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	tr, err := s.SpawnUnderRoot()
	if err != nil {
		t.Fatal(err)
	}
	var lts []uint64
	for i := 1; i <= 3; i++ {
		lt, err := s.Append(tr, "ir", 0, []byte(`{"turn":`+itoa(uint64(i))+`}`), nil)
		if err != nil {
			t.Fatal(err)
		}
		lts = append(lts, lt)
		if _, err := s.Append(tr, "translations", lt, []byte(`wire`), nil); err != nil {
			t.Fatal(err)
		}
		patch, _ := MapSetPatch([]string{"turn"}, []byte(itoa(lt)))
		if _, err := s.Append(tr, "chalkboard", lt, patch, nil); err != nil {
			t.Fatal(err)
		}
	}
	lost, kept := lts[2], lts[1]
	var headBranch []string
	for _, ti := range s.ListLight() {
		if ti.ID == tr {
			headBranch = ti.Head
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Simulate a crash that lost the last main entry but kept the related
	// records referencing it, with the store marked unclean.
	mainDir := filepath.Join(append([]string{dir, "ir"}, headBranch...)...)
	bases, err := segmentBases(mainDir, segment.JSONLCodec{})
	if err != nil || len(bases) == 0 {
		t.Fatalf("main segments: %v %v", bases, err)
	}
	truncateLastFrames(t, filepath.Join(mainDir, segFileName(bases[len(bases)-1], segment.JSONLCodec{})), 1)
	if err := os.WriteFile(uncleanPath(dir), []byte("open\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	s2, err := OpenStore(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	if _, ok, err := s2.Lookup(tr, "ir", lost); err != nil || ok {
		t.Fatalf("lost main LT should be gone: ok=%v err=%v", ok, err)
	}
	if _, ok, err := s2.Lookup(tr, "translations", lost); err != nil || ok {
		t.Fatalf("translations record for lost main LT survived: ok=%v err=%v", ok, err)
	}
	rec, ok, err := s2.Lookup(tr, "translations", kept)
	if err != nil || !ok || string(rec.Payload) != "wire" {
		t.Fatalf("translations kept LT: %+v ok=%v err=%v", rec, ok, err)
	}
	if _, ok, err := s2.Lookup(tr, "chalkboard", lost); err != nil || ok {
		t.Fatalf("chalkboard patch for lost main LT survived: ok=%v err=%v", ok, err)
	}
	chans, err := s2.Channels(tr)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range chans {
		if c.Name != "chalkboard" {
			continue
		}
		state, err := s2.StateAt(tr, "chalkboard", c.Last)
		if err != nil {
			t.Fatal(err)
		}
		if got := field(t, state, "turn"); got != itoa(kept) {
			t.Fatalf("chalkboard state ahead of main: turn=%s want %s", got, itoa(kept))
		}
	}
	lt, err := s2.Append(tr, "ir", 0, []byte(`{"turn":99}`), nil)
	if err != nil || lt != lost {
		t.Fatalf("append after repair: lt=%d err=%v, want %d", lt, err, lost)
	}
}

func TestCleanCloseSkipsRepairMarker(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenStore(dir, testStoreOptions())
	if err != nil {
		t.Fatal(err)
	}
	if !pathExists(uncleanPath(dir)) {
		t.Fatal("open did not mark the store unclean")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if pathExists(uncleanPath(dir)) {
		t.Fatal("clean close left the unclean marker")
	}
}
