package xwal

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/jack-work/figwal/disk"
	"github.com/jack-work/figwal/log"
	"github.com/jack-work/figwal/segment"
)

func seedForkStore(t *testing.T, dir string) (string, []string) {
	t.Helper()
	s, err := OpenStore(dir, testStoreOptions())
	if err != nil {
		t.Fatal(err)
	}
	tr, err := s.SpawnUnderRoot()
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 4; i++ {
		lt, err := s.Append(tr, "ir", 0, []byte(`{"turn":`+itoa(uint64(i))+`}`), nil)
		if err != nil {
			t.Fatal(err)
		}
		patch, _ := MapSetPatch([]string{"turn"}, []byte(itoa(lt)))
		if _, err := s.Append(tr, "chalkboard", lt, patch, nil); err != nil {
			t.Fatal(err)
		}
	}
	var branch []string
	for _, ti := range s.ListLight() {
		if ti.ID == tr {
			branch = ti.Head
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	return tr, branch
}

func trunkIDs(s *Store) map[string][]string {
	out := map[string][]string{}
	for _, ti := range s.ListLight() {
		out[ti.ID] = ti.Head
	}
	return out
}

// M1 window: every channel forked, plan (with commit info) still on
// disk, trunk markers never written. Reopen must roll the commit
// forward — the source trunk keeps its id via the continuation, the
// child id comes from the plan.
func TestForkRecoveryCommitsTrunkMarkers(t *testing.T) {
	dir := t.TempDir()
	tr, branch := seedForkStore(t, dir)
	cfg := testStoreOptions().config()

	x, err := Open(dir, cfg, branch...)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := x.buildForkPlan(4, "nA", "nC")
	if err != nil {
		t.Fatal(err)
	}
	plan.Commit = &forkCommit{SourceTrunk: tr, ChildTrunk: "t99"}
	if err := writeForkPlan(dir, plan); err != nil {
		t.Fatal(err)
	}
	getLog := func(e forkPlanEntry) (*log.Log, func(), error) {
		return x.chans[e.Name].log, func() {}, nil
	}
	if err := applyCachedForkPlan(dir, plan, getLog); err != nil {
		t.Fatal(err)
	}
	// Crash here: no markers, plan still present.
	if err := x.Close(); err != nil {
		t.Fatal(err)
	}

	s, err := OpenStore(dir, testStoreOptions())
	if err != nil {
		t.Fatalf("open after mid-fork crash: %v", err)
	}
	defer s.Close()
	ids := trunkIDs(s)
	if _, ok := ids[tr]; !ok {
		t.Fatalf("source trunk %s lost after recovery: %v", tr, ids)
	}
	if _, ok := ids["t99"]; !ok {
		t.Fatalf("child trunk from plan missing after recovery: %v", ids)
	}
	if _, err := s.Append(tr, "ir", 0, []byte(`{"cont":true}`), nil); err != nil {
		t.Fatalf("append to recovered source trunk: %v", err)
	}
	if lt, err := s.Append("t99", "ir", 0, []byte(`{"alt":true}`), nil); err != nil || lt != 4 {
		t.Fatalf("append to recovered child trunk: lt=%d err=%v", lt, err)
	}
	if _, payload, err := s.Read(tr, "ir", 3); err != nil || string(payload) != `{"turn":2}` {
		t.Fatalf("shared prefix after recovery: %s err=%v", payload, err)
	}
}

// M2 window: a dir-level .fork-pending sentinel survives inside a
// channel dir (crash just after forkImpl started). Reopen must consume
// it — never brick — and complete the joint fork.
func TestForkRecoveryConsumesDirSentinel(t *testing.T) {
	dir := t.TempDir()
	tr, branch := seedForkStore(t, dir)
	cfg := testStoreOptions().config()

	x, err := Open(dir, cfg, branch...)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := x.buildForkPlan(4, "nA", "nC")
	if err != nil {
		t.Fatal(err)
	}
	plan.Commit = &forkCommit{SourceTrunk: tr, ChildTrunk: "t99"}
	if err := writeForkPlan(dir, plan); err != nil {
		t.Fatal(err)
	}
	if err := x.Close(); err != nil {
		t.Fatal(err)
	}
	// Simulate a crash just after the first channel's forkImpl wrote its
	// sentinel and created the child dir.
	first := plan.Channels[0]
	chDir := filepath.Join(dir, first.Dir)
	if err := os.WriteFile(filepath.Join(chDir, disk.ForkPendingName), []byte("at=1\nchild=nA\nold=nC\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(chDir, "nA"), 0o755); err != nil {
		t.Fatal(err)
	}

	s, err := OpenStore(dir, testStoreOptions())
	if err != nil {
		t.Fatalf("open bricked on dir sentinel: %v", err)
	}
	defer s.Close()
	if pathExists(filepath.Join(chDir, disk.ForkPendingName)) {
		t.Fatal("dir sentinel not consumed")
	}
	ids := trunkIDs(s)
	if _, ok := ids[tr]; !ok {
		t.Fatalf("source trunk lost: %v", ids)
	}
	if _, ok := ids["t99"]; !ok {
		t.Fatalf("child trunk missing: %v", ids)
	}
}

// Post-swap window: the boundary segment was split and the prefix
// swapped in; the suffix lives only in the old-future. Rollback must
// rename it back (not delete it) before the redo.
func TestForkRecoveryRollsBackSwappedBoundary(t *testing.T) {
	dir := t.TempDir()
	tr, branch := seedForkStore(t, dir)
	cfg := testStoreOptions().config()

	x, err := Open(dir, cfg, branch...)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := x.buildForkPlan(3, "nA", "nC")
	if err != nil {
		t.Fatal(err)
	}
	plan.Commit = &forkCommit{SourceTrunk: tr, ChildTrunk: "t99"}
	if err := writeForkPlan(dir, plan); err != nil {
		t.Fatal(err)
	}
	if err := x.Close(); err != nil {
		t.Fatal(err)
	}

	// Hand-build the post-swap state in the MAIN channel dir: prefix
	// [base..2] in place, suffix [3..] only in the old-future, sentinel
	// still present.
	var mainEntry forkPlanEntry
	for _, e := range plan.Channels {
		if e.Name == plan.Main {
			mainEntry = e
		}
	}
	mainDir := filepath.Join(dir, mainEntry.Dir)
	codec := segment.JSONLCodec{}
	bases, err := segmentBases(mainDir, codec)
	if err != nil || len(bases) != 1 {
		t.Fatalf("main segments: %v err=%v", bases, err)
	}
	base := bases[0]
	at := mainEntry.AtIdx
	orig := filepath.Join(mainDir, segFileName(base, codec))
	spans, err := scanSegmentFrames(orig, codec)
	if err != nil {
		t.Fatal(err)
	}
	cutFrame := int(at - base)
	oldDir := filepath.Join(mainDir, "nC")
	if err := os.MkdirAll(oldDir, 0o755); err != nil {
		t.Fatal(err)
	}
	readFrames := func(path string, from, to int) [][]byte {
		f, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		var out [][]byte
		for i := from; i < to; i++ {
			p, _, err := codec.ReadFrame(f, spans[i].off, spans[i].off+int64(spans[i].len))
			if err != nil {
				t.Fatal(err)
			}
			out = append(out, p)
		}
		return out
	}
	writeSeg := func(path string, b uint64, frames [][]byte) {
		seg, err := segment.Create(path, codec, b, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, p := range frames {
			if _, err := seg.Append(p); err != nil {
				t.Fatal(err)
			}
		}
		if err := seg.Sync(); err != nil {
			t.Fatal(err)
		}
		if err := seg.Close(); err != nil {
			t.Fatal(err)
		}
	}
	prefix := readFrames(orig, 0, cutFrame)
	suffix := readFrames(orig, cutFrame, len(spans))
	writeSeg(filepath.Join(oldDir, segFileName(at, codec)), at, suffix)
	tmp := orig + ".swap"
	writeSeg(tmp, base, prefix)
	if err := os.Rename(tmp, orig); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(mainDir, disk.ForkPendingName), []byte("at=3\nchild=nA\nold=nC\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	s, err := OpenStore(dir, testStoreOptions())
	if err != nil {
		t.Fatalf("open after post-swap crash: %v", err)
	}
	defer s.Close()
	ids := trunkIDs(s)
	if _, ok := ids[tr]; !ok {
		t.Fatalf("source trunk lost: %v", ids)
	}
	// The source trunk's full timeline (incl. the suffix that lived only
	// in the old-future during the crash) must be readable.
	for lt, want := range map[uint64]string{2: `{"turn":1}`, 3: `{"turn":2}`, 5: `{"turn":4}`} {
		_, payload, err := s.Read(tr, "ir", lt)
		if err != nil || string(payload) != want {
			t.Fatalf("ir[%d] = %s err=%v, want %s", lt, payload, err, want)
		}
	}
	if lt, err := s.Append("t99", "ir", 0, []byte(`{"alt":true}`), nil); err != nil || lt != 3 {
		t.Fatalf("child append: lt=%d err=%v", lt, err)
	}
}
