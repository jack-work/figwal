package xwal

import (
	"fmt"
	"path/filepath"
	"testing"
)

func unkeyedCfg() Config {
	return Config{
		Main: "ir",
		Channels: []ChannelSpec{
			{Name: "ir", Kind: ChannelLog},
			{Name: "chalkboard", Kind: ChannelReducible, Reducer: MapReducerName, Unkeyed: true},
		},
		SegmentSize: 4096,
	}
}

func seedUnkeyed(t *testing.T, dir string) (*Trunks, TrunkID) {
	t.Helper()
	f, err := createTrunks(dir, unkeyedCfg())
	if err != nil {
		t.Fatal(err)
	}
	cleanupTrunks(t, f)
	if err := f.CreateStump("s"); err != nil {
		t.Fatal(err)
	}
	id, err := f.SpawnUnderStump("s")
	if err != nil {
		t.Fatal(err)
	}
	return f, id
}

// An unkeyed channel is appended with NO reference to the timeline, and a
// fork still inherits exactly what the board held at the fork point --
// because the main record there recorded where the channel stood.
func TestUnkeyedChannelForksAtTheCursor(t *testing.T) {
	f, trunk := seedUnkeyed(t, filepath.Join(t.TempDir(), "f"))
	var at uint64
	for i := range 4 {
		// Board patch FIRST, with mainLT 0: it does not consult main at all.
		patch, _ := MapSetPatch([]string{fmt.Sprintf("k%d", i)}, fmt.Appendf(nil, `%d`, i))
		if _, err := f.AppendChannel(string(trunk), "chalkboard", 0, patch, nil); err != nil {
			t.Fatal(err)
		}
		_, lt, err := f.Append(trunk, 0, fmt.Appendf(nil, `"turn%d"`, i), nil)
		if err != nil {
			t.Fatal(err)
		}
		if i == 1 {
			at = lt // fork here: the board held k0,k1 and nothing later
		}
	}
	alt, err := f.ForkAt(trunk, at)
	if err != nil {
		t.Fatalf("fork at %d: %v", at, err)
	}
	x, err := f.Head(alt)
	if err != nil {
		t.Fatal(err)
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
		t.Fatalf("state: %v", err)
	}
	got := string(st)
	if want := `{"k0":0,"k1":1}`; got != want {
		t.Fatalf("forked board = %s, want %s (the board AS OF the fork point)", got, want)
	}
}

// The cursor stamp is what makes that work, and a main record written
// before stamping existed has none. Such a record must still yield a
// boundary, derived from the channel's old main-LT key.
func TestUnkeyedCursorFallsBackForOldRecords(t *testing.T) {
	f, trunk := seedUnkeyed(t, filepath.Join(t.TempDir(), "f"))
	for i := range 3 {
		patch, _ := MapSetPatch([]string{fmt.Sprintf("k%d", i)}, fmt.Appendf(nil, `%d`, i))
		if _, err := f.AppendChannel(string(trunk), "chalkboard", 0, patch, nil); err != nil {
			t.Fatal(err)
		}
		if _, _, err := f.Append(trunk, 0, []byte(`"t"`), nil); err != nil {
			t.Fatal(err)
		}
	}
	x, err := f.Head(trunk)
	if err != nil {
		t.Fatal(err)
	}
	// A pre-stamp record: no cursor at all.
	cur, err := x.cursorAt(mainTail(x), "chalkboard")
	x.Close()
	if err != nil {
		t.Fatalf("cursorAt: %v", err)
	}
	if cur == 0 {
		t.Fatal("cursor is 0; the stamp is not being written")
	}
}

// A reader walking main already holds the cursor stamp, so attributing an
// unkeyed channel's records to turns costs no extra read at all.
func TestMainRecordCarriesTheCursor(t *testing.T) {
	f, trunk := seedUnkeyed(t, filepath.Join(t.TempDir(), "f"))
	for i := range 3 {
		patch, _ := MapSetPatch([]string{fmt.Sprintf("k%d", i)}, fmt.Appendf(nil, `%d`, i))
		if _, err := f.AppendChannel(string(trunk), "chalkboard", 0, patch, nil); err != nil {
			t.Fatal(err)
		}
		if _, _, err := f.Append(trunk, 0, []byte(`"t"`), nil); err != nil {
			t.Fatal(err)
		}
	}
	x, err := f.Head(trunk)
	if err != nil {
		t.Fatal(err)
	}
	defer x.Close()
	tail := mainTail(x)
	seen := 0
	var prev uint64
	for lt := uint64(1); lt <= tail; lt++ {
		rec, err := x.ReadAt("ir", lt)
		if err != nil {
			t.Fatal(err)
		}
		cur, ok := rec.Cursors["chalkboard"]
		if !ok {
			continue // pre-stamp genesis records
		}
		if cur < prev {
			t.Fatalf("cursor went backwards at LT %d: %d after %d", lt, cur, prev)
		}
		prev = cur
		seen++
	}
	if seen == 0 {
		t.Fatal("no main record carried a cursor; the stamp is not reaching Record")
	}
	if prev == 0 {
		t.Fatal("cursor never advanced despite three board patches")
	}
}
