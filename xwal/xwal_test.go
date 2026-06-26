package xwal

import (
	"encoding/json"
	"testing"
)

// jsonMerge is a chalkboard-style reducer: patches are {"set":{...},
// "remove":[...]} applied to a flat JSON object.
func jsonMerge(state, patch []byte) ([]byte, error) {
	m := map[string]json.RawMessage{}
	if len(state) > 0 {
		if err := json.Unmarshal(state, &m); err != nil {
			return nil, err
		}
	}
	var p struct {
		Set    map[string]json.RawMessage `json:"set"`
		Remove []string                   `json:"remove"`
	}
	if err := json.Unmarshal(patch, &p); err != nil {
		return nil, err
	}
	for k, v := range p.Set {
		m[k] = v
	}
	for _, k := range p.Remove {
		delete(m, k)
	}
	return json.Marshal(m)
}

func triune(t *testing.T, dir string) (*XWAL, Config) {
	t.Helper()
	cfg := Config{
		Main:        "ir",
		Registry:    map[string]ReduceFunc{"jsonmerge": jsonMerge},
		SegmentSize: 96, // small, to force watermark rotation
		Channels: []ChannelSpec{
			{Name: "ir", Kind: ChannelLog},
			{Name: "translations", Kind: ChannelLog},
			{Name: "chalkboard", Kind: ChannelReducible, Reducer: "jsonmerge"},
		},
	}
	x, err := Open(dir, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return x, cfg
}

func TestXWAL_TriuneAppendAndState(t *testing.T) {
	dir := t.TempDir()
	x, _ := triune(t, dir)

	// Drive a small conversation: each IR tic gets a translation and a
	// chalkboard patch (keyed to that tic).
	for i := uint64(1); i <= 6; i++ {
		mainLT, err := x.AppendMain([]byte("msg"))
		if err != nil {
			t.Fatalf("appendMain %d: %v", i, err)
		}
		if mainLT != i {
			t.Fatalf("mainLT = %d, want %d", mainLT, i)
		}
		if _, err := x.Append("translations", mainLT, []byte("wire")); err != nil {
			t.Fatalf("append translation: %v", err)
		}
		patch := []byte(`{"set":{"turn":` + itoa(i) + `}}`)
		if _, err := x.Append("chalkboard", mainLT, patch); err != nil {
			t.Fatalf("append chalkboard: %v", err)
		}
	}

	// Chalkboard state folds to the latest turn.
	st, err := x.StateAt("chalkboard", 6)
	if err != nil {
		t.Fatalf("stateAt: %v", err)
	}
	if got := field(t, st, "turn"); got != "6" {
		t.Fatalf("chalkboard turn = %s, want 6", got)
	}

	// Monotonic main-LT enforced.
	if _, err := x.Append("translations", 3, []byte("late")); err == nil {
		t.Fatal("expected non-decreasing main-LT violation, got nil")
	}

	x.Close()

	// Joint fork at main-LT 4: all three channels branch as a unit.
	x2, _ := triune(t, dir)
	child, err := x2.Fork(4, "alt", "orig")
	if err != nil {
		t.Fatalf("fork: %v", err)
	}
	x2.Close()
	defer child.Close()

	// Child sees the shared prefix on every channel.
	if m, _, err := child.Read("ir", 3); err != nil || m != 3 {
		t.Fatalf("child ir[3] = (%d,%v), want main-LT 3", m, err)
	}
	// Child chalkboard folds the fork-point state (turns 1..3) without
	// reading the read-only parent prefix patches directly.
	if st, err := child.StateAt("chalkboard", 3); err != nil {
		t.Fatalf("child stateAt(3): %v", err)
	} else if got := field(t, st, "turn"); got != "3" {
		t.Fatalf("child chalkboard at fork = turn %s, want 3", got)
	}

	// Child diverges: new tic at main-LT 4 with a different patch.
	mainLT, err := child.AppendMain([]byte("alt-msg"))
	if err != nil {
		t.Fatalf("child appendMain: %v", err)
	}
	if mainLT != 4 {
		t.Fatalf("child mainLT = %d, want 4 (continues numbering at fork)", mainLT)
	}
	if _, err := child.Append("chalkboard", mainLT, []byte(`{"set":{"branch":"alt"}}`)); err != nil {
		t.Fatalf("child append chalkboard: %v", err)
	}
	st, err = child.StateAt("chalkboard", child.chans["chalkboard"].log.LastIndex())
	if err != nil {
		t.Fatalf("child stateAt latest: %v", err)
	}
	if got := field(t, st, "branch"); got != `"alt"` {
		t.Fatalf("child chalkboard branch = %s, want \"alt\"", got)
	}
	if got := field(t, st, "turn"); got != "3" {
		t.Fatalf("child chalkboard turn = %s, want 3 (inherited from fork point)", got)
	}
}

func TestXWAL_ReopenManifest(t *testing.T) {
	dir := t.TempDir()
	x, _ := triune(t, dir)
	if _, err := x.AppendMain([]byte("a")); err != nil {
		t.Fatal(err)
	}
	x.Close()

	// Reopen with only the registry (no channel specs): manifest drives it.
	x2, err := Open(dir, Config{Registry: map[string]ReduceFunc{"jsonmerge": jsonMerge}})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer x2.Close()
	if x2.Main() != "ir" {
		t.Fatalf("main = %q, want ir", x2.Main())
	}
	chans := x2.Channels()
	if len(chans) != 3 {
		t.Fatalf("channels = %d, want 3", len(chans))
	}
	var cb *ChannelInfo
	for i := range chans {
		if chans[i].Name == "chalkboard" {
			cb = &chans[i]
		}
	}
	if cb == nil || cb.Kind != ChannelReducible || cb.Reducer != "jsonmerge" {
		t.Fatalf("chalkboard not restored as reducible: %+v", cb)
	}
}

func field(t *testing.T, obj []byte, key string) string {
	t.Helper()
	m := map[string]json.RawMessage{}
	if err := json.Unmarshal(obj, &m); err != nil {
		t.Fatalf("unmarshal state %s: %v", obj, err)
	}
	return string(m[key])
}

func itoa(n uint64) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
