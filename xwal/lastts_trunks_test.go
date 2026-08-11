package xwal

import (
	"path/filepath"
	"testing"
)

// Trunks.LastTS answers per-trunk recency without the caller holding a
// handle: the newest stamp across ALL the trunk's channels, zero for
// pre-timestamp trunks, and independent per trunk.
func TestTrunksLastTS(t *testing.T) {
	clk := &fakeClock{}
	clk.set(1_000)
	f, err := createTrunks(filepath.Join(t.TempDir(), "f"), clk.cfgWith(unkeyedCfg()))
	if err != nil {
		t.Fatal(err)
	}
	cleanupTrunks(t, f)
	a, err := f.SpawnUnderRoot()
	if err != nil {
		t.Fatal(err)
	}
	b, err := f.SpawnUnderRoot()
	if err != nil {
		t.Fatal(err)
	}
	clk.set(2_000)
	if _, _, err := f.Append(a, 0, []byte(`"a"`), nil); err != nil {
		t.Fatal(err)
	}
	clk.set(9_000)
	patch, _ := MapSetPatch([]string{"k"}, []byte(`1`))
	if _, err := f.AppendChannel(string(b), "chalkboard", 0, patch, nil); err != nil {
		t.Fatal(err)
	}
	if got := f.LastTS(a); got != 2_000 {
		t.Errorf("LastTS(a) = %d, want 2000", got)
	}
	if got := f.LastTS(b); got != 9_000 {
		t.Errorf("LastTS(b) = %d, want 9000 (the unkeyed channel counts)", got)
	}
	if got := f.LastTS("no-such-trunk"); got != 0 {
		t.Errorf("LastTS(unknown) = %d, want 0", got)
	}
}
