package xwal

import (
	"os"
	"path/filepath"
	"testing"
)

func TestForkPreflightRepairBudget(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "f")
	f, trunk := seedTrunk(t, dir)
	repairs := 0
	f.testDeepRepair = func() { repairs++ }

	for range 32 {
		if _, err := f.ForkTail(trunk); err != nil {
			t.Fatal(err)
		}
	}
	if repairs != 0 {
		t.Fatalf("deep repairs for validated topology = %d, want 0", repairs)
	}

	branch := f.node(f.head(trunk)).Branch
	f.retireRootHot()
	marker := filepath.Join(append([]string{dir, "chalkboard"}, branch...)...)
	if err := os.Remove(filepath.Join(marker, ".fork")); err != nil {
		t.Fatal(err)
	}
	if _, err := f.ForkTail(trunk); err != nil {
		t.Fatal(err)
	}
	if _, err := f.ForkTail(trunk); err != nil {
		t.Fatal(err)
	}
	if repairs > 1 {
		t.Fatalf("deep repairs after one detected fault = %d, want at most 1", repairs)
	}
}

func BenchmarkForkPreflightCached(b *testing.B) {
	dir := filepath.Join(b.TempDir(), "f")
	f, err := createTrunks(dir, trunksCfg())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = f.Close() })
	branch := []string(nil)

	b.ResetTimer()
	for range b.N {
		x, err := f.openForkSource(branch)
		if err != nil {
			b.Fatal(err)
		}
		if err := x.Close(); err != nil {
			b.Fatal(err)
		}
	}
}
