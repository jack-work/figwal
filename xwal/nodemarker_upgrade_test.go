package xwal

import (
	"os"
	"path/filepath"
	"testing"
)

// The .node upgrade runs on the READ path and is optional: both forms are
// ground truth. So a store that cannot be written to -- read-only media, a
// full disk -- must still OPEN. Returning the write error aborts
// RebuildFrom and therefore the open itself, turning an optimisation into
// the reason the store is unreachable.
func TestLegacyMarkersOpenWhenTheUpgradeCannotBeWritten(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "f")
	f, trunk := seedTrunk(t, dir)
	if _, _, err := f.Append(trunk, 0, []byte(`"turn"`), nil); err != nil {
		t.Fatal(err)
	}
	key := f.head(string(trunk))
	nodeDir := f.irDir(key)
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	// Rewind this node to the legacy pair, then make its directory
	// unwritable so the upgrade cannot land.
	n, ok, _ := readNodeMarker(nodeDir)
	if !ok {
		t.Fatalf("no marker at %s", nodeDir)
	}
	if err := os.Remove(filepath.Join(nodeDir, nodeMarkerName)); err != nil {
		t.Fatal(err)
	}
	if err := writeSyncedFile(filepath.Join(nodeDir, legacyFromName),
		[]byte(n.from+"\n"+n.kind+"\n")); err != nil {
		t.Fatal(err)
	}
	if n.trunk != "" {
		if err := writeSyncedFile(filepath.Join(nodeDir, legacyTrunkName), []byte(n.trunk+"\n")); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Chmod(nodeDir, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(nodeDir, 0o755) })

	g, err := openTrunks(dir, trunksCfg())
	if err != nil {
		t.Fatalf("a store on read-only media must still open: %v", err)
	}
	cleanupTrunks(t, g)
	if _, ok := g.idx.Head(trunk); !ok {
		t.Error("the legacy node lost its head after opening")
	}
}

// A completed upgrade leaves ONE identity file, not three. Otherwise every
// node carries both forms forever and a later reader has two sources to
// disagree about.
func TestUpgradeRetiresTheLegacyPair(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "f")
	f, trunk := seedTrunk(t, dir)
	if _, _, err := f.Append(trunk, 0, []byte(`"turn"`), nil); err != nil {
		t.Fatal(err)
	}
	key := f.head(string(trunk))
	nodeDir := f.irDir(key)
	n, _, _ := readNodeMarker(nodeDir)
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(nodeDir, nodeMarkerName)); err != nil {
		t.Fatal(err)
	}
	if err := writeSyncedFile(filepath.Join(nodeDir, legacyFromName),
		[]byte(n.from+"\n"+n.kind+"\n")); err != nil {
		t.Fatal(err)
	}
	if err := writeSyncedFile(filepath.Join(nodeDir, legacyTrunkName), []byte(n.trunk+"\n")); err != nil {
		t.Fatal(err)
	}

	g, err := openTrunks(dir, trunksCfg())
	if err != nil {
		t.Fatal(err)
	}
	cleanupTrunks(t, g)
	if _, err := os.Stat(filepath.Join(nodeDir, nodeMarkerName)); err != nil {
		t.Fatalf("upgrade did not write %s: %v", nodeMarkerName, err)
	}
	for _, legacy := range []string{legacyFromName, legacyTrunkName} {
		if _, err := os.Stat(filepath.Join(nodeDir, legacy)); !os.IsNotExist(err) {
			t.Errorf("%s survived the upgrade (err=%v)", legacy, err)
		}
	}
}
