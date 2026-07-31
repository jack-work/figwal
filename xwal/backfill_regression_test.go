package xwal

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestEnsureExistingReduciblePreservesForkBases(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "f")
	f, trunk := seedTrunk(t, dir)
	_, mainLT, err := f.Append(trunk, 0, []byte(`"turn"`), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.AppendChannel(
		trunk,
		"chalkboard",
		mainLT,
		[]byte(`{"set":{"existing":"state"}}`),
		nil,
	); err != nil {
		t.Fatal(err)
	}
	alt, err := f.ForkTail(trunk)
	if err != nil {
		t.Fatal(err)
	}
	spec := ChannelSpec{Name: "chalkboard", Kind: ChannelReducible, Reducer: "jsonmerge"}
	if err := f.ensureChannel(spec); err != nil {
		t.Fatal(err)
	}
	assertNoBaseOneWatermarkUnderLaterFork(t, filepath.Join(dir, "chalkboard"))

	cfg := withChannelSpec(trunksCfg(), spec)
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := openTrunks(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	cleanupTrunks(t, reopened)
	for _, id := range []string{trunk, alt} {
		head, err := reopened.Head(id)
		if err != nil {
			t.Fatal(err)
		}
		state, err := head.StateAt("chalkboard", channelTail(head, "chalkboard"))
		head.Close()
		if err != nil {
			t.Fatalf("StateAt(%s): %v", id, err)
		}
		var got map[string]any
		if err := json.Unmarshal(state, &got); err != nil {
			t.Fatal(err)
		}
		if got["existing"] != "state" {
			t.Fatalf("state on %s = %s", id, state)
		}
	}
}

func TestEnsureNewReducibleRepairsMissingForkWatermark(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "f")
	f, trunk := seedTrunk(t, dir)
	spec := ChannelSpec{Name: "turn-state", Kind: ChannelReducible, Reducer: "jsonmerge"}
	if err := f.ensureChannel(spec); err != nil {
		t.Fatal(err)
	}
	_, mainLT, err := f.Append(trunk, 0, []byte(`"turn"`), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.AppendChannel(
		trunk,
		spec.Name,
		mainLT,
		[]byte(`{"set":{"shared":"value"}}`),
		nil,
	); err != nil {
		t.Fatal(err)
	}
	alt, err := f.ForkTail(trunk)
	if err != nil {
		t.Fatal(err)
	}

	branch := f.node(f.head(alt)).Branch
	nodeDir := filepath.Join(append([]string{dir, spec.Name}, branch...)...)
	base, err := readForkBaseFile(filepath.Join(nodeDir, ".fork"))
	if err != nil {
		t.Fatal(err)
	}
	if base <= 1 {
		t.Fatalf("fork base = %d, want > 1", base)
	}
	codec, err := codecByName(f.cfg.Codec)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(watermarkPath(nodeDir, base, codec)); err != nil {
		t.Fatal(err)
	}
	if err := f.ensureChannel(spec); err != nil {
		t.Fatalf("repair incomplete backfill: %v", err)
	}
	if !pathExists(watermarkPath(nodeDir, base, codec)) {
		t.Fatalf("watermark at base %d was not repaired", base)
	}
	if pathExists(watermarkPath(nodeDir, 1, codec)) {
		t.Fatal("repair injected a base-1 watermark")
	}

	cfg := withChannelSpec(trunksCfg(), spec)
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := openTrunks(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	cleanupTrunks(t, reopened)
	if _, err := reopened.AppendChannel(
		alt,
		spec.Name,
		mainLT,
		[]byte(`{"set":{"alternative":true}}`),
		nil,
	); err != nil {
		t.Fatal(err)
	}
	head, err := reopened.Head(alt)
	if err != nil {
		t.Fatal(err)
	}
	state, err := head.StateAt(spec.Name, channelTail(head, spec.Name))
	head.Close()
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(state, &got); err != nil {
		t.Fatal(err)
	}
	if got["shared"] != "value" || got["alternative"] != true {
		t.Fatalf("repaired state = %s", state)
	}
}

func TestForkPreflightRepairsMissingReducibleWatermark(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "f")
	f, trunk := seedTrunk(t, dir)
	_, mainLT, err := f.Append(trunk, 0, []byte(`"turn"`), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.AppendChannel(
		trunk,
		"chalkboard",
		mainLT,
		[]byte(`{"set":{"shared":"value"}}`),
		nil,
	); err != nil {
		t.Fatal(err)
	}
	alt, err := f.ForkTail(trunk)
	if err != nil {
		t.Fatal(err)
	}
	branch := append([]string(nil), f.node(f.head(alt)).Branch...)
	nodeDir := filepath.Join(append([]string{dir, "chalkboard"}, branch...)...)
	base, err := readForkBaseFile(filepath.Join(nodeDir, ".fork"))
	if err != nil {
		t.Fatal(err)
	}
	codec, err := codecByName(f.cfg.Codec)
	if err != nil {
		t.Fatal(err)
	}
	f.retireRootHot()
	if err := os.Remove(watermarkPath(nodeDir, base, codec)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := f.Append(alt, 0, []byte(`"alternative"`), nil); err != nil {
		t.Fatal(err)
	}
	if _, err := f.ForkTail(alt); err != nil {
		t.Fatalf("fork after missing watermark: %v", err)
	}
	state, err := func() ([]byte, error) {
		parentDir := filepath.Dir(nodeDir)
		x, err := Open(dir, f.cfg, branch...)
		if err != nil {
			return nil, err
		}
		defer x.Close()
		return x.backfillWatermarkState(parentDir, base, x.chans["chalkboard"])
	}()
	if err != nil {
		t.Fatal(err)
	}
	valid, err := validateWatermark(nodeDir, base, codec, state)
	if err != nil || !valid {
		t.Fatalf("repaired watermark valid=%v err=%v", valid, err)
	}
	if base > 1 && pathExists(watermarkPath(nodeDir, 1, codec)) {
		t.Fatal("fork preflight injected a base-1 watermark")
	}
	if pathExists(filepath.Join(dir, channelPendingName)) {
		t.Fatal("fork preflight left a pending channel plan")
	}
}

func TestForkPreflightRepairsDeepEmptyBaseTwoWatermark(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "f")
	f, trunk := seedTrunk(t, dir)
	for i := 0; i < 14; i++ {
		if _, _, err := f.Append(trunk, 0, []byte(fmt.Sprintf(`"turn-%d"`, i)), nil); err != nil {
			t.Fatal(err)
		}
		if _, err := f.ForkTail(trunk); err != nil {
			t.Fatal(err)
		}
	}

	branch := append([]string(nil), f.node(f.head(trunk)).Branch...)
	targetBranch := branch[:len(branch)-4]
	nodeDir := filepath.Join(append([]string{dir, "chalkboard"}, targetBranch...)...)
	base, err := readForkBaseFile(filepath.Join(nodeDir, ".fork"))
	if err != nil {
		t.Fatal(err)
	}
	if base != 2 {
		t.Fatalf("fork base = %d, want 2", base)
	}
	codec, err := codecByName(f.cfg.Codec)
	if err != nil {
		t.Fatal(err)
	}
	f.retireRootHot()
	if err := os.Remove(watermarkPath(nodeDir, base, codec)); err != nil {
		t.Fatal(err)
	}

	if _, _, err := f.Append(trunk, 0, []byte(`"deep"`), nil); err != nil {
		t.Fatal(err)
	}
	if _, err := f.ForkTail(trunk); err != nil {
		t.Fatalf("fork after deep empty watermark removal: %v", err)
	}

	parentDir := filepath.Dir(nodeDir)
	ch, err := channelFromManifest(f.cfg, manifestChannel{
		Name: "chalkboard", Kind: ChannelReducible.String(), Reducer: "jsonmerge",
	})
	if err != nil {
		t.Fatal(err)
	}
	recovery := &XWAL{root: dir, main: "ir", cfg: f.cfg, codec: codec}
	state, err := recovery.backfillWatermarkState(parentDir, base, ch)
	if err != nil {
		t.Fatal(err)
	}
	valid, err := validateWatermark(nodeDir, base, codec, state)
	if err != nil || !valid {
		t.Fatalf("deep repaired watermark valid=%v err=%v", valid, err)
	}
	if pathExists(watermarkPath(nodeDir, 1, codec)) {
		t.Fatal("deep repair injected a base-1 watermark")
	}
	if pathExists(filepath.Join(dir, channelPendingName)) {
		t.Fatal("deep repair left a pending channel plan")
	}
}

func TestForkPreflightRepairsWatermarkHeaderPreservingEntries(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "f")
	f, trunk := seedTrunk(t, dir)
	_, mainLT, err := f.Append(trunk, 0, []byte(`"turn-1"`), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.AppendChannel(
		trunk, "chalkboard", mainLT, []byte(`{"set":{"shared":"value"}}`), nil,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := f.ForkTail(trunk); err != nil {
		t.Fatal(err)
	}
	_, mainLT, err = f.Append(trunk, 0, []byte(`"turn-2"`), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.AppendChannel(
		trunk, "chalkboard", mainLT, []byte(`{"set":{"own":"entry"}}`), nil,
	); err != nil {
		t.Fatal(err)
	}

	branch := append([]string(nil), f.node(f.head(trunk)).Branch...)
	nodeDir := filepath.Join(append([]string{dir, "chalkboard"}, branch...)...)
	base, err := readForkBaseFile(filepath.Join(nodeDir, ".fork"))
	if err != nil {
		t.Fatal(err)
	}
	if base != 3 {
		t.Fatalf("fork base = %d, want 3", base)
	}
	codec, err := codecByName(f.cfg.Codec)
	if err != nil {
		t.Fatal(err)
	}
	f.retireRootHot()
	_, beforeEntries, ok, err := readHeaderedSegment(watermarkPath(nodeDir, base, codec), codec)
	if err != nil || !ok || len(beforeEntries) == 0 {
		t.Fatalf("before entries=%d ok=%v err=%v", len(beforeEntries), ok, err)
	}
	if err := rewriteWatermark(nodeDir, base, codec, []byte(`{"wrong":true}`)); err != nil {
		t.Fatal(err)
	}

	if _, err := f.ForkTail(trunk); err != nil {
		t.Fatalf("fork after invalid populated watermark: %v", err)
	}

	parentDir := filepath.Dir(nodeDir)
	ch, err := channelFromManifest(f.cfg, manifestChannel{
		Name: "chalkboard", Kind: ChannelReducible.String(), Reducer: "jsonmerge",
	})
	if err != nil {
		t.Fatal(err)
	}
	recovery := &XWAL{root: dir, main: "ir", cfg: f.cfg, codec: codec}
	expected, err := recovery.backfillWatermarkState(parentDir, base, ch)
	if err != nil {
		t.Fatal(err)
	}
	header, afterEntries, ok, err := readHeaderedSegment(watermarkPath(nodeDir, base, codec), codec)
	if err != nil || !ok {
		t.Fatalf("repaired segment ok=%v err=%v", ok, err)
	}
	if !bytes.Equal(header, expected) {
		t.Fatalf("header = %s, want %s", header, expected)
	}
	if !reflect.DeepEqual(afterEntries, beforeEntries) {
		t.Fatalf("entries changed:\nbefore=%q\nafter=%q", beforeEntries, afterEntries)
	}
}

func TestForkPreflightRepairsSafeLegacyChannel(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "f")
	f, trunk := seedTrunk(t, dir)
	spec := ChannelSpec{Name: "translations/legacy", Kind: ChannelLog, Opaque: true}
	if err := f.ensureChannel(spec); err != nil {
		t.Fatal(err)
	}
	_, mainLT, err := f.Append(trunk, 0, []byte(`"turn"`), nil)
	if err != nil {
		t.Fatal(err)
	}

	if err := os.RemoveAll(filepath.Join(dir, spec.Name, "s")); err != nil {
		t.Fatal(err)
	}
	root, err := Open(dir, f.cfg)
	if err != nil {
		t.Fatal(err)
	}
	inheritedMainLT := mainLT - 1
	if _, err := root.Append(spec.Name, inheritedMainLT, opaquePayloadOne, nil); err != nil {
		root.Close()
		t.Fatal(err)
	}
	if err := root.Close(); err != nil {
		t.Fatal(err)
	}

	alt, err := f.ForkTail(trunk)
	if err != nil {
		t.Fatalf("fork after safe legacy repair: %v", err)
	}
	if _, err := f.AppendChannel(trunk, spec.Name, mainLT, opaquePayloadTwo, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := f.AppendChannel(alt, spec.Name, mainLT, opaquePayloadAlt, nil); err != nil {
		t.Fatal(err)
	}
	assertOpaqueChannelRecords(t, f, trunk, spec.Name, inheritedMainLT,
		[][]byte{opaquePayloadOne, opaquePayloadTwo})
	assertOpaqueChannelRecords(t, f, alt, spec.Name, inheritedMainLT,
		[][]byte{opaquePayloadOne, opaquePayloadAlt})

	if got, want := relDirs(t, filepath.Join(dir, spec.Name)), relDirs(t, filepath.Join(dir, "ir")); !reflect.DeepEqual(got, want) {
		t.Fatalf("upgraded channel tree differs from main:\nchannel=%v\nmain=%v", got, want)
	}

	cfg := f.cfg
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := openTrunks(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	cleanupTrunks(t, reopened)
	assertOpaqueChannelRecords(t, reopened, trunk, spec.Name, inheritedMainLT,
		[][]byte{opaquePayloadOne, opaquePayloadTwo})
	assertOpaqueChannelRecords(t, reopened, alt, spec.Name, inheritedMainLT,
		[][]byte{opaquePayloadOne, opaquePayloadAlt})
}

func TestInteriorForkRejectsAmbiguousLegacyFuture(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "f")
	f, trunk := seedTrunk(t, dir)
	spec := ChannelSpec{Name: "translations/legacy-interior", Kind: ChannelLog, Opaque: true}
	if err := f.ensureChannel(spec); err != nil {
		t.Fatal(err)
	}
	_, mainLT2, err := f.Append(trunk, 0, []byte(`"turn-1"`), nil)
	if err != nil {
		t.Fatal(err)
	}
	_, mainLT3, err := f.Append(trunk, 0, []byte(`"turn-2"`), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(filepath.Join(dir, spec.Name, "s")); err != nil {
		t.Fatal(err)
	}
	root, err := Open(dir, f.cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := root.Append(spec.Name, mainLT2, opaquePayloadOne, nil); err != nil {
		root.Close()
		t.Fatal(err)
	}
	if _, err := root.Append(spec.Name, mainLT3, opaquePayloadTwo, nil); err != nil {
		root.Close()
		t.Fatal(err)
	}
	if err := root.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := f.ForkAt(trunk, mainLT2); !errors.Is(err, ErrTopologyIncomplete) {
		t.Fatalf("fork error = %v, want ErrTopologyIncomplete", err)
	}
	if err := f.ensureChannel(spec); !errors.Is(err, ErrTopologyIncomplete) {
		t.Fatalf("EnsureChannel error = %v, want ambiguous topology rejection", err)
	}
	if pathExists(filepath.Join(dir, forkPendingName)) {
		t.Fatal("rejected interior fork wrote a fork plan")
	}
	if !pathExists(filepath.Join(dir, channelPendingName)) {
		t.Fatal("failed repair did not preserve its recoverable pending plan")
	}
}

func TestEnsureRepairsReducibleArtifactsBeforeFork(t *testing.T) {
	for _, fault := range []string{
		"empty-watermark",
		"partial-watermark",
		"wrong-watermark",
		"missing-marker",
		"invalid-marker",
	} {
		t.Run(fault, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "f")
			f, trunk := seedTrunk(t, dir)
			spec := ChannelSpec{Name: "turn-state", Kind: ChannelReducible, Reducer: "jsonmerge"}
			if err := f.ensureChannel(spec); err != nil {
				t.Fatal(err)
			}
			_, mainLT, err := f.Append(trunk, 0, []byte(`"turn"`), nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := f.AppendChannel(
				trunk,
				spec.Name,
				mainLT,
				[]byte(`{"set":{"shared":"value"}}`),
				nil,
			); err != nil {
				t.Fatal(err)
			}
			if _, err := f.ForkTail(trunk); err != nil {
				t.Fatal(err)
			}
			if _, _, err := f.Append(trunk, 0, []byte(`"continuation"`), nil); err != nil {
				t.Fatal(err)
			}

			branch := f.node(f.head(trunk)).Branch
			nodeDir := filepath.Join(append([]string{dir, spec.Name}, branch...)...)
			base, err := readForkBaseFile(filepath.Join(nodeDir, ".fork"))
			if err != nil {
				t.Fatal(err)
			}
			codec, err := codecByName(f.cfg.Codec)
			if err != nil {
				t.Fatal(err)
			}
			f.retireRootHot()
			switch fault {
			case "empty-watermark":
				if err := os.WriteFile(watermarkPath(nodeDir, base, codec), nil, 0o644); err != nil {
					t.Fatal(err)
				}
			case "partial-watermark":
				if err := os.WriteFile(watermarkPath(nodeDir, base, codec), []byte(`{"partial"`), 0o644); err != nil {
					t.Fatal(err)
				}
			case "wrong-watermark":
				if err := seedWatermark(nodeDir, base, codec, []byte(`{"wrong":true}`)); err != nil {
					t.Fatal(err)
				}
			case "missing-marker":
				if err := os.Remove(filepath.Join(nodeDir, ".fork")); err != nil {
					t.Fatal(err)
				}
			case "invalid-marker":
				if err := os.WriteFile(filepath.Join(nodeDir, ".fork"), []byte("base=nope\n"), 0o644); err != nil {
					t.Fatal(err)
				}
			}

			if _, err := f.ForkTail(trunk); err != nil {
				t.Fatalf("fork after repairing %s: %v", fault, err)
			}
			if err := f.ensureChannel(spec); err != nil {
				t.Fatalf("idempotent EnsureChannel after %s: %v", fault, err)
			}
			repairedBase, err := readForkBaseFile(filepath.Join(nodeDir, ".fork"))
			if err != nil {
				t.Fatal(err)
			}
			state, err := func() ([]byte, error) {
				parentDir := filepath.Dir(nodeDir)
				x, err := Open(dir, f.cfg, branch...)
				if err != nil {
					return nil, err
				}
				defer x.Close()
				return x.backfillWatermarkState(parentDir, repairedBase, x.chans[spec.Name])
			}()
			if err != nil {
				t.Fatal(err)
			}
			valid, err := validateWatermark(nodeDir, repairedBase, codec, state)
			if err != nil || !valid {
				t.Fatalf("watermark valid=%v err=%v", valid, err)
			}
			if pathExists(watermarkPath(nodeDir, repairedBase, codec)+".tmp") ||
				pathExists(watermarkPath(nodeDir, repairedBase, codec)+".invalid") {
				t.Fatal("watermark repair left staging files")
			}
			if pathExists(filepath.Join(nodeDir, ".fork.tmp")) ||
				pathExists(filepath.Join(nodeDir, ".fork.invalid")) {
				t.Fatal("marker repair left staging files")
			}
		})
	}
}

func TestEmptyParentChannelOrderingAndLatest(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{
		Main: "ir",
		Channels: []ChannelSpec{
			{Name: "ir", Kind: ChannelLog},
			{Name: "turn-wal", Kind: ChannelLog, Opaque: true},
		},
	}
	x, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := x.AppendMain([]byte(`"turn"`), nil); err != nil {
		t.Fatal(err)
	}
	child, err := x.Fork(2, "alternative", "continuation")
	if err != nil {
		t.Fatal(err)
	}
	x.Close()
	defer child.Close()
	for i, payload := range [][]byte{opaquePayloadOne, opaquePayloadTwo} {
		if _, err := child.Append("turn-wal", 1, payload, nil); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if got := child.chans["turn-wal"].log.FirstIndex(); got != 1 {
		t.Fatalf("FirstIndex = %d, want 1", got)
	}
	records, err := child.RecordsFrom("turn-wal", 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 || records[0].ChannelLT != 1 || records[1].ChannelLT != 2 {
		t.Fatalf("records out of order: %+v", records)
	}
	if !bytes.Equal(records[0].Payload, opaquePayloadOne) ||
		!bytes.Equal(records[1].Payload, opaquePayloadTwo) {
		t.Fatalf("payloads changed: %+v", records)
	}
	latest, ok, err := child.LatestChannelRecord("turn-wal", 1)
	if err != nil || !ok || latest.ChannelLT != 2 || !bytes.Equal(latest.Payload, opaquePayloadTwo) {
		t.Fatalf("latest = %+v ok=%v err=%v", latest, ok, err)
	}
}

func TestForkPreservesEmptyClearedChannelRoot(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "f")
	f, trunk := seedTrunk(t, dir)
	spec := ChannelSpec{Name: "translations/clear", Kind: ChannelLog, Opaque: true}
	if err := f.ensureChannel(spec); err != nil {
		t.Fatal(err)
	}
	_, mainLT, err := f.Append(trunk, 0, []byte(`"turn"`), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.AppendChannel(trunk, spec.Name, mainLT, opaquePayloadOne, nil); err != nil {
		t.Fatal(err)
	}
	head, err := f.Head(trunk)
	if err != nil {
		t.Fatal(err)
	}
	if err := head.Clear(spec.Name); err != nil {
		head.Close()
		t.Fatal(err)
	}
	if err := head.Close(); err != nil {
		t.Fatal(err)
	}
	alt, err := f.ForkTail(trunk)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{trunk, alt} {
		head, err := f.Head(id)
		if err != nil {
			t.Fatal(err)
		}
		records, err := head.RecordsFrom(spec.Name, 0, 0)
		head.Close()
		if err != nil {
			t.Fatal(err)
		}
		if len(records) != 0 {
			t.Fatalf("cleared channel on %s resurrected records: %+v", id, records)
		}
	}
}

func assertNoBaseOneWatermarkUnderLaterFork(t *testing.T, root string) {
	t.Helper()
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !entry.IsDir() || path == root {
			return nil
		}
		base, err := readForkBaseFile(filepath.Join(path, ".fork"))
		if err != nil {
			return err
		}
		if base > 1 && pathExists(filepath.Join(path, fmt.Sprintf("%020d.jsonl", 1))) {
			return fmt.Errorf("%s has base=%d and a base-1 watermark", path, base)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
