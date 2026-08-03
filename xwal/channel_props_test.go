package xwal

import (
	"strings"
	"testing"
)

// A store written before a channel became unkeyed kept it keyed forever:
// the manifest is authoritative once a store exists, and nothing
// reconciled it. So the unkeyed design reached stores created by this
// build and no store anyone already had.
//
// The symptom, which is how a fuzz worker found it from the outside: a
// board patch written with no turn in flight is keyed ABOVE the tail, so a
// tail fork's child does not inherit it although the parent has it. The
// contract in channelBases says the opposite, in as many words.
func TestAKeyedChannelBecomesUnkeyedAndATailForkInheritsAgain(t *testing.T) {
	dir := t.TempDir()
	// A store from before: nothing declared unkeyed.
	old := testStoreOptions()
	s, err := OpenStore(dir, old)
	if err != nil {
		t.Fatal(err)
	}
	trunk, err := s.SpawnUnderRoot()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := s.Append(string(trunk), "ir", 0, []byte(`{"turn":1}`), nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if m, err := readManifestFile(dir); err != nil {
		t.Fatal(err)
	} else if chanSpec(m, "chalkboard").Unkeyed {
		t.Fatal("fixture is not an old store: its chalkboard is already unkeyed")
	}

	// This build declares it unkeyed.
	now := testStoreOptions()
	now.Unkeyed = []string{"chalkboard"}
	s2, err := OpenStore(dir, now)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if m, err := readManifestFile(dir); err != nil {
		t.Fatal(err)
	} else if !chanSpec(m, "chalkboard").Unkeyed {
		t.Fatal("opening with the new declaration left the channel keyed on disk")
	}

	// A patch with NO turn in flight, then a tail fork. The child must
	// start from the board the parent declared, patch included.
	if _, err := s2.Append(string(trunk), "chalkboard", 0, []byte(`{"k":"v"}`), nil); err != nil {
		t.Fatal(err)
	}
	child, err := s2.Fork(string(trunk), 0) // 0 == at the tail
	if err != nil {
		t.Fatal(err)
	}
	recs, err := s2.RecordsFrom(child, "chalkboard", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) == 0 {
		t.Fatal("the child inherited NO board records")
	}
	found := false
	for _, r := range recs {
		if strings.Contains(string(r.Payload), `"k"`) {
			found = true
		}
	}
	if !found {
		t.Errorf("the child inherited %d board records but not the one written since the last turn", len(recs))
	}
}

// The other direction has no honest answer, so it is refused rather than
// guessed: records already written carry no key, and reading them as keyed
// reads every one of them as key 0.
func TestAnUnkeyedChannelIsNotSilentlyMadeKeyedAgain(t *testing.T) {
	dir := t.TempDir()
	opts := testStoreOptions()
	opts.Unkeyed = []string{"chalkboard"}
	s, err := OpenStore(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenStore(dir, testStoreOptions()); err == nil ||
		!strings.Contains(err.Error(), "unkeyed on disk") {
		t.Fatalf("re-keying an unkeyed channel: got %v, want a refusal", err)
	}
}

func chanSpec(m manifest, name string) manifestChannel {
	for _, c := range m.Channels {
		if c.Name == name {
			return c
		}
	}
	return manifestChannel{}
}

// A channel named ONLY in Unkeyed -- not a reducer, not opaque -- was
// declared in no ChannelSpec at all, so reconciliation could not see it and
// an existing store kept it keyed forever. The new-store path hid it:
// autoCreateChannel reads the options directly.
func TestAnUnkeyedOnlyChannelIsDeclaredAndReconciled(t *testing.T) {
	dir := t.TempDir()
	old := testStoreOptions()
	s, err := OpenStore(dir, old)
	if err != nil {
		t.Fatal(err)
	}
	tr, err := s.SpawnUnderRoot()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Append(string(tr), "ir", 0, []byte(`{"turn":1}`), nil); err != nil {
		t.Fatal(err)
	}
	// A plain log channel, born keyed because nothing said otherwise.
	if _, err := s.Append(string(tr), "notes", 1, []byte(`{"n":1}`), nil); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	now := testStoreOptions()
	now.Unkeyed = []string{"notes"} // neither a reducer nor opaque
	s2, err := OpenStore(dir, now)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	m, err := readManifestFile(dir)
	if err != nil {
		t.Fatal(err)
	}
	if !chanSpec(m, "notes").Unkeyed {
		t.Error("an unkeyed-only channel stayed keyed on disk: it was declared nowhere")
	}
}

// Opaque drifts too, and safely: the decoder keys off the frame, so a
// channel that changes its mind reads back mixed records correctly.
func TestOpaqueReconcilesAndMixedRecordsStillRead(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenStore(dir, testStoreOptions())
	if err != nil {
		t.Fatal(err)
	}
	tr, err := s.SpawnUnderRoot()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Append(string(tr), "ir", 0, []byte(`{"turn":1}`), nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Append(string(tr), "notes", 1, []byte(`{"plain":1}`), nil); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	now := testStoreOptions()
	now.Opaque = []string{"notes"}
	s2, err := OpenStore(dir, now)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if m, err := readManifestFile(dir); err != nil {
		t.Fatal(err)
	} else if !chanSpec(m, "notes").Opaque {
		t.Fatal("the channel did not become opaque on disk")
	}
	if _, err := s2.Append(string(tr), "notes", 1, []byte(`{"encoded":1}`), nil); err != nil {
		t.Fatal(err)
	}
	recs, err := s2.RecordsFrom(string(tr), "notes", 0, 0)
	if err != nil {
		t.Fatalf("reading a channel with records from both sides of the flip: %v", err)
	}
	if len(recs) != 2 {
		t.Fatalf("read %d records, want 2", len(recs))
	}
	for _, r := range recs {
		if !strings.Contains(string(r.Payload), `1}`) {
			t.Errorf("record decoded wrong across the flip: %q", r.Payload)
		}
	}
}
