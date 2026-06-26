package xwal

import "testing"

// Covers the figaro-driven additions: per-entry meta, FK lookup,
// per-channel clear, and dynamic add-channel.
func TestXWAL_MetaAndLookup(t *testing.T) {
	dir := t.TempDir()
	x, _ := triune(t, dir)
	defer x.Close()

	m1, err := x.AppendMain([]byte(`{"role":"user"}`), []byte(`{"fp":"v0"}`))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := x.Append("translations", m1, []byte(`["wire"]`), []byte(`"anthropic/v0"`)); err != nil {
		t.Fatal(err)
	}

	// Meta round-trips on the main channel.
	r, err := x.ReadAt("ir", m1)
	if err != nil {
		t.Fatal(err)
	}
	if string(r.Meta) != `{"fp":"v0"}` {
		t.Fatalf("ir meta = %s, want {\"fp\":\"v0\"}", r.Meta)
	}

	// FK lookup on translations by main LT, with its (string) meta.
	got, ok, err := x.Lookup("translations", m1)
	if err != nil || !ok {
		t.Fatalf("Lookup miss: ok=%v err=%v", ok, err)
	}
	if string(got.Meta) != `"anthropic/v0"` {
		t.Fatalf("translation meta = %s, want quoted anthropic/v0", got.Meta)
	}

	// FK index survives reopen.
	x.Close()
	x2, _ := triune(t, dir)
	defer x2.Close()
	if _, ok, _ := x2.Lookup("translations", m1); !ok {
		t.Fatal("Lookup miss after reopen")
	}
}

func TestXWAL_Clear(t *testing.T) {
	dir := t.TempDir()
	x, _ := triune(t, dir)
	defer x.Close()
	m1, _ := x.AppendMain([]byte(`{"a":1}`), nil)
	x.Append("translations", m1, []byte(`["x"]`), []byte(`"fp1"`))

	if err := x.Clear("translations"); err != nil {
		t.Fatalf("clear: %v", err)
	}
	if _, ok, _ := x.Lookup("translations", m1); ok {
		t.Fatal("translation survived Clear")
	}
	// Channel is reusable after clear.
	if _, err := x.Append("translations", m1, []byte(`["y"]`), []byte(`"fp2"`)); err != nil {
		t.Fatalf("append after clear: %v", err)
	}
	if got, ok, _ := x.Lookup("translations", m1); !ok || string(got.Meta) != `"fp2"` {
		t.Fatalf("post-clear lookup = (%q,%v)", got.Meta, ok)
	}
}

func TestXWAL_AddChannel(t *testing.T) {
	dir := t.TempDir()
	x, _ := triune(t, dir)
	m1, _ := x.AppendMain([]byte(`{"a":1}`), nil)

	// Add a translation channel for a newly-seen provider.
	if err := x.AddChannel(ChannelSpec{Name: "translations/openai", Kind: ChannelLog}); err != nil {
		t.Fatalf("AddChannel: %v", err)
	}
	if _, err := x.Append("translations/openai", m1, []byte(`["wire"]`), nil); err != nil {
		t.Fatalf("append to new channel: %v", err)
	}
	x.Close()

	// New channel persisted in the manifest and reopens.
	x2, _ := triune(t, dir)
	defer x2.Close()
	if _, ok, _ := x2.Lookup("translations/openai", m1); !ok {
		t.Fatal("added channel not found after reopen")
	}
}
