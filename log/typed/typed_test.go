package typed

import (
	"github.com/jack-work/figwal/log"
	"github.com/jack-work/figwal/segment"
	"testing"
)

type Order struct {
	ID   int    `json:"id"`
	Item string `json:"item"`
}

func TestRoundTripJSONL(t *testing.T) {
	tl, err := Open[Order](t.TempDir(), log.Options{Codec: segment.JSONLCodec{}})
	if err != nil {
		t.Fatal(err)
	}
	defer tl.Close()

	in := Order{ID: 7, Item: "espresso"}
	if err := tl.Write(1, in); err != nil {
		t.Fatal(err)
	}
	out, err := tl.Read(1)
	if err != nil {
		t.Fatal(err)
	}
	if out != in {
		t.Fatalf("got %+v want %+v", out, in)
	}
}

func TestRoundTripBinary(t *testing.T) {
	// Typed works over any codec; binary just stores length-prefixed JSON bytes.
	tl, err := Open[Order](t.TempDir(), log.Options{Codec: segment.BinaryCodec{}})
	if err != nil {
		t.Fatal(err)
	}
	defer tl.Close()

	in := Order{ID: 7, Item: "espresso"}
	if err := tl.Write(1, in); err != nil {
		t.Fatal(err)
	}
	out, _ := tl.Read(1)
	if out != in {
		t.Fatalf("got %+v want %+v", out, in)
	}
}

func TestHashInheritedJSONL(t *testing.T) {
	tl, _ := Open[Order](t.TempDir(), log.Options{Codec: segment.JSONLCodec{}})
	defer tl.Close()
	tl.Write(1, Order{ID: 1, Item: "a"})
	h, err := tl.Hash(1)
	if err != nil {
		t.Fatal(err)
	}
	if len(h) != 16 {
		t.Fatalf("expected jsonl 16-char hash, got %d (%q)", len(h), h)
	}
}

func TestHashInheritedBinary(t *testing.T) {
	tl, _ := Open[Order](t.TempDir(), log.Options{Codec: segment.BinaryCodec{}})
	defer tl.Close()
	tl.Write(1, Order{ID: 1, Item: "a"})
	h, err := tl.Hash(1)
	if err != nil {
		t.Fatal(err)
	}
	if len(h) != 8 {
		t.Fatalf("expected binary 8-char crc32 hex, got %d (%q)", len(h), h)
	}
}

func TestEmbeddedLogAccessible(t *testing.T) {
	tl, _ := Open[Order](t.TempDir(), log.Options{Codec: segment.JSONLCodec{}})
	defer tl.Close()
	tl.Write(1, Order{ID: 1, Item: "a"})
	// Escape hatch: byte-oriented Read via the embedded *log.Log.
	if _, err := tl.Log.Read(1); err != nil {
		t.Fatal(err)
	}
}
