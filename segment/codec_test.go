package segment

import (
	"bytes"
	"strings"
	"testing"
)

func TestBinaryCodecRoundTrip(t *testing.T) {
	c := BinaryCodec{}
	cases := [][]byte{
		[]byte("hello"),
		{},
		bytes.Repeat([]byte{0xAB}, 10_000),
	}
	for _, want := range cases {
		frame, err := c.Frame(0, want)
		if err != nil {
			t.Fatal(err)
		}
		got, n, err := c.ReadFrame(bytes.NewReader(frame), 0, int64(len(frame)))
		if err != nil {
			t.Fatalf("ReadFrame: %v", err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("payload mismatch")
		}
		if n != headerSize+len(want) {
			t.Fatalf("n=%d want %d", n, headerSize+len(want))
		}
	}
}

func TestBinaryCodecCorruptCRC(t *testing.T) {
	c := BinaryCodec{}
	frame, _ := c.Frame(0, []byte("hello"))
	frame[headerSize] ^= 0xFF
	if _, _, err := c.ReadFrame(bytes.NewReader(frame), 0, int64(len(frame))); err != ErrCorrupt {
		t.Fatalf("want ErrCorrupt, got %v", err)
	}
}

func TestBinaryCodecScanFrames(t *testing.T) {
	c := BinaryCodec{}
	var buf bytes.Buffer
	for _, p := range [][]byte{[]byte("a"), []byte("bb"), []byte("ccc")} {
		f, _ := c.Frame(0, p)
		buf.Write(f)
	}
	var offsets []int64
	err := c.ScanFrames(&buf, func(off int64, _ int) error {
		offsets = append(offsets, off)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(offsets) != 3 {
		t.Fatalf("got %d offsets", len(offsets))
	}
}

func TestJSONLCodecRoundTrip(t *testing.T) {
	c := JSONLCodec{}
	cases := []string{
		`{"hello":"world"}`,
		`[1,2,3]`,
		`null`,
		`42`,
	}
	for i, in := range cases {
		frame, err := c.Frame(uint64(i), []byte(in))
		if err != nil {
			t.Fatalf("Frame(%s): %v", in, err)
		}
		if frame[len(frame)-1] != '\n' {
			t.Fatal("expected trailing newline")
		}
		got, n, err := c.ReadFrame(bytes.NewReader(frame), 0, int64(len(frame)))
		if err != nil {
			t.Fatal(err)
		}
		if n != len(frame) {
			t.Fatalf("n=%d want %d", n, len(frame))
		}
		if string(got) != in {
			t.Fatalf("got %q want %q", got, in)
		}
	}
}

func TestJSONLCodecRejectsNonJSON(t *testing.T) {
	c := JSONLCodec{}
	if _, err := c.Frame(0, []byte("hello world")); err != ErrNotJSON {
		t.Fatalf("want ErrNotJSON, got %v", err)
	}
}

func TestJSONLCodecEnvelopeShape(t *testing.T) {
	c := JSONLCodec{}
	frame, err := c.Frame(7, []byte(`{"a":1}`))
	if err != nil {
		t.Fatal(err)
	}
	s := string(frame)
	for _, want := range []string{`"idx":7`, `"hash":`, `"value":{"a":1}`} {
		if !strings.Contains(s, want) {
			t.Fatalf("frame missing %q: %s", want, s)
		}
	}
}

func TestJSONLCodecHashTamperDetected(t *testing.T) {
	c := JSONLCodec{}
	frame, _ := c.Frame(0, []byte(`{"a":1}`))
	// Mutate the value bytes; hash should no longer match.
	tampered := bytes.Replace(frame, []byte(`"a":1`), []byte(`"a":2`), 1)
	if _, _, err := c.ReadFrame(bytes.NewReader(tampered), 0, int64(len(tampered))); err != ErrCorrupt {
		t.Fatalf("want ErrCorrupt, got %v", err)
	}
}

func TestJSONLCodecScanFrames(t *testing.T) {
	c := JSONLCodec{}
	var buf bytes.Buffer
	for i, p := range []string{`{"i":1}`, `{"i":2}`, `{"i":3}`} {
		f, _ := c.Frame(uint64(i), []byte(p))
		buf.Write(f)
	}
	var offsets []int64
	err := c.ScanFrames(&buf, func(off int64, _ int) error {
		offsets = append(offsets, off)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(offsets) != 3 {
		t.Fatalf("got %d offsets", len(offsets))
	}
}

func TestJSONLCodecScanStopsAtTornTail(t *testing.T) {
	c := JSONLCodec{}
	var buf bytes.Buffer
	good, _ := c.Frame(0, []byte(`{"i":1}`))
	buf.Write(good)
	buf.WriteString("not-json-at-all\n")
	var n int
	c.ScanFrames(&buf, func(_ int64, _ int) error { n++; return nil })
	if n != 1 {
		t.Fatalf("want 1 frame before stop, got %d", n)
	}
}
