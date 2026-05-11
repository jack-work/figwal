package segment

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
)

var (
	ErrCorrupt = errors.New("corrupt entry")
	ErrNotJSON = errors.New("payload is not valid JSON")
)

// SegmentCodec abstracts the on-disk representation of one entry.
type SegmentCodec interface {
	// Frame wraps payload into the on-disk byte sequence for one entry,
	// including any framing terminator. idx is the global log index of
	// the entry. Codecs that don't carry it on disk may ignore it.
	Frame(idx uint64, payload []byte) ([]byte, error)

	// ReadFrame reads one entry from r at offset off. nextOff is the byte
	// offset of the following entry, or the segment size for the last
	// entry; codecs that self-describe length may ignore it.
	ReadFrame(r io.ReaderAt, off, nextOff int64) (payload []byte, frameLen int, err error)

	// ScanFrames enumerates every frame in r in order, calling fn with
	// each frame's offset and length. Returns nil on torn tail or clean EOF.
	ScanFrames(r io.Reader, fn func(off int64, frameLen int) error) error

	// Hash returns the integrity token this codec uses for payload, as a
	// short hex string. Binary returns the CRC32; JSONL returns the
	// truncated value-stable SHA-256.
	Hash(payload []byte) (string, error)

	FileExt() string
	Name() string
}

const headerSize = 8

// BinaryCodec is length-prefixed + CRC32, opaque bytes.
type BinaryCodec struct{}

func (BinaryCodec) Name() string    { return "binary" }
func (BinaryCodec) FileExt() string { return ".seg" }

func (BinaryCodec) Hash(payload []byte) (string, error) {
	return fmt.Sprintf("%08x", crc32.ChecksumIEEE(payload)), nil
}

func (BinaryCodec) Frame(_ uint64, payload []byte) ([]byte, error) {
	buf := make([]byte, headerSize+len(payload))
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(buf[4:8], crc32.ChecksumIEEE(payload))
	copy(buf[headerSize:], payload)
	return buf, nil
}

func (BinaryCodec) ReadFrame(r io.ReaderAt, off, _ int64) ([]byte, int, error) {
	var hdr [headerSize]byte
	if _, err := r.ReadAt(hdr[:], off); err != nil {
		return nil, 0, err
	}
	n := binary.LittleEndian.Uint32(hdr[0:4])
	payload := make([]byte, n)
	if n > 0 {
		if _, err := r.ReadAt(payload, off+headerSize); err != nil {
			return nil, 0, err
		}
	}
	sum := binary.LittleEndian.Uint32(hdr[4:8])
	if crc32.ChecksumIEEE(payload) != sum {
		return nil, 0, ErrCorrupt
	}
	return payload, headerSize + int(n), nil
}

func (BinaryCodec) ScanFrames(r io.Reader, fn func(off int64, frameLen int) error) error {
	off := int64(0)
	for {
		var hdr [headerSize]byte
		_, err := io.ReadFull(r, hdr[:])
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil
		}
		if err != nil {
			return err
		}
		n := binary.LittleEndian.Uint32(hdr[0:4])
		payload := make([]byte, n)
		if _, err := io.ReadFull(r, payload); err != nil {
			return nil
		}
		sum := binary.LittleEndian.Uint32(hdr[4:8])
		if crc32.ChecksumIEEE(payload) != sum {
			return nil
		}
		frameLen := headerSize + int(n)
		if err := fn(off, frameLen); err != nil {
			return err
		}
		off += int64(frameLen)
	}
}

// JSONLCodec stores entries as one JSON envelope per line:
//
//	{"idx":N,"hash":"<16 hex>","value":<payload>}
//
// `value` is the user payload as raw JSON (no escaping). `hash` is the
// truncated value-stable hash of the canonical JSON form of the payload.
// Frame returns ErrNotJSON if payload is not valid JSON; ReadFrame
// returns ErrCorrupt if the envelope is malformed or the hash does not
// match the payload.
type JSONLCodec struct{}

func (JSONLCodec) Name() string    { return "jsonl" }
func (JSONLCodec) FileExt() string { return ".jsonl" }

func (JSONLCodec) Hash(payload []byte) (string, error) {
	return ValueHash(payload)
}

type jsonlEnvelope struct {
	Idx   uint64          `json:"idx"`
	Hash  string          `json:"hash"`
	Value json.RawMessage `json:"value"`
}

func (JSONLCodec) Frame(idx uint64, payload []byte) ([]byte, error) {
	if !json.Valid(payload) {
		return nil, ErrNotJSON
	}
	hash, err := ValueHash(payload)
	if err != nil {
		return nil, err
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, payload); err != nil {
		return nil, err
	}
	env := jsonlEnvelope{Idx: idx, Hash: hash, Value: compact.Bytes()}
	out, err := json.Marshal(env)
	if err != nil {
		return nil, err
	}
	return append(out, '\n'), nil
}

func (JSONLCodec) ReadFrame(r io.ReaderAt, off, nextOff int64) ([]byte, int, error) {
	if nextOff <= off {
		return nil, 0, errors.New("jsonl ReadFrame requires nextOff > off")
	}
	length := int(nextOff - off)
	buf := make([]byte, length)
	if _, err := r.ReadAt(buf, off); err != nil {
		return nil, 0, err
	}
	payload, err := decodeJSONLLine(buf)
	if err != nil {
		return nil, 0, err
	}
	return payload, length, nil
}

func (JSONLCodec) ScanFrames(r io.Reader, fn func(off int64, frameLen int) error) error {
	// Wrap with a buffered reader so the byte-at-a-time loop does not
	// fault into a syscall per byte. *os.File does not implement
	// io.ByteReader; bufio.Reader does.
	var reader io.ByteReader
	if br, ok := r.(io.ByteReader); ok {
		reader = br
	} else {
		reader = bufio.NewReaderSize(r, 64<<10)
	}
	off := int64(0)
	var line []byte
	for {
		b, err := reader.ReadByte()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		line = append(line, b)
		if b != '\n' {
			continue
		}
		if _, err := decodeJSONLLine(line); err != nil {
			return nil // torn tail or corruption
		}
		frameLen := len(line)
		if err := fn(off, frameLen); err != nil {
			return err
		}
		off += int64(frameLen)
		line = line[:0]
	}
}

// decodeJSONLLine parses a single JSONL envelope line (with or without
// trailing newline), validates the stored hash against the payload, and
// returns the raw payload JSON.
func decodeJSONLLine(line []byte) ([]byte, error) {
	if n := len(line); n > 0 && line[n-1] == '\n' {
		line = line[:n-1]
	}
	var env jsonlEnvelope
	if err := json.Unmarshal(line, &env); err != nil {
		return nil, ErrCorrupt
	}
	if len(env.Value) == 0 {
		return nil, ErrCorrupt
	}
	got, err := ValueHash(env.Value)
	if err != nil {
		return nil, ErrCorrupt
	}
	if got != env.Hash {
		return nil, ErrCorrupt
	}
	return env.Value, nil
}

