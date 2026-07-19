package xwal

import (
	"fmt"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/jack-work/figwal/disk"
	"github.com/jack-work/figwal/segment"
)

const benchSegmentSize = 64 << 10

func benchReducer(_ []byte, patch []byte) ([]byte, error) {
	return append([]byte(nil), patch...), nil
}

func benchConfig() Config {
	return Config{
		Main:        "ir",
		SegmentSize: benchSegmentSize,
		Registry: map[string]Reducer{
			"latest": {Reduce: benchReducer, Initial: []byte("{}")},
		},
		Channels: []ChannelSpec{
			{Name: "ir", Kind: ChannelLog},
			{Name: "translations", Kind: ChannelLog},
			{Name: "state", Kind: ChannelReducible, Reducer: "latest"},
		},
	}
}

func buildLongAria(b *testing.B, records int) (string, Config) {
	b.Helper()
	dir := filepath.Join(b.TempDir(), "aria")
	cfg := benchConfig()
	x, err := Open(dir, cfg)
	if err != nil {
		b.Fatal(err)
	}
	if err := x.Close(); err != nil {
		b.Fatal(err)
	}

	for _, spec := range cfg.Channels {
		opts := disk.Options{
			Codec:       segment.JSONLCodec{},
			SegmentSize: cfg.SegmentSize,
			SyncMode:    disk.SyncManual,
		}
		if spec.Kind == ChannelReducible {
			r := cfg.Registry[spec.Reducer]
			opts.OnSegmentOpen = reducibleFold(r.Reduce, r.Initial)
		}
		l, err := disk.Open(filepath.Join(dir, spec.Name), opts)
		if err != nil {
			b.Fatal(err)
		}
		for i := 1; i <= records; i++ {
			payload := []byte(fmt.Sprintf(`{"kind":"event","seq":%d,"text":"representative agent timeline payload"}`, i))
			if spec.Name == "state" {
				payload = []byte(fmt.Sprintf(`{"turn":%d}`, i))
			}
			if err := l.Write(uint64(i), encodeFrame(uint64(i), payload, []byte(`{"source":"bench"}`))); err != nil {
				b.Fatal(err)
			}
		}
		if err := l.Sync(); err != nil {
			b.Fatal(err)
		}
		if err := l.Close(); err != nil {
			b.Fatal(err)
		}
	}
	return dir, cfg
}

func BenchmarkXWALLongAria(b *testing.B) {
	for _, records := range []int{1_000, 10_000, 50_000} {
		b.Run(strconv.Itoa(records), func(b *testing.B) {
			dir, cfg := buildLongAria(b, records)

			b.Run("Open", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					x, err := Open(dir, cfg)
					if err != nil {
						b.Fatal(err)
					}
					if err := x.Close(); err != nil {
						b.Fatal(err)
					}
				}
			})

			x, err := Open(dir, cfg)
			if err != nil {
				b.Fatal(err)
			}
			defer x.Close()

			b.Run("Read", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					idx := uint64(i%records + 1)
					if _, err := x.ReadAt("ir", idx); err != nil {
						b.Fatal(err)
					}
				}
			})

			b.Run("ReadFromTail256", func(b *testing.B) {
				from := uint64(1)
				if records > 256 {
					from = uint64(records - 255)
				}
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					got, err := x.RecordsFrom("ir", from, 0)
					if err != nil {
						b.Fatal(err)
					}
					if len(got) != records-int(from)+1 {
						b.Fatalf("read %d records, want %d", len(got), records-int(from)+1)
					}
				}
			})

			b.Run("ScanFromEndTail256", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					seen := 0
					err := x.chans["ir"].log.ScanFromEnd(uint64(records), func(uint64, []byte) error {
						seen++
						if seen == 256 {
							return errStopRange
						}
						return nil
					})
					if err != errStopRange {
						b.Fatal(err)
					}
				}
			})

			b.Run("LookupCold", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					cold, err := Open(dir, cfg)
					if err != nil {
						b.Fatal(err)
					}
					if _, ok, err := cold.Lookup("translations", uint64(records)); err != nil || !ok {
						b.Fatalf("lookup: ok=%v err=%v", ok, err)
					}
					if err := cold.Close(); err != nil {
						b.Fatal(err)
					}
				}
			})

			b.Run("LookupColdFirst", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					cold, err := Open(dir, cfg)
					if err != nil {
						b.Fatal(err)
					}
					if _, ok, err := cold.Lookup("translations", 1); err != nil || !ok {
						b.Fatalf("lookup: ok=%v err=%v", ok, err)
					}
					if err := cold.Close(); err != nil {
						b.Fatal(err)
					}
				}
			})

			if _, ok, err := x.Lookup("translations", uint64(records)); err != nil || !ok {
				b.Fatalf("prime lookup: ok=%v err=%v", ok, err)
			}
			b.Run("LookupHot", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					if _, ok, err := x.Lookup("translations", uint64(i%records+1)); err != nil || !ok {
						b.Fatalf("lookup: ok=%v err=%v", ok, err)
					}
				}
			})

			b.Run("StateAt", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					if _, err := x.StateAt("state", uint64(records)); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

func buildDeepAria(b *testing.B, records, depth int) (string, Config, []string, uint64) {
	b.Helper()
	dir, cfg := buildLongAria(b, records)
	x, err := Open(dir, cfg)
	if err != nil {
		b.Fatal(err)
	}
	branch := make([]string, 0, depth)
	tail := uint64(records)
	for i := 0; i < depth; i++ {
		childName := fmt.Sprintf("branch-%02d", i)
		child, err := x.Fork(tail+1, childName, fmt.Sprintf("continuation-%02d", i))
		if err != nil {
			b.Fatal(err)
		}
		if err := x.Close(); err != nil {
			b.Fatal(err)
		}
		x = child
		branch = append(branch, childName)
		tail, err = x.AppendMain([]byte(`{"kind":"depth"}`), nil)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := x.Append("translations", tail, []byte(`{"wire":"depth"}`), nil); err != nil {
			b.Fatal(err)
		}
		if _, err := x.Append("state", tail, []byte(fmt.Sprintf(`{"depth":%d}`, i)), nil); err != nil {
			b.Fatal(err)
		}
	}
	if err := x.Close(); err != nil {
		b.Fatal(err)
	}
	return dir, cfg, branch, tail
}

func BenchmarkXWALDeepAria(b *testing.B) {
	for _, records := range []int{1_000, 10_000, 50_000} {
		b.Run(strconv.Itoa(records), func(b *testing.B) {
			dir, cfg, branch, tail := buildDeepAria(b, records, 16)

			b.Run("OpenDepth16", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					x, err := Open(dir, cfg, branch...)
					if err != nil {
						b.Fatal(err)
					}
					if err := x.Close(); err != nil {
						b.Fatal(err)
					}
				}
			})

			x, err := Open(dir, cfg, branch...)
			if err != nil {
				b.Fatal(err)
			}
			defer x.Close()

			b.Run("ReadRoot", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					if _, err := x.ReadAt("ir", 1); err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run("ReadTip", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					if _, err := x.ReadAt("ir", tail); err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run("ReadFromTip16", func(b *testing.B) {
				from := tail - 15
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					got, err := x.RecordsFrom("ir", from, 0)
					if err != nil {
						b.Fatal(err)
					}
					if len(got) != 16 {
						b.Fatalf("read %d records, want 16", len(got))
					}
				}
			})
		})
	}
}

func BenchmarkXWALForkLongAria(b *testing.B) {
	for _, records := range []int{1_000, 10_000, 50_000} {
		b.Run(strconv.Itoa(records), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				dir, cfg := buildLongAria(b, records)
				x, err := Open(dir, cfg)
				if err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
				child, err := x.Fork(uint64(records+1), "alternative", "continuation")
				b.StopTimer()
				if err != nil {
					b.Fatal(err)
				}
				if err := child.Close(); err != nil {
					b.Fatal(err)
				}
				if err := x.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func buildLongTrunk(b *testing.B, records int) (*Trunks, Config, TrunkID, uint64) {
	b.Helper()
	dir := filepath.Join(b.TempDir(), "forest")
	cfg := benchConfig()
	trunks, err := CreateTrunks(dir, cfg)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := trunks.Close(); err != nil {
			b.Error(err)
		}
	})
	trunk, err := trunks.SpawnUnderRoot()
	if err != nil {
		b.Fatal(err)
	}
	branch, err := trunks.headBranch(trunk)
	if err != nil {
		b.Fatal(err)
	}
	for _, spec := range cfg.Channels {
		opts := disk.Options{
			Codec:       segment.JSONLCodec{},
			SegmentSize: cfg.SegmentSize,
			SyncMode:    disk.SyncManual,
		}
		if spec.Kind == ChannelReducible {
			r := cfg.Registry[spec.Reducer]
			opts.OnSegmentOpen = reducibleFold(r.Reduce, r.Initial)
		}
		chDir := filepath.Join(append([]string{dir, spec.Name}, branch...)...)
		if !pathExists(chDir) {
			chDir = filepath.Join(dir, spec.Name)
		}
		l, err := disk.Open(chDir, opts)
		if err != nil {
			b.Fatal(err)
		}
		for i := 0; i < records; i++ {
			idx := uint64(i + 1)
			if l.ForkBase() > 0 {
				idx++
			}
			mainLT := uint64(i + 2)
			payload := []byte(fmt.Sprintf(`{"kind":"event","seq":%d,"text":"representative agent timeline payload"}`, i+1))
			if spec.Name == "state" {
				payload = []byte(fmt.Sprintf(`{"turn":%d}`, i+1))
			}
			if spec.Name == "ir" {
				mainLT = idx
			}
			if err := l.Write(idx, encodeFrame(mainLT, payload, []byte(`{"source":"bench"}`))); err != nil {
				b.Fatal(err)
			}
		}
		if err := l.Sync(); err != nil {
			b.Fatal(err)
		}
		if err := l.Close(); err != nil {
			b.Fatal(err)
		}
	}
	return trunks, cfg, trunk, uint64(records + 1)
}

func BenchmarkTrunksLongAria(b *testing.B) {
	for _, records := range []int{1_000, 10_000, 50_000} {
		b.Run(strconv.Itoa(records), func(b *testing.B) {
			trunks, _, trunk, tail := buildLongTrunk(b, records)

			b.Run("HeadOpenReadTip", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					x, err := trunks.Head(trunk)
					if err != nil {
						b.Fatal(err)
					}
					if _, err := x.ReadAt("ir", tail); err != nil {
						b.Fatal(err)
					}
					if err := x.Close(); err != nil {
						b.Fatal(err)
					}
				}
			})

			b.Run("List", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					if got := trunks.List(); len(got) != 1 || got[0].Tip != tail {
						b.Fatalf("List = %+v", got)
					}
				}
			})

			b.Run("ListLight", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					if got := trunks.ListLight(); len(got) != 1 {
						b.Fatalf("ListLight = %+v", got)
					}
				}
			})

			other, err := trunks.ForkTail(trunk)
			if err != nil {
				b.Fatal(err)
			}
			b.Run("Append", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					if _, _, err := trunks.Append(trunk, 0, []byte(`{"kind":"event"}`), nil); err != nil {
						b.Fatal(err)
					}
				}
			})

			b.Run("AppendTwoTrunks", func(b *testing.B) {
				var next atomic.Uint64
				b.ReportAllocs()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						target := trunk
						if next.Add(1)&1 == 0 {
							target = other
						}
						if _, _, err := trunks.Append(target, 0, []byte(`{"kind":"event"}`), nil); err != nil {
							b.Error(err)
							return
						}
					}
				})
			})
		})
	}
}
