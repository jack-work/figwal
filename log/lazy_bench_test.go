package log

import (
	"fmt"
	"testing"
)

// What a read that can MISS costs. The cache snapshot copies every payload
// into RAM at Open; the disk layer keeps only per-record offsets and reads
// by pread. Both paths exist today, over the same bytes, so the price of
// going lazy is measurable before any of it is built.

func benchFixture(b *testing.B, n int, size int) *Log {
	b.Helper()
	dir := b.TempDir()
	l, err := Open(dir, Options{SegmentSize: 1 << 20})
	if err != nil {
		b.Fatal(err)
	}
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte(i)
	}
	for i := 1; i <= n; i++ {
		if err := l.write(uint64(i), payload); err != nil {
			b.Fatal(err)
		}
	}
	if err := l.Sync(); err != nil {
		b.Fatal(err)
	}
	return l
}

func BenchmarkReplayCachedVsDisk(b *testing.B) {
	for _, n := range []int{100, 2000} {
		for _, size := range []int{200, 4096} {
			l := benchFixture(b, n, size)
			b.Run(fmt.Sprintf("cached/n=%d/size=%d", n, size), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					if err := l.Range(1, func(uint64, []byte) error { return nil }); err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run(fmt.Sprintf("disk/n=%d/size=%d", n, size), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					if err := l.RangeOwn(0, func(uint64, []byte) error { return nil }); err != nil {
						b.Fatal(err)
					}
				}
			})
			l.Close()
		}
	}
}

func BenchmarkPointReadCachedVsDisk(b *testing.B) {
	l := benchFixture(b, 2000, 4096)
	defer l.Close()
	b.Run("cached", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := l.Read(uint64(i%2000) + 1); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("disk", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := l.inner.Read(uint64(i%2000) + 1); err != nil {
				b.Fatal(err)
			}
		}
	})
}
