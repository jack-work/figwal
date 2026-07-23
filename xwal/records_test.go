package xwal

import (
	"fmt"
	"sync"
	"testing"
)

func TestRecordsFromForkedMidLifeChannel(t *testing.T) {
	dir := t.TempDir()
	x, cfg := triune(t, dir)
	for i := uint64(1); i <= 6; i++ {
		if _, err := x.AppendMain([]byte(fmt.Sprintf(`{"ir":%d}`, i)), nil); err != nil {
			t.Fatal(err)
		}
		if i%2 == 0 {
			if _, err := x.Append("translations", i, []byte(fmt.Sprintf(`{"wire":%d}`, i)), []byte(`"raw-meta"`)); err != nil {
				t.Fatal(err)
			}
		}
	}

	child, err := x.Fork(5, "alt", "continuation")
	if err != nil {
		t.Fatal(err)
	}
	if err := x.Close(); err != nil {
		t.Fatal(err)
	}
	defer child.Close()
	for i := uint64(5); i <= 6; i++ {
		mainLT, err := child.AppendMain([]byte(fmt.Sprintf(`{"alt":%d}`, i)), nil)
		if err != nil {
			t.Fatal(err)
		}
		if mainLT != i {
			t.Fatalf("child main LT = %d, want %d", mainLT, i)
		}
		if _, err := child.Append("translations", i, []byte(fmt.Sprintf(`{"alt-wire":%d}`, i)), []byte(`"raw-meta"`)); err != nil {
			t.Fatal(err)
		}
	}

	records, err := child.RecordsFrom("translations", 3, 0)
	if err != nil {
		t.Fatal(err)
	}
	wantMain := []uint64{4, 5, 6}
	if len(records) != len(wantMain) {
		t.Fatalf("records = %d, want %d", len(records), len(wantMain))
	}
	for i, want := range wantMain {
		if records[i].MainLT != want {
			t.Fatalf("records[%d].MainLT = %d, want %d", i, records[i].MainLT, want)
		}
		if i > 0 && records[i].ChannelLT <= records[i-1].ChannelLT {
			t.Fatalf("records not ascending: %d then %d", records[i-1], records[i])
		}
		if string(records[i].Meta) != `"raw-meta"` {
			t.Fatalf("records[%d].Meta = %q", i, records[i].Meta)
		}
	}
	if string(records[0].Payload) != `{"wire":4}` {
		t.Fatalf("inherited payload = %q", records[0].Payload)
	}
	if string(records[1].Payload) != `{"alt-wire":5}` {
		t.Fatalf("child payload = %q", records[1].Payload)
	}

	limited, err := child.RecordsFrom("translations", 4, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(limited) != 2 || limited[0].MainLT != 4 || limited[1].MainLT != 5 {
		t.Fatalf("limited records = %+v", limited)
	}
	missing, err := child.RecordsFrom("translations", 7, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(missing) != 0 {
		t.Fatalf("records after tail = %+v", missing)
	}
	if _, err := child.RecordsFrom("translations", 1, -1); err == nil {
		t.Fatal("negative limit succeeded")
	}

	if err := child.flushAll(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(dir, cfg, "alt")
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	records, err = reopened.RecordsFrom("translations", 4, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 3 || records[0].MainLT != 4 || records[2].MainLT != 6 {
		t.Fatalf("reopened records = %+v", records)
	}
}

func TestRecordsFromConcurrentAppend(t *testing.T) {
	x, _ := triune(t, t.TempDir())
	defer x.Close()
	for i := uint64(1); i <= 8; i++ {
		if _, err := x.AppendMain([]byte(fmt.Sprintf(`{"ir":%d}`, i)), nil); err != nil {
			t.Fatal(err)
		}
		if _, err := x.Append("translations", i, []byte(fmt.Sprintf(`{"wire":%d}`, i)), nil); err != nil {
			t.Fatal(err)
		}
	}

	errs := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := uint64(9); i <= 48; i++ {
			if _, err := x.AppendMain([]byte(fmt.Sprintf(`{"ir":%d}`, i)), nil); err != nil {
				errs <- err
				return
			}
			if _, err := x.Append("translations", i, []byte(fmt.Sprintf(`{"wire":%d}`, i)), nil); err != nil {
				errs <- err
				return
			}
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 80 {
			records, err := x.RecordsFrom("translations", 1, 0)
			if err != nil {
				errs <- err
				return
			}
			for i := 1; i < len(records); i++ {
				if records[i].MainLT <= records[i-1].MainLT || records[i].ChannelLT <= records[i-1].ChannelLT {
					errs <- fmt.Errorf("records not ordered: %+v then %+v", records[i-1], records[i])
					return
				}
			}
		}
	}()
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}
