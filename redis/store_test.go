package redis

import (
	"strconv"
	"sync"
	"testing"
)

func TestMakeEntryID_MonotonicAndUnique(t *testing.T) {
	s := &Store{}
	const n = 10_000
	seen := make(map[string]struct{}, n)
	var prev int64
	for i := 0; i < n; i++ {
		id := s.makeEntryID("ns", "k")
		if _, dup := seen[id]; dup {
			t.Fatalf("duplicate id at iteration %d: %q", i, id)
		}
		seen[id] = struct{}{}

		v, err := strconv.ParseInt(id, 10, 64)
		if err != nil {
			t.Fatalf("id %q does not parse as int64: %v", id, err)
		}
		if v <= prev {
			t.Fatalf("id went backwards at iteration %d: prev=%d cur=%d", i, prev, v)
		}
		prev = v
	}
}

func TestMakeEntryID_ConcurrentUnique(t *testing.T) {
	s := &Store{}
	const workers = 16
	const perWorker = 1000
	var mu sync.Mutex
	seen := make(map[string]struct{}, workers*perWorker)

	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			local := make([]string, 0, perWorker)
			for j := 0; j < perWorker; j++ {
				local = append(local, s.makeEntryID("ns", "k"))
			}
			mu.Lock()
			defer mu.Unlock()
			for _, id := range local {
				if _, dup := seen[id]; dup {
					t.Errorf("duplicate id: %q", id)
					return
				}
				seen[id] = struct{}{}
			}
		}()
	}
	wg.Wait()
}

func TestHashKey(t *testing.T) {
	s := &Store{opts: storeOptions{keyPrefix: "cfg"}}
	if got := s.hashKey("ns1"); got != "cfg:ns1" {
		t.Fatalf("expected cfg:ns1, got %q", got)
	}
	if got := s.chanKey(); got != "cfg:changes" {
		t.Fatalf("expected cfg:changes, got %q", got)
	}
}

func TestDefaultOptions(t *testing.T) {
	o := defaultOptions()
	if o.addr != "localhost:6379" {
		t.Errorf("addr: %q", o.addr)
	}
	if o.keyPrefix != "cfg" {
		t.Errorf("keyPrefix: %q", o.keyPrefix)
	}
	if o.watchBufSize != 100 {
		t.Errorf("watchBufSize: %d", o.watchBufSize)
	}
}

func TestOptionFunctions(t *testing.T) {
	o := defaultOptions()
	WithAddress("")(&o) // empty ignored
	if o.addr != "localhost:6379" {
		t.Error("empty addr should be ignored")
	}
	WithAddress("r:6379")(&o)
	if o.addr != "r:6379" {
		t.Errorf("addr: %q", o.addr)
	}
	WithDB(3)(&o)
	if o.db != 3 {
		t.Error("db not set")
	}
	WithKeyPrefix("")(&o)
	if o.keyPrefix != "cfg" {
		t.Error("empty prefix should be ignored")
	}
	WithKeyPrefix("my")(&o)
	if o.keyPrefix != "my" {
		t.Errorf("prefix: %q", o.keyPrefix)
	}
	WithWatchBufferSize(-1)(&o)
	if o.watchBufSize != 100 {
		t.Error("negative bufSize should be ignored")
	}
	WithWatchBufferSize(42)(&o)
	if o.watchBufSize != 42 {
		t.Errorf("bufSize: %d", o.watchBufSize)
	}
}
