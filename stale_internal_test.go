package cache

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// fakeClock is a controllable time source for deterministic tests.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) time() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// fakeRedis is an in-memory rediser. Get can be made to block until a test
// releases it, which makes refresh races deterministic.
type fakeRedis struct {
	// 64-bit fields accessed atomically must come first so they are
	// 8-byte aligned on 32-bit platforms (see sync/atomic docs).
	getCalls int64
	setCalls int64

	mu     sync.Mutex
	store  map[string]string
	getErr error // forced error for every Get
	setErr error // forced error for every Set

	blockMu   sync.Mutex
	blockGet  chan struct{}
	insideGet chan struct{}
}

func newFakeRedis() *fakeRedis {
	return &fakeRedis{
		store: make(map[string]string),
	}
}

func (r *fakeRedis) block(insideGet chan struct{}) {
	r.blockMu.Lock()
	defer r.blockMu.Unlock()
	r.blockGet = make(chan struct{})
	r.insideGet = insideGet
}

func (r *fakeRedis) release() {
	r.blockMu.Lock()
	defer r.blockMu.Unlock()
	if r.blockGet != nil {
		close(r.blockGet)
		r.blockGet = nil
	}
}

func (r *fakeRedis) gate() (gate, inside chan struct{}) {
	r.blockMu.Lock()
	defer r.blockMu.Unlock()
	return r.blockGet, r.insideGet
}

func (r *fakeRedis) Get(ctx context.Context, key string) *redis.StringCmd {
	atomic.AddInt64(&r.getCalls, 1)
	if blockGet, insideGet := r.gate(); blockGet != nil {
		if insideGet != nil {
			select {
			case <-insideGet:
			default:
				close(insideGet)
			}
		}
		select {
		case <-blockGet:
		case <-ctx.Done():
			return redis.NewStringResult("", ctx.Err())
		}
	}
	if r.getErr != nil {
		return redis.NewStringResult("", r.getErr)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	val, ok := r.store[key]
	if !ok {
		return redis.NewStringResult("", redis.Nil)
	}
	return redis.NewStringResult(val, nil)
}

func (r *fakeRedis) Set(ctx context.Context, key string, value interface{}, ttl time.Duration) *redis.StatusCmd {
	atomic.AddInt64(&r.setCalls, 1)
	if r.setErr != nil {
		return redis.NewStatusResult("", r.setErr)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.store[key] = string(value.([]byte))
	return redis.NewStatusResult("OK", nil)
}

func (r *fakeRedis) SetXX(ctx context.Context, key string, value interface{}, ttl time.Duration) *redis.BoolCmd {
	return redis.NewBoolResult(false, nil)
}

func (r *fakeRedis) SetNX(ctx context.Context, key string, value interface{}, ttl time.Duration) *redis.BoolCmd {
	return redis.NewBoolResult(false, nil)
}

func (r *fakeRedis) Del(ctx context.Context, keys ...string) *redis.IntCmd {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := int64(0)
	for _, key := range keys {
		if _, ok := r.store[key]; ok {
			delete(r.store, key)
			n++
		}
	}
	return redis.NewIntResult(n, nil)
}

func (r *fakeRedis) seeded(key string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.store[key]
	return ok
}

func newTestCache(rdb rediser) (*Cache, *fakeClock) {
	cd := New(&Options{Redis: rdb})
	clock := newFakeClock()
	cd.stale.now = clock.time
	return cd, clock
}

func staleItem(key string, do func(*StaleItem) (interface{}, error)) *StaleItem {
	return &StaleItem{
		Ctx:   context.Background(),
		Key:   key,
		Value: new(string),
		Fresh: time.Minute,
		Stale: 5 * time.Minute,
		Do:    do,
	}
}

// gatedDo returns a Do callback that blocks until release is closed, signaling
// entry through inside. Holding Do keeps a refresh in flight deterministically
// even when the Redis read itself misses instantly.
func gatedDo(inside, release chan struct{}, value interface{}) func(*StaleItem) (interface{}, error) {
	return func(*StaleItem) (interface{}, error) {
		close(inside)
		<-release
		return value, nil
	}
}

func mustGetStale(t *testing.T, cd *Cache, item *StaleItem) *StaleResult {
	t.Helper()
	res, err := cd.GetStale(item)
	if err != nil {
		t.Fatalf("GetStale failed: %v", err)
	}
	return res
}

// waitInside blocks until a refresh reaches the fake Redis Get gate.
func waitInside(t *testing.T, inside <-chan struct{}) {
	t.Helper()
	select {
	case <-inside:
	case <-time.After(2 * time.Second):
		t.Fatal("refresh never reached Redis")
	}
}

func waitForIdle(t *testing.T, cd *Cache) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		cd.stale.mu.Lock()
		busy := len(cd.stale.calls) > 0
		cd.stale.mu.Unlock()
		if !busy {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("refresh did not finish in time")
}

func TestGetStale_FreshServedWithoutRedis(t *testing.T) {
	rdb := newFakeRedis()
	cd, clock := newTestCache(rdb)

	var doCalls int64
	item := staleItem("k", func(*StaleItem) (interface{}, error) {
		atomic.AddInt64(&doCalls, 1)
		return "v1", nil
	})

	res := mustGetStale(t, cd, item)
	if res.Source != Refresh {
		t.Fatalf("first call source = %s, want refresh", res.Source)
	}
	if atomic.LoadInt64(&rdb.getCalls) != 1 {
		t.Fatalf("redis get calls = %d, want 1", rdb.getCalls)
	}

	clock.advance(30 * time.Second) // still fresh
	res = mustGetStale(t, cd, item)
	if res.Source != Fresh {
		t.Fatalf("second call source = %s, want fresh", res.Source)
	}
	if got := atomic.LoadInt64(&rdb.getCalls); got != 1 {
		t.Fatalf("redis get calls = %d, want still 1", got)
	}
	if atomic.LoadInt64(&doCalls) != 1 {
		t.Fatalf("Do called %d times, want 1", doCalls)
	}
	if *item.Value.(*string) != "v1" {
		t.Fatalf("value = %q, want v1", *item.Value.(*string))
	}
}

func TestGetStale_StaleWindowServesOldValueWithSingleRefresh(t *testing.T) {
	rdb := newFakeRedis()
	cd, clock := newTestCache(rdb)

	var doCalls int64
	seed := staleItem("k", func(*StaleItem) (interface{}, error) {
		atomic.AddInt64(&doCalls, 1)
		return "v1", nil
	})
	mustGetStale(t, cd, seed) // cold load -> v1
	firstDo := atomic.LoadInt64(&doCalls)
	// Force the refresh down the Do path instead of reading the value that
	// the cold load just wrote back to Redis.
	delete(rdb.store, "k")

	clock.advance(2 * time.Minute) // past fresh (1m), inside stale (6m absolute)

	// Hold the refresh inside Do so the in-flight window is deterministic.
	insideDo := make(chan struct{})
	releaseDo := make(chan struct{})
	item := staleItem("k", func(i *StaleItem) (interface{}, error) {
		atomic.AddInt64(&doCalls, 1)
		return gatedDo(insideDo, releaseDo, "v2")(i)
	})

	res := mustGetStale(t, cd, item)
	if res.Source != Stale {
		t.Fatalf("source = %s, want stale", res.Source)
	}
	if *item.Value.(*string) != "v1" {
		t.Fatalf("stale value = %q, want v1", *item.Value.(*string))
	}
	select {
	case <-insideDo:
	case <-time.After(2 * time.Second):
		t.Fatal("refresh Do never started")
	}

	// Concurrent callers must not pile onto Redis/Do while the single shared
	// refresh is in flight: they all see the old value.
	for i := 0; i < 10; i++ {
		other := staleItem("k", func(*StaleItem) (interface{}, error) {
			atomic.AddInt64(&doCalls, 1)
			return "v2", nil
		})
		res := mustGetStale(t, cd, other)
		if res.Source != Stale {
			t.Fatalf("concurrent source = %s, want stale", res.Source)
		}
		if *other.Value.(*string) != "v1" {
			t.Fatalf("concurrent stale value = %q, want v1", *other.Value.(*string))
		}
	}

	close(releaseDo)
	waitForIdle(t, cd)

	if got := atomic.LoadInt64(&doCalls); got != firstDo+1 {
		t.Fatalf("Do called %d times total, want %d (one shared refresh)", got, firstDo+1)
	}
	if got := atomic.LoadInt64(&rdb.getCalls); got != 2 {
		t.Fatalf("redis get calls = %d, want 2 (cold load + one refresh)", got)
	}

	clock.advance(time.Second)
	res = mustGetStale(t, cd, item)
	if res.Source != Fresh {
		t.Fatalf("post-refresh source = %s, want fresh", res.Source)
	}
	if *item.Value.(*string) != "v2" {
		t.Fatalf("refreshed value = %q, want v2", *item.Value.(*string))
	}
}

func TestGetStale_AbsoluteExpiryRejectsOldValue(t *testing.T) {
	rdb := newFakeRedis()
	cd, clock := newTestCache(rdb)

	var doCalls int64
	item := staleItem("k", func(*StaleItem) (interface{}, error) {
		n := atomic.AddInt64(&doCalls, 1)
		if n == 1 {
			return "v1", nil
		}
		return "v2", nil
	})

	mustGetStale(t, cd, item)
	// The refresh must run Do, not read the write-back value from Redis.
	delete(rdb.store, "k")

	// Advance past the absolute deadline (1m + 5m) before calling, so the
	// stale entry is provably unusable at the moment of the read.
	clock.advance(7 * time.Minute)
	res := mustGetStale(t, cd, item)
	if res.Source != Refresh {
		t.Fatalf("source = %s, want refresh", res.Source)
	}
	if *item.Value.(*string) != "v2" {
		t.Fatalf("value = %q, want v2, old value must not be served", *item.Value.(*string))
	}
	if atomic.LoadInt64(&doCalls) != 2 {
		t.Fatalf("Do called %d times, want 2", doCalls)
	}
}

func TestGetStale_RedisErrorServesStaleUntilAbsoluteDeadline(t *testing.T) {
	rdb := newFakeRedis()
	cd, clock := newTestCache(rdb)
	redisDown := errors.New("redis is down")

	var doCalls int64
	item := staleItem("k", func(*StaleItem) (interface{}, error) {
		n := atomic.AddInt64(&doCalls, 1)
		return "v" + string(rune('0'+n)), nil
	})
	mustGetStale(t, cd, item) // v1
	clock.advance(2 * time.Minute)

	// Redis goes down (reads and writes fail). With Do available, the
	// refresh succeeds via Do; the Redis write-back failure must not roll
	// the local value back.
	rdb.getErr = redisDown
	rdb.setErr = redisDown
	res := mustGetStale(t, cd, item)
	if res.Source != Stale {
		t.Fatalf("source = %s, want stale", res.Source)
	}
	if *item.Value.(*string) != "v1" {
		t.Fatalf("stale value = %q, want v1", *item.Value.(*string))
	}

	waitForIdle(t, cd)
	clock.advance(time.Second)
	res = mustGetStale(t, cd, item)
	if res.Source != Fresh {
		t.Fatalf("source after Do refresh = %s, want fresh", res.Source)
	}
	if *item.Value.(*string) != "v2" {
		t.Fatalf("value = %q, want v2", *item.Value.(*string))
	}

	// Remove Do and push past the absolute deadline: with Redis still down
	// and no stale value left, the Redis error must surface.
	item.Do = nil
	clock.advance(6 * time.Minute)
	_, err := cd.GetStale(item)
	if !errors.Is(err, redisDown) {
		t.Fatalf("err = %v, want redis error after absolute expiry", err)
	}
}

func TestGetStale_RedisErrorWithoutDoAtAbsoluteDeadline(t *testing.T) {
	rdb := newFakeRedis()
	cd, _ := newTestCache(rdb)
	rdb.getErr = errors.New("boom")

	item := staleItem("k", nil)
	// A cold key with Redis down and no Do returns the Redis error directly.
	if _, err := cd.GetStale(item); !errors.Is(err, rdb.getErr) {
		t.Fatalf("cold err = %v, want boom", err)
	}
}

func TestGetStale_CancellingRequestDoesNotCancelSharedRefresh(t *testing.T) {
	rdb := newFakeRedis()
	cd, _ := newTestCache(rdb)

	insideGet := make(chan struct{})
	rdb.block(insideGet)

	started := make(chan struct{})
	var doCalls int64
	item := staleItem("k", func(*StaleItem) (interface{}, error) {
		close(started)
		atomic.AddInt64(&doCalls, 1)
		return "v1", nil
	})
	// The originating request is cancellable and gets cancelled while the
	// refresh is in flight.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	item.Ctx = ctx

	done := make(chan error, 1)
	go func() {
		_, err := cd.GetStale(item)
		done <- err
	}()

	waitInside(t, insideGet)

	// Give up on the request mid-flight. The foreground caller observes the
	// cancellation, but the shared refresh keeps running detached.
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("GetStale err = %v, want context.Canceled for the caller", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled caller did not return")
	}

	// Releasing the blocked Redis call lets the detached refresh continue:
	// the Do call must still run and install the value.
	rdb.release()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("Do was cancelled by the request cancellation")
	}
	waitForIdle(t, cd)

	// A later caller reaps the value the detached refresh installed.
	fresh := mustGetStale(t, cd, staleItem("k", nil))
	if fresh.Source != Fresh {
		t.Fatalf("source = %s, want fresh", fresh.Source)
	}
	if atomic.LoadInt64(&doCalls) != 1 {
		t.Fatalf("Do called %d times, want 1", doCalls)
	}
}

func TestGetStale_ForegroundWaiterCanCancel(t *testing.T) {
	rdb := newFakeRedis()
	cd, clock := newTestCache(rdb)

	var doCalls int64
	seed := staleItem("k", func(*StaleItem) (interface{}, error) {
		atomic.AddInt64(&doCalls, 1)
		return "v1", nil
	})
	mustGetStale(t, cd, seed)
	delete(rdb.store, "k")          // force the post-expiry refresh down the Do path
	clock.advance(10 * time.Minute) // absolute expired: no stale value to serve

	insideDo := make(chan struct{})
	releaseDo := make(chan struct{})

	ctx, cancel := context.WithCancel(context.Background())
	waiter := staleItem("k", func(i *StaleItem) (interface{}, error) {
		atomic.AddInt64(&doCalls, 1)
		return gatedDo(insideDo, releaseDo, "v2")(i)
	})
	waiter.Ctx = ctx

	errCh := make(chan error, 1)
	go func() {
		_, err := cd.GetStale(waiter)
		errCh <- err
	}()
	select {
	case <-insideDo:
	case <-time.After(2 * time.Second):
		t.Fatal("refresh Do never started")
	}

	cancel()
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("waiter err = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waiter was not unblocked by cancellation")
	}

	// The shared refresh itself keeps running and installs v2.
	close(releaseDo)
	waitForIdle(t, cd)
	clock.advance(time.Second)
	got := mustGetStale(t, cd, staleItem("k", nil))
	if got.Source != Fresh {
		t.Fatalf("source = %s, want fresh", got.Source)
	}
	// One Do for the original seed plus one for the post-expiry refresh.
	if atomic.LoadInt64(&doCalls) != 2 {
		t.Fatalf("Do calls = %d, want 2", doCalls)
	}
}

func TestGetStale_DeleteBumpsVersionAndBlocksResurrection(t *testing.T) {
	rdb := newFakeRedis()
	cd, clock := newTestCache(rdb)

	item := staleItem("k", func(*StaleItem) (interface{}, error) {
		return "v1", nil
	})
	mustGetStale(t, cd, item)
	clock.advance(2 * time.Minute) // stale

	insideGet := make(chan struct{})
	rdb.block(insideGet)

	// Start a refresh that gets stuck inside Redis.
	staleRes := mustGetStale(t, cd, item)
	if staleRes.Source != Stale {
		t.Fatalf("source = %s, want stale", staleRes.Source)
	}
	waitInside(t, insideGet)

	// Delete while the refresh is in flight. Delete's own Redis call must
	// not go through the Get gate (it uses Del).
	if err := cd.Delete(context.Background(), "k"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if cd.stale.get("k") != nil {
		t.Fatal("stale entry still present after Delete")
	}

	rdb.release()
	waitForIdle(t, cd)

	// The in-flight refresh must not have resurrected the deleted key.
	if cd.stale.get("k") != nil {
		t.Fatal("stale entry resurrected after Delete")
	}
	if rdb.seeded("k") {
		t.Fatal("redis key resurrected after Delete")
	}

	// A subsequent call starts from scratch and reports a refresh.
	res := mustGetStale(t, cd, staleItem("k", func(*StaleItem) (interface{}, error) {
		return "v2", nil
	}))
	if res.Source != Refresh {
		t.Fatalf("source = %s, want refresh", res.Source)
	}
}

func TestGetStale_OlderLateResultCannotOverwriteNewerValue(t *testing.T) {
	rdb := newFakeRedis()
	cd, clock := newTestCache(rdb)

	var doCalls int64
	first := staleItem("k", func(*StaleItem) (interface{}, error) {
		atomic.AddInt64(&doCalls, 1)
		return "v1", nil
	})
	mustGetStale(t, cd, first)
	clock.advance(2 * time.Minute) // stale

	insideGet := make(chan struct{})
	rdb.block(insideGet)

	// Slow refresh A starts for the stale entry...
	slow := staleItem("k", func(*StaleItem) (interface{}, error) {
		atomic.AddInt64(&doCalls, 1)
		return "slow", nil
	})
	staleRes := mustGetStale(t, cd, slow)
	if staleRes.Source != Stale {
		t.Fatalf("source = %s, want stale", staleRes.Source)
	}
	waitInside(t, insideGet)

	// While A is stuck, Delete bumps the version.
	if err := cd.Delete(context.Background(), "k"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	rdb.release()
	waitForIdle(t, cd)

	// A's late commit must have been discarded.
	if rdb.seeded("k") {
		t.Fatal("slow refresh overwrote the deleted/newer state")
	}
	if cd.stale.get("k") != nil {
		t.Fatal("stale entry should be absent after the discarded late commit")
	}

	// A fresh Do installs the newer value normally.
	next := staleItem("k", func(*StaleItem) (interface{}, error) {
		atomic.AddInt64(&doCalls, 1)
		return "v2", nil
	})
	res := mustGetStale(t, cd, next)
	if res.Source != Refresh {
		t.Fatalf("source = %s, want refresh", res.Source)
	}
	if *next.Value.(*string) != "v2" {
		t.Fatalf("value = %q, want v2", *next.Value.(*string))
	}
}

func TestGetStale_MarshalErrorDoesNotReplaceLocalState(t *testing.T) {
	rdb := newFakeRedis()
	cd, clock := newTestCache(rdb)

	marshalErr := errors.New("marshal boom")
	cd.marshal = func(interface{}) ([]byte, error) {
		return nil, marshalErr
	}

	item := staleItem("k", func(*StaleItem) (interface{}, error) {
		return "v1", nil
	})
	if _, err := cd.GetStale(item); !errors.Is(err, marshalErr) {
		t.Fatalf("err = %v, want marshal error", err)
	}
	if cd.stale.get("k") != nil {
		t.Fatal("entry must not be stored when marshaling fails")
	}

	// Restoring marshal still works: the failed attempt left no state.
	cd.marshal = cd._marshal
	clock.advance(time.Second)
	res := mustGetStale(t, cd, item)
	if res.Source != Refresh || *item.Value.(*string) != "v1" {
		t.Fatalf("res = %+v value = %q", res, *item.Value.(*string))
	}
}

func TestGetStale_CorruptEntryEvictedAndRefreshedOnce(t *testing.T) {
	rdb := newFakeRedis()
	cd, clock := newTestCache(rdb)

	type obj struct {
		Name string
	}
	item := &StaleItem{
		Key:   "k",
		Value: new(obj),
		Fresh: time.Minute,
		Stale: 5 * time.Minute,
		Do: func(*StaleItem) (interface{}, error) {
			return &obj{Name: "v1"}, nil
		},
	}
	mustGetStale(t, cd, item)
	delete(rdb.store, "k")         // force the self-healing refresh down the Do path
	clock.advance(2 * time.Minute) // stale, but still inside the absolute window

	// Corrupt the stored bytes so msgpack decoding genuinely fails for the
	// struct destination (0xc1 is a never-valid msgpack type byte).
	entry := cd.stale.get("k")
	entry.data = []byte{0xc1, 0xc1, 0xc1, 0x01}

	insideDo := make(chan struct{})
	releaseDo := make(chan struct{})
	var doCalls int64
	item.Do = func(i *StaleItem) (interface{}, error) {
		atomic.AddInt64(&doCalls, 1)
		return gatedDo(insideDo, releaseDo, &obj{Name: "v2"})(i)
	}

	errCh := make(chan error, 1)
	resCh := make(chan *StaleResult, 1)
	go func() {
		res, err := cd.GetStale(item)
		errCh <- err
		resCh <- res
	}()
	select {
	case <-insideDo:
	case <-time.After(2 * time.Second):
		t.Fatal("refresh Do never started")
	}
	close(releaseDo)

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("GetStale err = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("GetStale did not return after refresh")
	}
	res := <-resCh
	if res.Source != Refresh {
		t.Fatalf("source = %s, want refresh", res.Source)
	}
	if item.Value.(*obj).Name != "v2" {
		t.Fatalf("value = %q, want v2", item.Value.(*obj).Name)
	}
	if atomic.LoadInt64(&doCalls) != 1 {
		t.Fatalf("Do called %d times, want 1", doCalls)
	}
}

func TestGetStale_LocalCacheOnlyServesStale(t *testing.T) {
	cd := New(&Options{LocalCache: NewTinyLFU(10, time.Hour)})
	clock := newFakeClock()
	cd.stale.now = clock.time

	var doCalls int64
	item := staleItem("k", func(*StaleItem) (interface{}, error) {
		atomic.AddInt64(&doCalls, 1)
		return "v1", nil
	})
	res := mustGetStale(t, cd, item)
	if res.Source != Refresh {
		t.Fatalf("source = %s, want refresh", res.Source)
	}

	clock.advance(2 * time.Minute) // stale, absolute deadline at 6m
	res = mustGetStale(t, cd, item)
	if res.Source != Stale {
		t.Fatalf("source = %s, want stale", res.Source)
	}
	waitForIdle(t, cd)
	// The background refresh reset storedAt to the frozen "now"; move on so
	// the refreshed entry is fresh, then advance past its absolute deadline.
	clock.advance(time.Second)
	if got := mustGetStale(t, cd, item); got.Source != Fresh {
		t.Fatalf("post-refresh source = %s, want fresh", got.Source)
	}
	clock.advance(6 * time.Minute)
	res = mustGetStale(t, cd, item)
	if res.Source != Refresh {
		t.Fatalf("source = %s, want refresh", res.Source)
	}
	if atomic.LoadInt64(&doCalls) != 3 {
		t.Fatalf("Do called %d times, want 3", doCalls)
	}
}

func TestGetStale_InvalidConfigAndArguments(t *testing.T) {
	cd := New(&Options{})
	item := staleItem("k", nil)
	if _, err := cd.GetStale(item); !errors.Is(err, errRedisLocalCacheNil) {
		t.Fatalf("no-backend err = %v, want errRedisLocalCacheNil", err)
	}

	cd, _ = newTestCache(newFakeRedis())
	item.Fresh = 0
	if _, err := cd.GetStale(item); err == nil {
		t.Fatal("Fresh=0 must be rejected")
	}
	item.Fresh = -time.Second
	if _, err := cd.GetStale(item); err == nil {
		t.Fatal("negative Fresh must be rejected")
	}
}

func TestGetStale_RedisHitSkipsDoAndResetsWindows(t *testing.T) {
	rdb := newFakeRedis()
	cd, clock := newTestCache(rdb)

	// Seed Redis directly with marshaled bytes.
	b, err := cd.Marshal("from-redis")
	if err != nil {
		t.Fatal(err)
	}
	rdb.store["k"] = string(b)

	var doCalls int64
	item := staleItem("k", func(*StaleItem) (interface{}, error) {
		atomic.AddInt64(&doCalls, 1)
		return "from-do", nil
	})
	res := mustGetStale(t, cd, item)
	if res.Source != Refresh {
		t.Fatalf("source = %s, want refresh", res.Source)
	}
	if *item.Value.(*string) != "from-redis" {
		t.Fatalf("value = %q, want from-redis", *item.Value.(*string))
	}
	if atomic.LoadInt64(&doCalls) != 0 {
		t.Fatal("Do must not run when Redis has the value")
	}

	clock.advance(30 * time.Second) // fresh again, no Redis contact
	res = mustGetStale(t, cd, item)
	if res.Source != Fresh {
		t.Fatalf("source = %s, want fresh", res.Source)
	}
	if got := atomic.LoadInt64(&rdb.getCalls); got != 1 {
		t.Fatalf("redis get calls = %d, want 1", got)
	}
}

func TestGetStale_ColdLoadIsSingleFlight(t *testing.T) {
	rdb := newFakeRedis()
	cd, _ := newTestCache(rdb)

	// Gate on Do: every cold refresh misses Redis instantly and converges on
	// the single shared Do call, which is what must not fan out.
	insideDo := make(chan struct{})
	releaseDo := make(chan struct{})
	doOnce := &sync.Once{}

	// Barrier so every goroutine is inside GetStale before any of them can
	// win the leader race; this removes scheduler-dependent flakiness.
	var started sync.WaitGroup
	started.Add(20)

	var doCalls int64
	const n = 20
	var wg sync.WaitGroup
	errs := make(chan error, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			item := staleItem("k", func(i *StaleItem) (interface{}, error) {
				atomic.AddInt64(&doCalls, 1)
				doOnce.Do(func() { close(insideDo) })
				<-releaseDo
				return "v1", nil
			})
			started.Done()
			started.Wait()
			_, err := cd.GetStale(item)
			errs <- err
		}()
	}

	started.Wait()
	select {
	case <-insideDo:
	case <-time.After(2 * time.Second):
		t.Fatal("cold refresh Do never started")
	}
	close(releaseDo)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("GetStale err = %v", err)
		}
	}
	if atomic.LoadInt64(&rdb.getCalls) != 1 {
		t.Fatalf("redis get calls = %d, want 1", rdb.getCalls)
	}
	if atomic.LoadInt64(&doCalls) != 1 {
		t.Fatalf("Do calls = %d, want 1", doCalls)
	}
}

func TestGetStale_ZeroStaleMeansNoOldValue(t *testing.T) {
	rdb := newFakeRedis()
	cd, clock := newTestCache(rdb)

	insideGet := make(chan struct{})
	rdb.block(insideGet)

	var doCalls int64
	item := &StaleItem{
		Key:   "k",
		Value: new(string),
		Fresh: time.Minute,
		Stale: 0,
		Do: func(*StaleItem) (interface{}, error) {
			n := atomic.AddInt64(&doCalls, 1)
			return "v" + string(rune('0'+n)), nil
		},
	}

	// Cold load with a deterministic in-flight window.
	done := make(chan struct{})
	go func() {
		mustGetStale(t, cd, item)
		close(done)
	}()
	waitInside(t, insideGet)
	rdb.release()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("cold load did not finish")
	}
	delete(rdb.store, "k") // force the post-expiry refresh down the Do path

	clock.advance(2 * time.Minute) // fresh gone; with Stale=0 it is absolute-expired
	res := mustGetStale(t, cd, item)
	if res.Source != Refresh {
		t.Fatalf("source = %s, want refresh", res.Source)
	}
	if *item.Value.(*string) != "v2" {
		t.Fatalf("value = %q, want v2", *item.Value.(*string))
	}
	if atomic.LoadInt64(&doCalls) != 2 {
		t.Fatalf("Do calls = %d, want 2", doCalls)
	}
}
