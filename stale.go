package cache

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// Source describes where the value returned by GetStale comes from.
type Source uint8

const (
	// Fresh means the value is still inside its fresh window and was served
	// from the local state without contacting Redis.
	Fresh Source = iota
	// Stale means the value is past its fresh window but still inside its
	// stale window. At most one shared refresh is in flight while the value
	// is served.
	Stale
	// Refresh means the value was produced by a shared refresh (a Redis
	// read-through or an item.Do call) because no usable value was available.
	Refresh
)

func (s Source) String() string {
	switch s {
	case Fresh:
		return "fresh"
	case Stale:
		return "stale"
	case Refresh:
		return "refresh"
	default:
		return "unknown"
	}
}

// StaleItem is the input to GetStale. It declares, per key, the fresh and
// stale windows that implement the stale-while-revalidate read strategy.
type StaleItem struct {
	Ctx context.Context

	Key string

	// Value is the destination the result is unmarshaled into, like
	// Item.Value in Once.
	Value interface{}

	// Fresh is the duration during which a cached value is served directly
	// without contacting Redis. It must be greater than zero.
	Fresh time.Duration

	// Stale is the duration after the fresh window during which the last
	// known value may still be served while a single shared refresh runs.
	// After Fresh+Stale (the absolute deadline) the value is never served.
	// Zero means stale values are never served.
	Stale time.Duration

	// Do is the optional source of truth, used when Redis does not have a
	// usable value (cache miss or Redis error with no stale value left).
	// When Do succeeds, its result atomically replaces the local state and
	// is written back to Redis.
	Do func(*StaleItem) (interface{}, error)
}

func (item *StaleItem) Context() context.Context {
	if item.Ctx == nil {
		return context.Background()
	}
	return item.Ctx
}

// StaleResult is the result of a successful GetStale call.
type StaleResult struct {
	// Source reports where the value unmarshaled into StaleItem.Value came
	// from.
	Source Source
}

//------------------------------------------------------------------------------

// staleEntry is an immutable snapshot of a key's stale-while-revalidate
// state. Replacements are installed atomically by swapping the pointer under
// staleStore.mu, so readers can safely use a snapshot without holding the lock.
type staleEntry struct {
	data     []byte
	version  uint64
	storedAt time.Time
	fresh    time.Duration
	stale    time.Duration
}

func (e *staleEntry) isFresh(now time.Time) bool {
	return !now.After(e.storedAt.Add(e.fresh))
}

func (e *staleEntry) absoluteExpired(now time.Time) bool {
	return now.After(e.storedAt.Add(e.fresh + e.stale))
}

// staleCall represents a single shared refresh in flight for a key. Every
// caller that needs the refresh waits on done; only the leader runs it.
type staleCall struct {
	done chan struct{}
	ver  uint64
	err  error
}

// staleStore holds the versioned stale-while-revalidate state. It is separate
// from LocalCache on purpose: Get and Once keep their existing semantics,
// while GetStale needs per-key fresh/stale windows and version-gated replaces
// that the LocalCache interface cannot express.
type staleStore struct {
	mu sync.Mutex

	entries  map[string]*staleEntry
	calls    map[string]*staleCall
	versions map[string]uint64

	now func() time.Time
}

func newStaleStore() *staleStore {
	return &staleStore{
		entries:  make(map[string]*staleEntry),
		calls:    make(map[string]*staleCall),
		versions: make(map[string]uint64),
		now:      time.Now,
	}
}

func (s *staleStore) get(key string) *staleEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.entries[key]
}

// lookup atomically returns the current entry together with the in-flight
// refresh for the key. When no refresh exists, start is true and the caller
// becomes the leader and must run it. Combining the entry read and call
// creation under one lock closes the cold-start window in which a caller
// could observe "no entry and no call" while another leader was already
// running, which would otherwise fan out duplicate refreshes.
func (s *staleStore) lookup(key string) (entry *staleEntry, call *staleCall, start bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if call, ok := s.calls[key]; ok {
		return s.entries[key], call, false
	}

	call = &staleCall{
		done: make(chan struct{}),
		ver:  s.versions[key],
	}
	s.calls[key] = call
	return s.entries[key], call, true
}

// release cancels a call that was optimistically created by lookup but turns
// out to be unnecessary (e.g. a fresh entry was found). It only removes the
// exact call passed in, so a genuinely started refresh is never touched.
func (s *staleStore) release(key string, call *staleCall) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cur, ok := s.calls[key]; ok && cur == call {
		delete(s.calls, key)
		close(call.done)
	}
}

// commit atomically replaces the entry, but only if the key's current version
// still matches the version the refresh started with. A newer refresh, a Set
// through another path or a Delete makes the refresh discard its result, so
// an older late-arriving value can never overwrite a newer one.
func (s *staleStore) commit(
	key string, ver uint64, data []byte, fresh, stale time.Duration,
) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.versions[key] != ver {
		return false
	}

	newVer := ver + 1
	s.versions[key] = newVer
	s.entries[key] = &staleEntry{
		data:     data,
		version:  newVer,
		storedAt: s.now(),
		fresh:    fresh,
		stale:    stale,
	}
	return true
}

// finish publishes the refresh outcome and unblocks every waiting caller.
func (s *staleStore) finish(key string, call *staleCall) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if cur, ok := s.calls[key]; ok && cur == call {
		delete(s.calls, key)
		close(call.done)
	}
}

// delete removes the entry and bumps the version so a refresh already in
// flight cannot resurrect the deleted key.
func (s *staleStore) delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.entries, key)
	s.versions[key]++
}

// evictCorrupt drops an entry that fails to unmarshal. When a refresh is
// already in flight for the key, the entry is removed without bumping the
// version, so the in-flight refresh can still commit its replacement under
// the version it started with.
func (s *staleStore) evictCorrupt(key string, ver uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, ok := s.entries[key]
	if !ok || entry.version != ver {
		return
	}
	delete(s.entries, key)
	if _, busy := s.calls[key]; !busy {
		s.versions[key] = ver + 1
	}
}

// evictIfExpired removes an entry once its absolute deadline has passed, as
// long as no refresh is currently trying to replace it.
func (s *staleStore) evictIfExpired(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, ok := s.entries[key]
	if !ok {
		return
	}
	if _, busy := s.calls[key]; busy {
		return
	}
	if entry.absoluteExpired(s.now()) {
		delete(s.entries, key)
		s.versions[key] = entry.version + 1
	}
}

//------------------------------------------------------------------------------

// GetStale implements a stale-while-revalidate read for item.Key:
//
//   - Inside the fresh window the local value is returned with Source=Fresh
//     and Redis is not contacted.
//   - Inside the stale window the last known value is returned with
//     Source=Stale while at most one shared refresh is in flight; concurrent
//     callers either get the stale value immediately or join the wait.
//   - After the absolute deadline (Fresh+Stale) old data is never returned;
//     callers wait for the shared refresh and get Source=Refresh, or the
//     refresh error.
//
// A Redis error does not invalidate a value that is still inside its stale
// window. Cancelling the request context unblocks the caller but never
// cancels a shared refresh that has already started.
//
// GetStale keeps its own versioned state and does not read from or write to
// Options.LocalCache; the local state is maintained inside Cache as long as
// Redis or LocalCache is configured.
func (cd *Cache) GetStale(item *StaleItem) (*StaleResult, error) {
	if item.Fresh <= 0 {
		return nil, errors.New("cache: StaleItem.Fresh must be greater than zero")
	}
	if cd.opt.Redis == nil && cd.opt.LocalCache == nil {
		return nil, errRedisLocalCacheNil
	}

	store := cd.stale
	key := item.Key
	store.evictIfExpired(key)

	// Atomically snapshot the entry and join or create the single shared
	// refresh, so callers can never disagree about whether a refresh is
	// already running.
	entry, call, start := store.lookup(key)

	if entry != nil {
		now := store.now()

		// A value inside its fresh window needs no refresh at all.
		if entry.isFresh(now) {
			if cd.unmarshal(entry.data, item.Value) == nil {
				if start {
					store.release(key, call)
				}
				cd.staleHit()
				return &StaleResult{Source: Fresh}, nil
			}
			// Undecodable fresh value: drop it and keep the refresh.
			store.evictCorrupt(key, entry.version)
			return cd.runOrAwait(item, store, call, start)
		}

		// Inside the stale window: keep the refresh going and serve the old
		// value while it is still decodable.
		if !entry.absoluteExpired(now) {
			if cd.unmarshal(entry.data, item.Value) == nil {
				cd.staleHit()
				if start {
					go cd.runStaleRefresh(item, store, call)
				}
				return &StaleResult{Source: Stale}, nil
			}
			// Undecodable stale value: drop it and wait for the refresh.
			store.evictCorrupt(key, entry.version)
			return cd.runOrAwait(item, store, call, start)
		}

		// Absolute-expired entries are already removed by evictIfExpired when
		// no refresh is busy; otherwise fall through to the foreground wait.
	}

	return cd.runOrAwait(item, store, call, start)
}

// runOrAwait starts the shared refresh when the caller is its leader, then
// waits for it in the foreground.
func (cd *Cache) runOrAwait(
	item *StaleItem, store *staleStore, call *staleCall, start bool,
) (*StaleResult, error) {
	if start {
		go cd.runStaleRefresh(item, store, call)
	}
	return cd.awaitStale(item, store, call)
}

// runStaleRefresh executes the shared refresh. The item is copied and its
// context detached, so cancelling the originating request cannot cancel a
// refresh other callers may already be waiting on.
func (cd *Cache) runStaleRefresh(item *StaleItem, store *staleStore, call *staleCall) {
	itemCopy := *item
	itemCopy.Ctx = detachedContext{parent: item.Context()}

	call.err = cd.refreshStale(&itemCopy, store, call.ver)
	store.finish(item.Key, call)
}

// refreshStale performs one read-through refresh and returns its error. It
// first tries Redis; on miss (or on a Redis error when Do is available) it
// falls back to item.Do and writes the result back.
func (cd *Cache) refreshStale(item *StaleItem, store *staleStore, ver uint64) error {
	var redisErr error
	if cd.opt.Redis != nil {
		b, err := cd.opt.Redis.Get(item.Context(), item.Key).Bytes()
		if err == nil {
			// A newer Redis value resets the fresh/stale windows locally.
			store.commit(item.Key, ver, b, item.Fresh, item.Stale)
			return nil
		}
		redisErr = err
		if err == redis.Nil {
			redisErr = ErrCacheMiss
		}
	}

	if item.Do == nil {
		if redisErr != nil {
			return redisErr
		}
		return ErrCacheMiss
	}

	value, err := item.Do(item)
	if err != nil {
		return err
	}

	b, err := cd.Marshal(value)
	if err != nil {
		return err
	}

	// Replace the local state first, under the version CAS: a refresh that
	// has been superseded simply discards its result.
	if !store.commit(item.Key, ver, b, item.Fresh, item.Stale) {
		return nil
	}

	// Then write the remote version. A Redis failure here does not roll the
	// local value back: callers can keep serving it until the absolute
	// deadline, and the next refresh retries the write.
	if cd.opt.Redis != nil {
		ttl := item.Fresh + item.Stale
		if ttl < time.Second {
			ttl = time.Second
		}
		if err := cd.opt.Redis.Set(item.Context(), item.Key, b, ttl).Err(); err != nil {
			return err
		}
	}
	return nil
}

// awaitStale waits for the shared refresh to finish and resolves what the
// caller should receive: the refreshed value, a still-usable stale value, or
// an explicit error.
func (cd *Cache) awaitStale(
	item *StaleItem, store *staleStore, call *staleCall,
) (*StaleResult, error) {
	ctx := item.Context()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-call.done:
	}

	entry := store.get(item.Key)
	if entry != nil {
		if err := cd.unmarshal(entry.data, item.Value); err != nil {
			store.evictCorrupt(item.Key, entry.version)
			return nil, err
		}
		// A successful refresh always advances the version by exactly one.
		if entry.version == call.ver+1 {
			return &StaleResult{Source: Refresh}, nil
		}
		if !entry.absoluteExpired(store.now()) {
			return &StaleResult{Source: Stale}, nil
		}
	}

	cd.staleMiss()
	if call.err != nil {
		return nil, call.err
	}
	return nil, ErrCacheMiss
}

func (cd *Cache) staleHit() {
	if cd.opt.StatsEnabled {
		atomic.AddUint64(&cd.hits, 1)
	}
}

func (cd *Cache) staleMiss() {
	if cd.opt.StatsEnabled {
		atomic.AddUint64(&cd.misses, 1)
	}
}

//------------------------------------------------------------------------------

// detachedContext carries the values of its parent but is never cancelled and
// has no deadline of its own. Shared refreshes outlive the request that
// started them.
type detachedContext struct {
	parent context.Context
}

func (ctx detachedContext) Deadline() (time.Time, bool) {
	return time.Time{}, false
}

func (ctx detachedContext) Done() <-chan struct{} {
	return nil
}

func (ctx detachedContext) Err() error {
	return nil
}

func (ctx detachedContext) Value(key interface{}) interface{} {
	return ctx.parent.Value(key)
}
