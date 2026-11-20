# Redis cache library for Golang

[![Build Status](https://travis-ci.org/go-redis/cache.svg)](https://travis-ci.org/go-redis/cache)
[![GoDoc](https://godoc.org/github.com/go-redis/cache?status.svg)](https://pkg.go.dev/github.com/go-redis/cache/v9?tab=doc)

> go-redis/cache is brought to you by :star:
> [**uptrace/uptrace**](https://github.com/uptrace/uptrace). Uptrace is an open source and blazingly
> fast [distributed tracing tool](https://get.uptrace.dev/) powered by OpenTelemetry and ClickHouse.
> Give it a star as well!

go-redis/cache library implements a cache using Redis as a key/value storage. It uses
[MessagePack](https://github.com/vmihailenco/msgpack) to marshal values.

Optionally, you can use [TinyLFU](https://github.com/vmihailenco/go-tinylfu) or any other
[cache algorithm](https://github.com/vmihailenco/go-cache-benchmark) as a local in-process cache.

If you are interested in monitoring cache hit rate, see the guide for
[Monitoring using OpenTelemetry Metrics](https://blog.uptrace.dev/posts/opentelemetry-metrics-cache-stats/).

## Installation

go-redis/cache supports 2 last Go versions and requires a Go version with
[modules](https://github.com/golang/go/wiki/Modules) support. So make sure to initialize a Go
module:

```shell
go mod init github.com/my/repo
```

And then install go-redis/cache/v9 (note _v9_ in the import; omitting it is a popular mistake):

```shell
go get github.com/go-redis/cache/v9
```

## Quickstart

```go
package cache_test

import (
    "context"
    "fmt"
    "time"

    "github.com/redis/go-redis/v9"
    "github.com/go-redis/cache/v9"
)

type Object struct {
    Str string
    Num int
}

func Example_basicUsage() {
    ring := redis.NewRing(&redis.RingOptions{
        Addrs: map[string]string{
            "server1": ":6379",
            "server2": ":6380",
        },
    })

    mycache := cache.New(&cache.Options{
        Redis:      ring,
        LocalCache: cache.NewTinyLFU(1000, time.Minute),
    })

    ctx := context.TODO()
    key := "mykey"
    obj := &Object{
        Str: "mystring",
        Num: 42,
    }

    if err := mycache.Set(&cache.Item{
        Ctx:   ctx,
        Key:   key,
        Value: obj,
        TTL:   time.Hour,
    }); err != nil {
        panic(err)
    }

    var wanted Object
    if err := mycache.Get(ctx, key, &wanted); err == nil {
        fmt.Println(wanted)
    }

    // Output: {mystring 42}
}

func Example_advancedUsage() {
    ring := redis.NewRing(&redis.RingOptions{
        Addrs: map[string]string{
            "server1": ":6379",
            "server2": ":6380",
        },
    })

    mycache := cache.New(&cache.Options{
        Redis:      ring,
        LocalCache: cache.NewTinyLFU(1000, time.Minute),
    })

    obj := new(Object)
    err := mycache.Once(&cache.Item{
        Key:   "mykey",
        Value: obj, // destination
        Do: func(*cache.Item) (interface{}, error) {
            return &Object{
                Str: "mystring",
                Num: 42,
            }, nil
        },
    })
    if err != nil {
        panic(err)
    }
    fmt.Println(obj)
    // Output: &{mystring 42}
}
```

## Stale-while-revalidate

For hot keys (for example configuration fetched on every request) a local
cache entry that expires while Redis is briefly unavailable would otherwise
make every instance fall back to the origin at the same time. `GetStale`
implements a [stale-while-revalidate](https://en.wikipedia.org/wiki/Cache_stamping)
read strategy with per-key windows:

- `Fresh` is the window during which the local value is served directly
  without contacting Redis.
- `Stale` is the extra window after freshness expires during which the last
  known value may still be served. At most **one** shared refresh runs per key;
  other callers receive the stale value immediately or wait for that refresh,
  so there is no stampede.
- After `Fresh + Stale` (the absolute deadline) old data is never returned and
  callers wait for the refresh, receiving its error if it cannot produce a
  value.

The result carries a `Source` (`Fresh`, `Stale` or `Refresh`) so callers know
where the value came from. Cancelling the request context unblocks the caller
but never cancels a shared refresh that has already started, and successful
refreshes replace the local and Redis versions atomically: an older
late-arriving result can never overwrite a newer value.

```go
var obj Object
res, err := mycache.GetStale(&cache.StaleItem{
    Ctx:   ctx,
    Key:   "hot-config",
    Value: &obj,
    Fresh: time.Minute,     // serve directly for a minute
    Stale: 10 * time.Minute, // then tolerate up to ten minutes of staleness
    Do: func(*cache.StaleItem) (interface{}, error) {
        return loadConfigFromOrigin()
    },
})
if err != nil {
    panic(err)
}
fmt.Println(res.Source, obj) // cache.Fresh | cache.Stale | cache.Refresh
```

Notes on semantics:

- A Redis error does not invalidate a value still inside its stale window; the
  value keeps being served until the absolute deadline.
- When `Do` is nil, `GetStale` only reads through Redis and returns
  `cache.ErrCacheMiss` (or the Redis error) when no usable value exists.
- `GetStale` keeps its own versioned state inside `Cache`. It does not read
  from or write to `Options.LocalCache`, and the existing `Get`, `Once`, `Set`
  and `Delete` APIs keep their original behavior. `Delete` also invalidates the
  stale-while-revalidate state so an in-flight refresh cannot resurrect a
  deleted key.
- A value that fails to unmarshal is evicted and replaced through a single
  shared refresh, mirroring the self-healing behavior of `Once`.

