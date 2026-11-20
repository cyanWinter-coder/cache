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

// Example_staleWhileRevalidate shows how to serve a value from a fresh window,
// then keep serving the last known value during a stale window while a single
// shared refresh runs, instead of letting every instance hit the origin at once.
func Example_staleWhileRevalidate() {
	ring := redis.NewRing(&redis.RingOptions{
		Addrs: map[string]string{
			"server1": ":6379",
		},
	})

	mycache := cache.New(&cache.Options{
		Redis:      ring,
		LocalCache: cache.NewTinyLFU(1000, time.Minute),
	})

	var obj Object
	res, err := mycache.GetStale(&cache.StaleItem{
		Ctx:   context.TODO(),
		Key:   "hot-config",
		Value: &obj,
		// Serve directly for a minute, then allow up to ten minutes of
		// stale serving while a single shared refresh revalidates.
		Fresh: time.Minute,
		Stale: 10 * time.Minute,
		Do: func(*cache.StaleItem) (interface{}, error) {
			return &Object{
				Str: "mystring",
				Num: 42,
			}, nil
		},
	})
	if err != nil {
		panic(err)
	}

	// res.Source is cache.Fresh, cache.Stale or cache.Refresh.
	fmt.Println(res.Source, obj)
	// Output: refresh {mystring 42}
}
