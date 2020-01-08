package zync

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
)

type singleFlightItem struct {
	notify  chan struct{}
	counter int32
	wg      sync.WaitGroup
	owner   chan bool
	done    chan struct{}
	v       interface{}
	err     error
}

func (item *singleFlightItem) incr() {
	item.wg.Add(1)
	atomic.AddInt32(&item.counter, 1)
}

func (item *singleFlightItem) decr() (ret int32) {
	ret = atomic.AddInt32(&item.counter, -1)
	item.wg.Done()
	return
}

type SingleFlight struct {
	mu sync.RWMutex
	m  map[interface{}]*singleFlightItem
}

func NewSingleFlight() *SingleFlight {
	return &SingleFlight{
		m: map[interface{}]*singleFlightItem{},
	}
}

type SingleFlightFunc func(ctx context.Context) (interface{}, error)

func (sf *SingleFlight) get(key interface{}) (item *singleFlightItem, owner bool) {
	sf.mu.RLock()
	if item = sf.m[key]; item != nil {
		item.incr()
		sf.mu.RUnlock()
		return
	}
	sf.mu.RUnlock()

	// double check
	sf.mu.Lock()
	item = sf.m[key]
	if item == nil {
		item = &singleFlightItem{
			notify: make(chan struct{}, 1),
			owner:  make(chan bool, 1),
			done:   make(chan struct{}),
		}
		sf.m[key] = item
		owner = true
	}
	item.incr()
	sf.mu.Unlock()
	return
}

func (sf *SingleFlight) delete(key interface{}) {
	sf.mu.Lock()
	delete(sf.m, key)
	sf.mu.Unlock()
}

var wl sync.Mutex

func (sf *SingleFlight) Do(ctx context.Context, key interface{}, fn SingleFlightFunc) (v interface{}, err error, owner bool) {
	item, owner := sf.get(key)
	if owner {
		subCtx, cancel := context.WithCancel(context.TODO())
		go func() {
			for {
				<-item.notify
				if atomic.LoadInt32(&item.counter) == 0 {
					break
				}
			}
			if zyncTesting {
				wl.Lock()
				log.Println("locked")
			}
			sf.delete(key)
			if zyncTesting {
				log.Println("deleted")
			}
			// the add and wait of WaitGroup can cause race problem
			// this wait is safe because no more add is possible
			item.wg.Wait()
			if zyncTesting {
				wl.Unlock()
				log.Println("unlocked")
			}
			cancel() // cancel the task when all waiters quited
		}()

		go func() {
			defer func() {
				if e := recover(); e != nil {
					if ex, ok := e.(error); ok {
						item.err = ex
					} else {
						item.err = fmt.Errorf("panic: %v", e)
					}
				}
				close(item.done)
			}()
			item.v, item.err = fn(subCtx)
		}()
	}
OuterLoop:
	for {
		select {
		case <-item.owner:
			owner = true
		case <-item.done:
			v = item.v
			err = item.err
			break OuterLoop
		case <-ctx.Done():
			if owner {
				// transfer owner
				owner = false
				select {
				case item.owner <- true:
				}
			}
			err = ctx.Err()
			break OuterLoop
		}
	}
	if item.decr() == 0 {
		if zyncTesting {
			log.Println("decr zero")
		}
		select {
		case item.notify <- struct{}{}:
			if zyncTesting {
				log.Println("decr zero notified")
			}
		default:
			if zyncTesting {
				log.Println("decr zero already notified")
			}
		}
	}
	return
}
