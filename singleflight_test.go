package zync

import (
	"context"
	"log"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type resItem struct {
	v     interface{}
	err   error
	owner bool
}

func TestSingleFlight(t *testing.T) {
	must := require.New(t)

	sf := NewSingleFlight()

	key := struct {
		A string
		B int
	}{
		A: "a",
		B: 1,
	}

	fn := func(res int) SingleFlightFunc {
		return func(ctx context.Context) (interface{}, error) {
			select {
			case <-ctx.Done():
				log.Println("quitting for no waiters")
				return nil, ctx.Err()
			case <-time.After(time.Second * 5):
				return res, nil
			}
		}
	}

	count := 5
	ch := make(chan resItem, count)

	f := func(res int, timeout time.Duration) {
		go func() {
			ctx, cancel := context.WithTimeout(context.TODO(), timeout)
			defer cancel()
			v, err, owner := sf.Do(ctx, key, fn(res))
			log.Printf("%v | %v | %v", v, err, owner)
			ch <- resItem{
				v:     v,
				err:   err,
				owner: owner,
			}
		}()
	}

	for res := 0; res < 2; res++ {
		log.Println("----- res = ", res, "-----")
		for i := 0; i < count; i++ {
			f(res, time.Second*time.Duration(rand.Intn(6)))
		}

		errCount := 0
		ownerCount := 0
		for i := 0; i < count; i++ {
			item := <-ch
			if item.err != nil {
				errCount += 1
			} else {
				must.Equal(res, item.v)
			}
			if item.owner {
				ownerCount += 1
			}
		}

		must.LessOrEqualf(ownerCount, 1, "more than 1 owner: %d", ownerCount)
	}
}

func TestSingleFlight_2(t *testing.T) {
	must := require.New(t)

	if !zyncTesting {
		t.Fatalf("not testing with `testing` build tag")
	}
	sf := NewSingleFlight()

	key := struct {
		A string
		B int
	}{
		A: "a",
		B: 1,
	}

	resCh := make(chan int)

	var internalCtx context.Context

	fn := func(ctx context.Context) (interface{}, error) {
		internalCtx = ctx
		select {
		case <-ctx.Done():
			log.Println("quitting for no waiters")
			return nil, ctx.Err()
		case i := <-resCh:
			return i, nil
		}
	}

	ctx, cancel := context.WithCancel(context.TODO())

	quitted := make(chan resItem, 1)

	go func() {
		v, err, owner := sf.Do(ctx, key, fn)
		log.Printf("1 | %v | %v | %v", v, err, owner)
		quitted <- resItem{
			v, err, owner,
		}
	}()
	wl.Lock()
	cancel()

	log.Printf("f1 canceled")
	res := <-quitted
	log.Printf("f1 quitted")
	must.Error(res.err)
	must.False(res.owner)

	ctx, cancel = context.WithCancel(context.TODO())

	go func() {
		v, err, owner := sf.Do(ctx, key, fn)
		log.Printf("2 | %v | %v | %v", v, err, owner)
		quitted <- resItem{
			v, err, owner,
		}
	}()

	cancel()
	log.Printf("f2 canceled")
	res = <-quitted
	log.Printf("f2 quitted")
	must.Error(res.err)
	must.False(res.owner)

	ctx, cancel = context.WithCancel(context.TODO())
	go func() {
		v, err, owner := sf.Do(ctx, key, fn)
		log.Printf("3 | %v | %v | %v", v, err, owner)
		quitted <- resItem{
			v, err, owner,
		}
	}()

	cancel()
	log.Printf("f3 canceled")
	res = <-quitted
	log.Printf("f3 quitted")
	must.Error(res.err)
	must.False(res.owner)

	v := 1
	go func() {
		v, err, owner := sf.Do(context.TODO(), key, fn)
		log.Printf("4 | %v | %v | %v", v, err, owner)
		quitted <- resItem{
			v, err, owner,
		}
	}()

	resCh <- v
	res = <-quitted
	log.Printf("f4 quitted")
	must.NoError(res.err)
	// must.True(res.owner) // we cannot ensure this
	must.Equal(v, res.v)

	wl.Unlock()

	// make sure that the internal context is canceled
	<-internalCtx.Done()
	must.Error(internalCtx.Err())
	log.Printf("internal context canceled")
}
