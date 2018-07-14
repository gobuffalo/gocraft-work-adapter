package gwa

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/gobuffalo/buffalo/worker"
	"github.com/gobuffalo/envy"
	"github.com/gomodule/redigo/redis"
	"github.com/markbates/going/randx"
	"github.com/stretchr/testify/require"
)

var q = New(Options{
	Pool: &redis.Pool{
		MaxActive: 25,
		MaxIdle:   25,
		Wait:      true,
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", envy.Get("REDIS_PORT", ":6379"))
		},
	},
	Name:           randx.String(20),
	MaxConcurrency: 25,
})

func TestMain(m *testing.M) {
	ctx, cancel := context.WithCancel(context.Background())
	ctx, cancel = context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	go func() {
		select {
		case <-ctx.Done():
			cancel()
			log.Fatal(ctx.Err())
		}
	}()

	go func() {
		fmt.Println("start q")
		err := q.Start(ctx)
		if err != nil {
			cancel()
			log.Fatal(err)
		}
	}()

	code := m.Run()

	err := q.Stop()
	if err != nil {
		log.Fatal(err)
	}
	os.Exit(code)
}

func Test_Perform(t *testing.T) {
	r := require.New(t)

	var hit bool
	wg := &sync.WaitGroup{}
	wg.Add(1)
	q.Register("perform", func(worker.Args) error {
		hit = true
		wg.Done()
		return nil
	})
	q.Perform(worker.Job{
		Handler: "perform",
	})
	wg.Wait()
	r.True(hit)
}

func Test_PerformAt(t *testing.T) {
	r := require.New(t)

	var hit bool
	wg := &sync.WaitGroup{}
	wg.Add(1)
	q.Register("perform_at", func(args worker.Args) error {
		hit = true
		wg.Done()
		return nil
	})
	q.PerformAt(worker.Job{
		Handler: "perform_at",
	}, time.Now().Add(5*time.Nanosecond))
	wg.Wait()
	r.True(hit)
}

func Test_PerformIn(t *testing.T) {
	r := require.New(t)

	var hit bool
	wg := &sync.WaitGroup{}
	wg.Add(1)
	q.Register("perform_in", func(worker.Args) error {
		hit = true
		wg.Done()
		return nil
	})
	q.PerformIn(worker.Job{
		Handler: "perform_in",
	}, 5*time.Nanosecond)
	wg.Wait()
	r.True(hit)
}
