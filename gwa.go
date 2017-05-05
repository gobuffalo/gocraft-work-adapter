package gwa

import (
	"context"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/gobuffalo/buffalo/worker"
	"github.com/gocraft/work"
	"github.com/markbates/going/defaults"
	"github.com/pkg/errors"
)

type Options struct {
	*redis.Pool
	Name           string
	MaxConcurrency int
}

func New(opts Options) worker.Worker {
	ctx := context.Background()

	opts.Name = defaults.String(opts.Name, "buffalo")
	enqueuer := work.NewEnqueuer(opts.Name, opts.Pool)

	opts.MaxConcurrency = defaults.Int(opts.MaxConcurrency, 25)
	pool := work.NewWorkerPool(struct{}{}, uint(opts.MaxConcurrency), opts.Name, opts.Pool)

	return &Adapter{
		Enqueur: enqueuer,
		Pool:    pool,
		ctx:     ctx,
	}
}

type Adapter struct {
	Enqueur *work.Enqueuer
	Pool    *work.WorkerPool
	ctx     context.Context
}

func (q *Adapter) Start(ctx context.Context) error {
	q.ctx = ctx
	go func() {
		select {
		case <-ctx.Done():
			q.Stop()
		}
	}()
	q.Pool.Start()
	return nil
}

func (q *Adapter) Stop() error {
	q.Pool.Stop()
	return nil
}

func (q *Adapter) Register(name string, h worker.Handler) error {
	q.Pool.Job(name, func(job *work.Job) error {
		return h(job.Args)
	})
	return nil
}

func (q Adapter) Perform(job worker.Job) error {
	_, err := q.Enqueur.Enqueue(job.Handler, job.Args)
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (q Adapter) PerformIn(job worker.Job, t time.Duration) error {
	d := int64(t / time.Second)
	_, err := q.Enqueur.EnqueueIn(job.Handler, d, job.Args)
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (q Adapter) PerformAt(job worker.Job, t time.Time) error {
	return q.PerformIn(job, t.Sub(time.Now()))
}
