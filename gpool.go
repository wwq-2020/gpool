package gpool

import (
	"context"
	"log"
	"runtime"
	"sync"
	"sync/atomic"
)

// Pool Pool
type Pool interface {
	Do(task Task)
	Close()
}

type pool struct {
	wg            sync.WaitGroup
	ctx           context.Context
	cancel        func()
	workQueue     WorkerQueue
	taskQueue     chan func()
	maxSize       int64
	size          int64
	workerFactory WorkerFactory
}

// New New
func New(maxSize int64) Pool {
	ctx, cancel := context.WithCancel(context.Background())
	return &pool{
		ctx:           ctx,
		cancel:        cancel,
		workQueue:     defaultWorkerQueueFactory(),
		taskQueue:     make(chan func(), 1024),
		maxSize:       maxSize,
		workerFactory: defaultWorkerFactory,
	}
}

func (p *pool) getWorker() Worker {
	for {
		w, ok := p.workQueue.Get()
		if !ok {
			p.wg.Add(1)
			w = p.workerFactory(p.ctx.Done(), p.taskQueue, func() { p.wg.Done() })
			size := atomic.LoadInt64(&p.size)
			if size <= p.maxSize && atomic.CompareAndSwapInt64(&p.size, size, size+1) {
				return w
			}
			runtime.Gosched()
			continue
		}
		return w
	}
}

// Do Do
func (p *pool) Do(task Task) {
	w := p.getWorker()
	ch := make(chan struct{})
	taskWrapped := func() {
		defer func() {
			if err := recover(); err != nil {
				log.Printf("recovery from %#v", err)
			}
			p.workQueue.Put(w)
			close(ch)
		}()
		task(p.ctx)
	}
	w.Do(taskWrapped)
	<-ch
}

func (p *pool) AsyncDo(task Task) {
	w, ok := p.workQueue.Get()
	if !ok {
		taskWrapped := func() {
			defer func() {
				if err := recover(); err != nil {
					log.Printf("recovery from %#v", err)
				}
			}()
			task(p.ctx)
		}
		p.taskQueue <- taskWrapped
		return
	}

	ch := make(chan struct{})
	taskWrapped := func() {
		defer func() {
			if err := recover(); err != nil {
				log.Printf("recovery from %#v", err)
			}
			p.workQueue.Put(w)
			close(ch)
		}()
		task(p.ctx)
	}

	w.Do(taskWrapped)
}

// Close Close
func (p *pool) Close() {
	p.cancel()
}

// GracefulClose GracefulClose
func (p *pool) GracefulClose() {
	p.cancel()
	p.wg.Wait()
}
