package gpool

import (
	"sync/atomic"
	"unsafe"
)

// WorkerQueue WorkerQueue
type WorkerQueue interface {
	Get() (Worker, bool)
	Put(Worker)
}

type queueItem struct {
	val  interface{}
	next unsafe.Pointer
}

type workerQueue struct {
	head unsafe.Pointer
	tail unsafe.Pointer
}

func defaultWorkerQueueFactory() WorkerQueue {
	item := &queueItem{}
	pointer := unsafe.Pointer(item)
	wq := &workerQueue{
		head: pointer,
		tail: pointer,
	}
	return wq
}

// Push 添加元素
func (wq *workerQueue) Put(worker Worker) {
	newItem := &queueItem{val: worker}
	newItemPointer := unsafe.Pointer(newItem)
	tailPointer := atomic.LoadPointer(&wq.tail)
	tail := (*queueItem)(tailPointer)

	for !atomic.CompareAndSwapPointer(&tail.next, nil, newItemPointer) {
		nextPointer := atomic.LoadPointer(&tail.next)
		for nextPointer != nil {
			tail = (*queueItem)(nextPointer)
			tailPointer = nextPointer
			nextPointer = atomic.LoadPointer(&tail.next)
		}
	}

	for {
		oldTailPointer := atomic.LoadPointer(&wq.tail)
		nextPointer := atomic.LoadPointer(&tail.next)
		for nextPointer != nil {
			tail = (*queueItem)(nextPointer)
			tailPointer = nextPointer
			nextPointer = atomic.LoadPointer(&tail.next)
		}
		for atomic.CompareAndSwapPointer(&wq.tail, oldTailPointer, tailPointer) {
			return
		}
	}
}

// Pop 获取元素
func (wq *workerQueue) Get() (Worker, bool) {
	for {
		headPointer := atomic.LoadPointer(&wq.head)
		head := (*queueItem)(headPointer)

		next := atomic.LoadPointer(&head.next)
		if next == nil {
			return nil, false
		}
		nextPointer := unsafe.Pointer(next)

		if atomic.CompareAndSwapPointer(&wq.head, headPointer, nextPointer) {
			nextItem := (*queueItem)(unsafe.Pointer(next))
			return nextItem.val.(Worker), true
		}
	}
}
