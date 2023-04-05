package logicalreplicationresolver

import (
	"container/list"
	"sync"
)

type replicationQueue[E any] struct {
	queue  *list.List
	mutex  sync.Mutex
	locked bool
}

func newReplicationQueue[E any]() *replicationQueue[E] {
	return &replicationQueue[E]{
		queue: list.New(),
		mutex: sync.Mutex{},
	}
}

func (rq *replicationQueue[E]) push(fn E) bool {
	rq.mutex.Lock()
	defer rq.mutex.Unlock()

	if rq.locked {
		return false
	}

	rq.queue.PushBack(fn)
	return true
}

func (rq *replicationQueue[E]) pop() E {
	rq.mutex.Lock()
	defer rq.mutex.Unlock()

	e := rq.queue.Front()
	if e == nil {
		return *new(E)
	}
	rq.queue.Remove(e)
	return e.Value.(E)
}

func (rq *replicationQueue[E]) lock() {
	rq.mutex.Lock()
	defer rq.mutex.Unlock()

	rq.locked = true
}
