package supporting

import (
	"container/list"
	"sync"
)

type Queue[E any] struct {
	queue  *list.List
	mutex  sync.Mutex
	locked bool
}

func NewQueue[E any]() *Queue[E] {
	return &Queue[E]{
		queue: list.New(),
		mutex: sync.Mutex{},
	}
}

func (rq *Queue[E]) Push(fn E) bool {
	rq.mutex.Lock()
	defer rq.mutex.Unlock()

	if rq.locked {
		return false
	}

	rq.queue.PushBack(fn)
	return true
}

func (rq *Queue[E]) Pop() E {
	rq.mutex.Lock()
	defer rq.mutex.Unlock()

	e := rq.queue.Front()
	if e == nil {
		return *new(E)
	}
	rq.queue.Remove(e)
	return e.Value.(E)
}

func (rq *Queue[E]) Lock() {
	rq.mutex.Lock()
	defer rq.mutex.Unlock()

	rq.locked = true
}
