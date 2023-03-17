package logicalreplicationresolver

import (
	"container/list"
	"github.com/jackc/pglogrepl"
	"sync"
)

type replicationQueue struct {
	queue  *list.List
	mutex  sync.Mutex
	locked bool
}

func newReplicationQueue() *replicationQueue {
	return &replicationQueue{
		queue: list.New(),
		mutex: sync.Mutex{},
	}
}

func (rq *replicationQueue) push(fn func(snapshot pglogrepl.LSN) error) bool {
	rq.mutex.Lock()
	defer rq.mutex.Unlock()

	if rq.locked {
		return false
	}

	rq.queue.PushBack(fn)
	return true
}

func (rq *replicationQueue) pop() func(snapshot pglogrepl.LSN) error {
	rq.mutex.Lock()
	defer rq.mutex.Unlock()

	e := rq.queue.Front()
	if e == nil {
		return nil
	}
	rq.queue.Remove(e)
	return e.Value.(func(pglogrepl.LSN) error)
}

func (rq *replicationQueue) lock() {
	rq.mutex.Lock()
	defer rq.mutex.Unlock()

	rq.locked = true
}
