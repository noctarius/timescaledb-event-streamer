package waiting

import "sync"

type ShutdownAwaiter struct {
	start *Waiter
	done  *Waiter
}

func NewShutdownAwaiter() *ShutdownAwaiter {
	return &ShutdownAwaiter{
		start: NewWaiter(),
		done:  NewWaiter(),
	}
}

func (sa *ShutdownAwaiter) SignalShutdown() {
	sa.start.Signal()
}

func (sa *ShutdownAwaiter) AwaitShutdown() error {
	return sa.start.Await()
}

func (sa *ShutdownAwaiter) AwaitShutdownChan() <-chan bool {
	return sa.start.done
}

func (sa *ShutdownAwaiter) SignalDone() {
	sa.done.Signal()
}

func (sa *ShutdownAwaiter) AwaitDone() error {
	return sa.done.Await()
}

type MultiShutdownAwaiter struct {
	slots     uint
	starters  []*Waiter
	doneGroup sync.WaitGroup
}

func NewMultiShutdownAwaiter(slots uint) *MultiShutdownAwaiter {
	starters := make([]*Waiter, 0, slots)
	for i := uint(0); i < slots; i++ {
		starters = append(starters, NewWaiter())
	}
	msa := &MultiShutdownAwaiter{
		slots:     slots,
		starters:  starters,
		doneGroup: sync.WaitGroup{},
	}
	msa.doneGroup.Add(int(slots))
	return msa
}

func (msa *MultiShutdownAwaiter) SignalShutdown() {
	for i := uint(0); i < msa.slots; i++ {
		msa.starters[i].Signal()
	}
}

func (msa *MultiShutdownAwaiter) AwaitShutdown(slot uint) error {
	return msa.starters[slot].Await()
}

func (msa *MultiShutdownAwaiter) AwaitShutdownChan(slot uint) <-chan bool {
	return msa.starters[slot].done
}

func (msa *MultiShutdownAwaiter) SignalDone() {
	msa.doneGroup.Done()
}

func (msa *MultiShutdownAwaiter) AwaitDone() {
	msa.doneGroup.Wait()
}
