package supporting

import (
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"
)

var validCharacters = []string{
	"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m",
	"n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z",
}

func RandomTextString(length int) string {
	builder := strings.Builder{}
	for i := 0; i < length; i++ {
		index := rand.Intn(len(validCharacters))
		builder.WriteString(validCharacters[index])
	}
	return builder.String()
}

func DistinctItems[V any](items []V, identityFn func(item V) string) []V {
	distincting := make(map[string]V, 0)
	for _, item := range items {
		identity := identityFn(item)
		distincting[identity] = item
	}
	distinctItems := make([]V, 0, len(distincting))
	for _, item := range distincting {
		distinctItems = append(distinctItems, item)
	}
	return distinctItems
}

var ErrWaiterTimeout = fmt.Errorf("waiter timed out")

type Waiter struct {
	done    chan bool
	timer   *time.Timer
	timeout time.Duration
}

func NewWaiter() *Waiter {
	return &Waiter{
		done: make(chan bool, 1),
	}
}

func NewWaiterWithTimeout(timeout time.Duration) *Waiter {
	return &Waiter{
		done:    make(chan bool, 1),
		timer:   time.NewTimer(timeout),
		timeout: timeout,
	}
}

func (w *Waiter) Reset() {
	if w.timer != nil {
		w.timer.Stop()
		// Make sure channel is drained
		select {
		case <-w.timer.C:
		default:
		}
		w.timer = time.NewTimer(w.timeout)
	}
}

func (w *Waiter) Signal() {
	w.done <- true
}

func (w *Waiter) Await() error {
	if w.timer == nil {
		<-w.done
		return nil
	}

	select {
	case <-w.done:
		w.timer.Stop()
		// Make sure channel is drained
		select {
		case <-w.timer.C:
		default:
		}
		return nil
	case <-w.timer.C:
		return ErrWaiterTimeout
	}
}

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

func (sa *ShutdownAwaiter) AwaitShutdown() {
	sa.start.Await()
}

func (sa *ShutdownAwaiter) AwaitShutdownChan() <-chan bool {
	return sa.start.done
}

func (sa *ShutdownAwaiter) SignalDone() {
	sa.done.Signal()
}

func (sa *ShutdownAwaiter) AwaitDone() {
	sa.done.Await()
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

func (msa *MultiShutdownAwaiter) AwaitShutdown(slot uint) {
	msa.starters[slot].Await()
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
