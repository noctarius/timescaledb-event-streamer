package waiting

import (
	"fmt"
	"time"
)

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

func NewWaiterWithTimeout(
	timeout time.Duration,
) *Waiter {

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
