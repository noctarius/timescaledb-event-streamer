package containers

import "github.com/noctarius/timescaledb-event-streamer/internal/functional"

type UnboundedChannel[T any] struct {
	in  chan<- T
	out <-chan T
}

func MakeUnboundedChannel[T any](
	initialCapacity int,
) *UnboundedChannel[T] {

	in := make(chan T)
	out := make(chan T)

	go func() {
		inQueue := make([]T, 0, initialCapacity)
		outChannel := func() chan T {
			if len(inQueue) == 0 {
				return nil
			}
			return out
		}
		currentValue := func() T {
			if len(inQueue) == 0 {
				return functional.Zero[T]()
			}
			return inQueue[0]
		}
		for len(inQueue) > 0 || in != nil {
			select {
			case v, ok := <-in:
				if !ok {
					in = nil
				} else {
					inQueue = append(inQueue, v)
				}
			case outChannel() <- currentValue():
				inQueue = inQueue[1:]
			}
		}
		close(out)
	}()
	return &UnboundedChannel[T]{
		in:  in,
		out: out,
	}
}

func (uc *UnboundedChannel[T]) Send(
	item T,
) {

	uc.in <- item
}

func (uc *UnboundedChannel[T]) Receive() T {
	return <-uc.out
}

func (uc *UnboundedChannel[T]) ReceiveChannel() <-chan T {
	return uc.out
}

func (uc *UnboundedChannel[T]) Close() {
	close(uc.in)
}
