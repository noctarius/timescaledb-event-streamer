package supporting

// Channel is based on https://github.com/jtarchie/ringbuffer, but
// extended with the option to skip waiting on an empty output channel
type Channel[T any] struct {
	input  chan T
	output chan T
}

func NewChannel[T any](size int) *Channel[T] {
	b := &Channel[T]{
		input:  make(chan T),
		output: make(chan T, size),
	}
	go b.run()

	return b
}

func (c *Channel[T]) run() {
	for v := range c.input {
	retry:
		select {
		case c.output <- v:
		default:
			<-c.output

			goto retry
		}
	}

	close(c.output)
}

func (c *Channel[T]) Write(value T) {
	c.input <- value
}

func (c *Channel[T]) Read() T {
	return <-c.output
}

func (c *Channel[T]) ReadChannel() <-chan T {
	return c.output
}

func (c *Channel[T]) Close() {
	close(c.input)
}
