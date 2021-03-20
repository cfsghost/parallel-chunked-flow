package parallel_chunked_flow

import (
	"errors"
	"sync/atomic"
	"time"
)

type Chunk struct {
	id         int
	incoming   chan interface{}
	output     chan interface{}
	closed     chan struct{}
	counter    uint64
	bufferSize int
	Handler    func(interface{}, chan interface{})
	isClosing  bool
}

func NewChunk(size int) *Chunk {
	return &Chunk{
		bufferSize: size,
		incoming:   make(chan interface{}, size),
		output:     make(chan interface{}, size),
		closed:     make(chan struct{}),
		counter:    0,
		isClosing:  false,
	}
}

func (chunk *Chunk) Initialize() {
	go chunk.receiver()
}

func (chunk *Chunk) start(size int) {
	chunk.output = make(chan interface{}, size)
}

func (chunk *Chunk) close() error {

	if chunk.isClosing {
		return errors.New("In closeing")
	}
	chunk.isClosing = true

RETRY:
	if !chunk.isEmpty() {
		// delay
		timer := time.NewTimer(500 * time.Millisecond)
		<-timer.C

		// retry check chunk length
		goto RETRY
		//return chunk.close()
	}

	chunk.closed <- struct{}{}
	chunk.isClosing = false

	return nil
}

func (chunk *Chunk) receiver() {

	for {
		select {
		case data := <-chunk.incoming:

			// Process data from queue of chunk
			chunk.handle(data)
		case <-chunk.closed:
			close(chunk.output)
		}
	}
}

func (chunk *Chunk) handle(data interface{}) {
	chunk.Handler(data, chunk.output)
}

func (chunk *Chunk) push(data interface{}) bool {

	if chunk.len() == uint64(chunk.bufferSize) {
		return false
	}

	select {
	case chunk.incoming <- data:
		atomic.AddUint64((*uint64)(&chunk.counter), 1)
		return true
	default:
		// full
		return false
	}
}

func (chunk *Chunk) pop() interface{} {

	data := <-chunk.output
	atomic.AddUint64((*uint64)(&chunk.counter), ^uint64(0))

	return data
}

func (chunk *Chunk) len() uint64 {
	return atomic.LoadUint64(&chunk.counter)
}

func (chunk *Chunk) isEmpty() bool {

	if chunk.len() == 0 {
		return true
	}

	return false
}
