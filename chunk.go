package parallel_chunked_flow

import (
	"sync/atomic"
)

type Chunk struct {
	id         int
	incoming   chan interface{}
	output     chan interface{}
	closed     chan struct{}
	counter    uint64
	bufferSize int
	isClosed   bool
	isActive   bool
	Handler    func(interface{}, chan interface{}, func())
}

func NewChunk(size int) *Chunk {
	return &Chunk{
		bufferSize: size,
		incoming:   make(chan interface{}, size),
		output:     make(chan interface{}, size),
		closed:     make(chan struct{}),
		counter:    0,
		isClosed:   false,
		isActive:   false,
	}
}

func (chunk *Chunk) Initialize() {
	go chunk.receiver()
}

func (chunk *Chunk) Output() chan interface{} {
	return chunk.output
}

func (chunk *Chunk) resetOutput() {

	// Chunk is closed already. Do nothing.
	if chunk.isClosed {
		return
	}

	close(chunk.output)
	chunk.output = make(chan interface{}, chunk.bufferSize)
}

func (chunk *Chunk) activate() {
	chunk.isActive = true
}

func (chunk *Chunk) deactivate() {
	chunk.isActive = false
	chunk.checkStatus()
}

func (chunk *Chunk) close() {
	chunk.isClosed = true
	chunk.closed <- struct{}{}
}

func (chunk *Chunk) receiver() {

	for {
		select {
		case data := <-chunk.incoming:

			// Process data from queue of chunk
			chunk.handle(data)
		case <-chunk.closed:
			return
		}
	}
}

func (chunk *Chunk) handle(data interface{}) {
	chunk.Handler(data, chunk.output, chunk.reject)
}

func (chunk *Chunk) push(data interface{}) bool {

	// full
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
	chunk.unref()

	return data
}

func (chunk *Chunk) reject() {
	chunk.unref()
}

func (chunk *Chunk) unref() uint64 {
	count := atomic.AddUint64((*uint64)(&chunk.counter), ^uint64(0))
	if count == 0 && !chunk.isActive {
		chunk.resetOutput()
	}
	return count
}

func (chunk *Chunk) checkStatus() {
	if chunk.isEmpty() && !chunk.isActive {
		chunk.resetOutput()
	}
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
