package parallel_chunked_flow

import (
	"sync"
	"sync/atomic"
)

type Chunk struct {
	id           int
	incoming     chan interface{}
	output       chan interface{}
	closed       chan struct{}
	suspended    chan struct{}
	pendingCount uint64
	outputCount  uint64
	bufferSize   int
	isClosed     bool
	isActive     bool
	Handler      func(interface{}, func(interface{}))
	mutex        sync.Mutex
}

func NewChunk(size int) *Chunk {
	return &Chunk{
		bufferSize:   size,
		incoming:     make(chan interface{}, size),
		output:       make(chan interface{}, size),
		closed:       make(chan struct{}),
		suspended:    make(chan struct{}),
		pendingCount: 0,
		outputCount:  0,
		isClosed:     false,
		isActive:     false,
	}
}

func (chunk *Chunk) Initialize() {
	go chunk.receiver()
}

func (chunk *Chunk) Output() chan interface{} {
	return chunk.output
}

func (chunk *Chunk) activate() {
	chunk.isActive = true
}

func (chunk *Chunk) deactivate() {
	chunk.isActive = false
}

func (chunk *Chunk) close() {

	chunk.mutex.Lock()
	defer chunk.mutex.Unlock()

	if chunk.isClosed {
		return
	}

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
			close(chunk.output)
			return
		}
	}
}

func (chunk *Chunk) handle(data interface{}) {

	if chunk.isClosed {
		return
	}

	chunk.Handler(data, chunk.publish)

	// update counter
	atomic.AddUint64((*uint64)(&chunk.pendingCount), ^uint64(0))
}

func (chunk *Chunk) push(data interface{}) bool {

	if chunk.isClosed {
		return false
	}

	// full
	if chunk.isFull() {
		return false
	}

	atomic.AddUint64((*uint64)(&chunk.pendingCount), 1)
	chunk.incoming <- data

	return true
}

func (chunk *Chunk) publish(data interface{}) {

	if chunk.isClosed {
		return
	}

	atomic.AddUint64((*uint64)(&chunk.outputCount), 1)
	chunk.output <- data
}

func (chunk *Chunk) ack() {
	atomic.AddUint64((*uint64)(&chunk.outputCount), ^uint64(0))
}

func (chunk *Chunk) isFull() bool {
	pendingCount := atomic.LoadUint64(&chunk.pendingCount)
	if pendingCount < uint64(chunk.bufferSize) {
		return false
	}

	return true
}

func (chunk *Chunk) isEmpty() bool {

	pendingCount := atomic.LoadUint64(&chunk.pendingCount)
	outputCount := atomic.LoadUint64(&chunk.outputCount)

	if pendingCount == 0 && outputCount == 0 {
		return true
	}

	return false
}
