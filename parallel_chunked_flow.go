package parallel_chunked_flow

import (
	"container/ring"
	"errors"
	"time"
)

type ParallelChunkedFlow struct {
	closedDataReceiver chan struct{}
	closedDataExporter chan struct{}
	incoming           chan interface{}
	output             chan interface{}
	chunkSize          int
	chunks             *ring.Ring
	cursor             *ring.Ring
	isClosed           bool

	availableChunks chan *Chunk
	currentChunk    *Chunk

	handler func(interface{}, func(interface{}))
}

// NewParallelChunckedFlow creates a new parallel chunked flow
func NewParallelChunkedFlow(options *Options) *ParallelChunkedFlow {

	pcf := &ParallelChunkedFlow{
		closedDataReceiver: make(chan struct{}),
		closedDataExporter: make(chan struct{}),
		incoming:           make(chan interface{}, options.BufferSize),
		output:             make(chan interface{}, options.BufferSize),
		chunkSize:          options.ChunkSize,
		chunks:             ring.New(options.ChunkCount),
		availableChunks:    make(chan *Chunk, options.ChunkCount),
		handler:            options.Handler,
		isClosed:           true,
	}
	/*
		pcf := &ParallelChunkedFlow{
			incoming:        make(chan interface{}, 102400),
			output:          make(chan interface{}, 102400),
			chunkSize:       10,
			chunks:          ring.New(10),
			availableChunks: make(chan *Chunk, 10),
		}
	*/

	pcf.initialize()

	return pcf
}

func (pcf *ParallelChunkedFlow) initialize() error {

	// Initializing chunks
	pcf.cursor = pcf.chunks
	chunks := pcf.chunks
	for i := 0; i < pcf.chunks.Len(); i++ {
		chunk := NewChunk(pcf.chunkSize)
		chunk.id = i
		chunk.Handler = pcf.handler
		chunks.Value = chunk
		chunk.Initialize()
		chunks = chunks.Next()

		pcf.availableChunks <- chunk
	}

	// Set first chunk as default
	pcf.currentChunk = <-pcf.availableChunks
	pcf.currentChunk.activate()

	go pcf.dataReceiver()
	go pcf.dataExporter()

	pcf.isClosed = false

	return nil
}

func (pcf *ParallelChunkedFlow) dataReceiver() {
	for {
		select {
		case data := <-pcf.incoming:
			// Process input data
			pcf.dispatch(data)
		case <-pcf.closedDataReceiver:
			return
		case <-time.After(time.Millisecond):
			continue
		}
	}
}

func (pcf *ParallelChunkedFlow) dataExporter() {

	for cursor := pcf.cursor; ; cursor = cursor.Next() {
		//		log.Warn("entering ", cursor.Value.(*Chunk).id)
		//		log.Warn(cursor.Value.(*Chunk).len())

		pcf.cursor = cursor
		chunk := cursor.Value.(*Chunk)

		// Waiting for data from chunk
		for {

			// This chunk is empty and not activated, so switching to the next
			//if !chunk.isActive && chunk.isEmpty() {
			if !chunk.getIsactivateStatus() && chunk.isEmpty() {
				break
			}

			select {
			case data := <-chunk.Output():
				pcf.output <- data

				// update counter
				chunk.ack()
			case <-pcf.closedDataExporter:
				return
			case <-time.After(time.Millisecond):
				continue
			}
		}

		// No more data in this chunk so release it
		pcf.availableChunks <- cursor.Value.(*Chunk)

		//		log.Warn("done ", cursor.Value.(*Chunk).id)

	}
}

func (pcf *ParallelChunkedFlow) dispatch(data interface{}) {

	if pcf.isClosed {
		return
	}

	// Split data into equally sized chunks
	for {
		if pcf.currentChunk.push(data) {
			return
		}

		//		log.Info("full ", pcf.currentChunk.id)

		// Getting available chunk
		previousChunk := pcf.currentChunk
		pcf.currentChunk = <-pcf.availableChunks
		pcf.currentChunk.activate()
		previousChunk.deactivate()

		//		log.Error("next ", pcf.currentChunk.id)
	}
}

// Push will put data to the flow
func (pcf *ParallelChunkedFlow) Push(data interface{}) error {

	if pcf.isClosed {
		return errors.New("Flow is closed")
	}

	select {
	case pcf.incoming <- data:
		return nil
	default:
		return errors.New("Buffer is full")
	}
}

// Output will return a channel for receive proccessed data from flow
func (pcf *ParallelChunkedFlow) Output() chan interface{} {
	return pcf.output
}

// Close all goroutines
func (pcf *ParallelChunkedFlow) Close() {

	if pcf.isClosed {
		return
	}

	pcf.closedDataReceiver <- struct{}{}
	pcf.closedDataExporter <- struct{}{}

	pcf.chunks.Do(func(value interface{}) {
		value.(*Chunk).close()
	})
}
