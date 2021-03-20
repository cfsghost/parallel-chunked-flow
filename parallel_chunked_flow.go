package parallel_chunked_flow

import (
	"container/ring"
)

type ParallelChunkedFlow struct {
	closedDataReceiver chan struct{}
	closedDataExporter chan struct{}
	incoming           chan interface{}
	output             chan interface{}
	chunkSize          int
	chunks             *ring.Ring
	cursor             *ring.Ring

	availableChunks chan *Chunk
	currentChunk    *Chunk

	handler func(interface{}, chan interface{})
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

	go pcf.dataReceiver()
	go pcf.dataExporter()

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
		}
	}
}

func (pcf *ParallelChunkedFlow) dataExporter() {

	for cursor := pcf.cursor; ; cursor = cursor.Next() {

		select {
		case <-pcf.closedDataExporter:
			return
		default:
			break
		}
		//		log.Warn("entering ", cursor.Value.(*Chunk).id)
		//		log.Warn(cursor.Value.(*Chunk).len())

		pcf.cursor = cursor
		chunk := cursor.Value.(*Chunk)

		select {
		case pcf.output <- chunk.pop():
		case <-pcf.closedDataExporter:
			return
		}
		//		log.Info("<<")

		for chunk == pcf.currentChunk {
			select {
			case pcf.output <- chunk.pop():
			case <-pcf.closedDataExporter:
				return
			}
			//			log.Info("<<")
		}

		/*
			for !chunk.isEmpty() {
				pcf.output <- chunk.pop()
				//			log.Info("<<")
			}
		*/

		// No more data in this chunk
		cursor.Value.(*Chunk).start(pcf.chunkSize)
		pcf.availableChunks <- cursor.Value.(*Chunk)

		//		log.Warn("done ", cursor.Value.(*Chunk).id)

	}
}

func (pcf *ParallelChunkedFlow) dispatch(data interface{}) {

	if pcf.currentChunk == nil {
		pcf.currentChunk = <-pcf.availableChunks
	}

	// Split data into equally sized chunks
	for {
		if pcf.currentChunk.push(data) {
			return
		}

		//		log.Info("full ", pcf.currentChunk.id)

		err := pcf.currentChunk.close()
		if err != nil {
			continue
		}

		// Getting available chunk
		pcf.currentChunk = <-pcf.availableChunks

		//		log.Error("next ", pcf.currentChunk.id)
	}
}

// Push will put data to the flow
func (pcf *ParallelChunkedFlow) Push(data interface{}) error {
	pcf.incoming <- data
	return nil
}

// Output will return a channel for receive proccessed data from flow
func (pcf *ParallelChunkedFlow) Output() chan interface{} {
	return pcf.output
}

// Close all goroutines
func (pcf *ParallelChunkedFlow) Close() {
	pcf.closedDataReceiver <- struct{}{}
	pcf.closedDataExporter <- struct{}{}

	pcf.chunks.Do(func(value interface{}) {
		value.(*Chunk).close()
	})
}
