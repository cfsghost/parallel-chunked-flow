/*
Package parallel chunked flow provides the ability to craete parallel pipeline for sequential data.
Here is example to create a flow:
	// Create Options object
	options := &parallel_chunked_flow.Options{
			BufferSize: 1024000,
			ChunkSize:  1024,
			ChunkCount: 128,
			Handler: func(data interface{}, output chan interface{}) {
				output <- data.(int) + 1
			},
	}

	// Create flow with options
	flow := parallel_chunked_flow.NewParallelChunkedFlow(options)

	// Push data into flow
	flow.Push(1)

	// Getting result
	result := <-flow.Output()
*/

package parallel_chunked_flow
