package parallel_chunked_flow

// Options represent all of the available options when creating a parallel chunked flow.
type Options struct {
	BufferSize int
	ChunkSize  int
	ChunkCount int
	Handler    func(interface{}) interface{}
}

// NewOptions creates a Options object.
func NewOptions() *Options {
	return &Options{
		BufferSize: 102400,
		ChunkSize:  1024,
		ChunkCount: 128,
		Handler: func(data interface{}) interface{} {
			return nil
		},
	}
}
