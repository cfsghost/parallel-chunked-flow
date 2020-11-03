# parallel-chunked-flow

[![GoDoc](https://godoc.org/github.com/cfsghost/parallel-chunked-flow?status.svg)](http://godoc.org/github.com/cfsghost/parallel-chunked-flow)

Package parallel chunked flow provides the ability to craete parallel pipeline for sequential data.

## Benchmark

Here is result of benchmark with parsing JSON string:

```shell
$ go test -bench=.
goos: darwin
goarch: amd64
pkg: github.com/cfsghost/parallel-chunked-flow
BenchmarkBasic/Small-16                      	  721980	      1516 ns/op
BenchmarkBasic/Large-16                      	    9712	    110151 ns/op
BenchmarkChunkedFlowWithLowChunkCount/Small-16         	 2427859	       441 ns/op
BenchmarkChunkedFlowWithLowChunkCount/Large-16         	   52950	     31015 ns/op
BenchmarkChunkedFlowWithHighChunkCount/Small-16        	 2539164	       449 ns/op
BenchmarkChunkedFlowWithHighChunkCount/Large-16        	   54291	     26143 ns/op
PASS
ok  	github.com/cfsghost/parallel-chunked-flow	10.282s
```

## License
Licensed under the MIT License

## Authors
Copyright(c) 2020 Fred Chien <cfsghost@gmail.com>
