# The One Billion Row Challenge

## Overview

I tried out the [Billion Row Challenge](https://github.com/gunnarmorling/1brc)
using Golang. This is a challenge to read and process a one billion line text
file efficiently. It's a fun problem for practicing performance benchmarking
and optimization.

I've included several iterations on the challenge in the
[`/solutions`](https://github.com/armsnyder/1brc/tree/main/solutions)
directory. I attempted a few different optimizations before settling on
[`4_customscan`](https://github.com/armsnyder/1brc/blob/main/solutions/4_customscan/solution.go).
The hallmarks of this solution are:

1. Divide reading the file between multiple goroutines.
2. Optimize the file reading such that each byte is only read once.

I was able to achieve `2.97s` for 1 billion rows on a 2021 M1 Macbook Pro. For
context, reading this file without performing any processing takes `1.92s`.

## Benchmarks

```
go test -run=xxx -bench=Benchmark/data -benchmem -benchtime=10s
```

```
goos: darwin
goarch: arm64
pkg: 1brc
Benchmark/data/10m/baseline-10     596  19990858 ns/op    38 B/op        1 allocs/op
Benchmark/data/10m/solution/4-10   312  37987268 ns/op    21755002 B/op  16962 allocs/op
Benchmark/data/100m/baseline-10    60   201109167 ns/op   310 B/op       1 allocs/op
Benchmark/data/100m/solution/4-10  36   339840758 ns/op   21755636 B/op  16966 allocs/op
Benchmark/data/1b/baseline-10      6    1920414868 ns/op  1634 B/op      2 allocs/op
Benchmark/data/1b/solution/4-10    4    2968820927 ns/op  21753862 B/op  16958 allocs/op
PASS
ok  	1brc	92.076s
```
