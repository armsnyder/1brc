package main

import (
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
)

func Benchmark(b *testing.B) {
	tests := []struct {
		name   string
		fileFn func(b *testing.B, benchmarkFn func(b *testing.B, txtFilename string))
	}{
		{
			name:   "sample",
			fileFn: benchmarkForEachSample,
		},
		{
			name:   "data",
			fileFn: benchmarkForEachDataFile,
		},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			tt.fileFn(b, func(b *testing.B, txtFilename string) {
				b.Run("baseline", func(b *testing.B) {
					benchmarkFileBaseline(b, txtFilename)
				})

				b.Run("solution", func(b *testing.B) {
					benchmarkForEachSolution(b, func(b *testing.B, solution Solution) {
						benchmarkFileSolution(b, txtFilename, solution)
					})
				})
			})
		})
	}
}

func benchmarkForEachDataFile(b *testing.B, benchmarkFn func(b *testing.B, txtFilename string)) {
	suffixes := []string{"10m", "100m", "1b"}

	for _, suffix := range suffixes {
		b.Run(suffix, func(b *testing.B) {
			txtFilename := fmt.Sprintf("measurements_%s.txt", suffix)

			if _, err := os.Stat(txtFilename); os.IsNotExist(err) {
				b.Skipf("no %s file", txtFilename)
			}

			benchmarkFn(b, txtFilename)
		})
	}
}

func benchmarkForEachSample(b *testing.B, benchmarkFn func(b *testing.B, txtFilename string)) {
	files, err := os.ReadDir("samples")
	if err != nil {
		b.Fatalf("unexpected error: %v", err)
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}
		if !strings.HasSuffix(file.Name(), ".txt") {
			continue
		}

		name := strings.TrimPrefix(strings.TrimSuffix(file.Name(), ".txt"), "measurements-")

		b.Run(name, func(b *testing.B) {
			benchmarkFn(b, "samples/"+file.Name())
		})
	}
}

func benchmarkForEachSolution(b *testing.B, runFn func(b *testing.B, solution Solution)) {
	for i, solution := range Solutions {
		b.Run(fmt.Sprintf("%d", i), func(b *testing.B) {
			if i != len(Solutions)-1 && os.Getenv("ALL_SOLUTIONS") == "" {
				b.Skip("ALL_SOLUTIONS is not set")
			}

			runFn(b, solution)
		})
	}
}

func benchmarkFileSolution(b *testing.B, txtFilename string, solution Solution) {
	fsys := os.DirFS(".")

	for n := 0; n < b.N; n++ {
		_, err := solution(fsys, txtFilename)
		if err != nil {
			b.Fatalf("unexpected error: %v", err)
		}
	}
}

func benchmarkFileBaseline(b *testing.B, txtFilename string) {
	f, err := os.Open(txtFilename)
	if err != nil {
		b.Fatalf("unexpected error: %v", err)
	}

	defer f.Close()

	for n := 0; n < b.N; n++ {
		_, err = io.Copy(io.Discard, f)
		if err != nil {
			b.Fatalf("unexpected error: %v", err)
		}

		_, err = f.Seek(0, io.SeekStart)
		if err != nil {
			b.Fatalf("unexpected error: %v", err)
		}
	}
}
