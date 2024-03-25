package main

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"testing"
)

func Test(t *testing.T) {
	tests := []struct {
		name   string
		fileFn func(t *testing.T, solveFn func(t *testing.T, txtFilename, outFilename string))
	}{
		{
			name:   "sample",
			fileFn: testForEachSample},
		{
			name:   "data",
			fileFn: testForEachDataFile,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.fileFn(t, func(t *testing.T, txtFilename, outFilename string) {
				testForEachSolution(t, func(t *testing.T, solution Solution) {
					testFile(t, solution, txtFilename, outFilename)
				})
			})
		})
	}
}

func testForEachDataFile(t *testing.T, solveFn func(t *testing.T, txtFilename, outFilename string)) {
	suffixes := []string{"10m", "100m", "1b"}

	for _, suffix := range suffixes {
		t.Run(suffix, func(t *testing.T) {
			txtFilename := fmt.Sprintf("measurements_%s.txt", suffix)
			outFilename := fmt.Sprintf("measurements_%s.out", suffix)

			if _, err := os.Stat(txtFilename); os.IsNotExist(err) {
				t.Skipf("no %s file", txtFilename)
			}

			if _, err := os.Stat(outFilename); os.IsNotExist(err) {
				t.Skipf("no %s file", outFilename)
			}

			solveFn(t, txtFilename, outFilename)
		})
	}
}

func testForEachSample(t *testing.T, solveFn func(t *testing.T, txtFilename, outFilename string)) {
	files, err := os.ReadDir("samples")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}
		if !strings.HasSuffix(file.Name(), ".txt") {
			continue
		}

		name := strings.TrimSuffix(file.Name(), ".txt")

		t.Run(name, func(t *testing.T) {
			solveFn(t, "samples/"+name+".txt", "samples/"+name+".out")
		})
	}
}

func testForEachSolution(t *testing.T, runFn func(t *testing.T, solution Solution)) {
	for i, solution := range Solutions {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			if i != len(Solutions)-1 && os.Getenv("ALL_SOLUTIONS") == "" {
				t.Skip("ALL_SOLUTIONS is not set")
			}

			runFn(t, solution)
		})
	}
}

func testFile(t *testing.T, solution Solution, txtFilename, outFilename string) {
	t.Helper()

	outFileBytes, err := os.ReadFile(outFilename)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := string(bytes.TrimSpace(outFileBytes))

	got, err := solution(os.DirFS("."), txtFilename)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}

	if expected != got {
		t.Errorf("\nexpected:\n  %q\n\ngot:\n  %q", expected, got)
	}
}
