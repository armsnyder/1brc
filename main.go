package main

import (
	naive "1brc/solutions/0_naive"
	chunkreduce "1brc/solutions/1_chunkreduce"
	custommap "1brc/solutions/2_custommap"
	parallelreaders "1brc/solutions/3_parallelreaders"
	customscan "1brc/solutions/4_customscan"
	"bytes"
	"runtime"

	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"runtime/pprof"
	"time"
)

type Solution func(fsys fs.FS, filename string) (string, error)

type ReaderSolution func(reader io.Reader, writer io.Writer) error

func asSolution(s ReaderSolution) Solution {
	return func(fsys fs.FS, filename string) (string, error) {
		file, err := fsys.Open(filename)
		if err != nil {
			return "", err
		}
		defer file.Close()

		writer := &bytes.Buffer{}

		err = s(file, writer)
		if err != nil {
			return "", err
		}

		return writer.String(), nil
	}
}

var Solutions = []Solution{
	asSolution(naive.Solution),
	asSolution(chunkreduce.Solution),
	asSolution(custommap.Solution),
	parallelreaders.Solution,
	customscan.Solution,
}

func main() {
	cpuProfile, err := os.Create("cpuprofile")
	if err != nil {
		log.Fatal(err)
	}
	defer cpuProfile.Close()

	memProfile, err := os.Create("memprofile")
	if err != nil {
		log.Fatal(err)
	}
	defer memProfile.Close()

	runtime.MemProfileRate = 1

	if err := pprof.StartCPUProfile(cpuProfile); err != nil {
		log.Fatal(err)
	}
	defer pprof.StopCPUProfile()

	fsys := os.DirFS(".")
	for i := 0; i < 3; i++ {
		start := time.Now()
		_, err = customscan.Solution(fsys, "measurements_1b.txt")
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(time.Since(start))
	}

	runtime.GC()
	if err := pprof.WriteHeapProfile(memProfile); err != nil {
		log.Fatal(err)
	}
}
