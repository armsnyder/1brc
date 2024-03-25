package final

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"runtime"
	"sort"
	"sync"
)

// Solution is the main entrypoint to the problem. It takes a file as input
// and returns the station temperature summaries in the expected string format.
func Solution(fsys fs.FS, filename string) (string, error) {
	// 1. Split the file into chunks before reading.

	fileChunks, err := splitFile(fsys, filename, runtime.GOMAXPROCS(0))
	if err != nil {
		return "", err
	}

	// 2. Read and process each chunk in parallel.

	chunkedResults, err := getChunkedResults(fileChunks)
	if err != nil {
		return "", err
	}

	// 3. Combine the results.

	results := chunkedResults[0]
	for i := 1; i < len(chunkedResults); i++ {
		results.Fold(chunkedResults[i])
	}

	// 4. Format the results.

	return results.String(), nil
}

// FixedInt64 represents a large fixed-point number with 1 decimal place.
type FixedInt64 int64

// String formats the number with one decimal place.
func (f FixedInt64) String() string {
	return fmt.Sprintf("%0.1f", float64(f)/10)
}

// Divide divides the fixed-point number by an integer, rounding to one decimal
// point, towards positive infinity.
func (f FixedInt64) Divide(n int64) FixedInt64 {
	n2 := FixedInt64(n)

	q := f / n2
	r := f % n2

	if r < 0 {
		q--
		r += n2
	}

	if r*2 >= n2 {
		q++
	}

	return q
}

// FixedInt16 represents a small fixed-point number with 1 decimal place.
type FixedInt16 int16

// String formats the number with one decimal place.
func (f FixedInt16) String() string {
	return FixedInt64(f).String()
}

// Summary tracks the history of temperature measurements for a station.
type Summary struct {
	Station []byte     // Station name
	N       int64      // Number of measurements
	Total   FixedInt64 // Total temperature
	Min     FixedInt16 // Minimum temperature
	Max     FixedInt16 // Maximum temperature
}

// NewSummary creates a new summary for a station with an initial temperature
// measurement.
func NewSummary(station []byte, temperature FixedInt16) *Summary {
	stationCopy := make([]byte, len(station))
	copy(stationCopy, station)

	return &Summary{
		Station: stationCopy,
		Min:     temperature,
		Max:     temperature,
		Total:   FixedInt64(temperature),
		N:       1,
	}
}

// Add adds a temperature measurement to the summary.
func (s *Summary) Add(temperature FixedInt16) {
	s.Min = min(s.Min, temperature)
	s.Max = max(s.Max, temperature)
	s.Total += FixedInt64(temperature)
	s.N++
}

// HashMapBucketCount is the number of buckets in the custom hash map
// implementation.
const HashMapBucketCount = 1 << 17

// SummaryMap is an optimized hash map for storing summaries.
type SummaryMap struct {
	Buckets [HashMapBucketCount]*Summary // Buckets for storing summaries
	Size    int                          // Number of items in the map
}

// Add records a new temperature measurement for a station.
func (c *SummaryMap) Add(station []byte, hash uint32, temperature FixedInt16) error {
	hashIndex := int(hash & (HashMapBucketCount - 1))

	for {
		if c.Buckets[hashIndex] == nil {
			c.Buckets[hashIndex] = NewSummary(station, temperature)
			c.Size++
			if c.Size > HashMapBucketCount/2 {
				return errors.New("too many items")
			}
			return nil
		}

		if string(c.Buckets[hashIndex].Station) == string(station) {
			item := c.Buckets[hashIndex]
			item.Add(temperature)
			return nil
		}

		hashIndex = (hashIndex + 1) & (HashMapBucketCount - 1)
	}
}

// Results is a map of station names to summaries.
type Results map[string]*Summary

// Fold combines multiple results into a single result.
func (u Results) Fold(uu ...Results) {
	for _, u2 := range uu {
		for station, summary := range u2 {
			if r, ok := u[station]; ok {
				r.Min = min(r.Min, summary.Min)
				r.Max = max(r.Max, summary.Max)
				r.Total += summary.Total
				r.N += summary.N
			} else {
				u[station] = summary
			}
		}
	}
}

// String prints the results in the expected format.
func (u Results) String() string {
	summaries := make([]*Summary, 0, len(u))

	for _, summary := range u {
		summaries = append(summaries, summary)
	}

	sort.Slice(summaries, func(i, j int) bool {
		return string(summaries[i].Station) < string(summaries[j].Station)
	})

	var buf bytes.Buffer

	buf.WriteByte('{')

	for _, stationSummary := range summaries {
		buf.WriteString(fmt.Sprintf(
			"%s=%s/%s/%s, ",
			stationSummary.Station,
			stationSummary.Min,
			stationSummary.Total.Divide(stationSummary.N),
			stationSummary.Max,
		))
	}

	if len(summaries) > 0 {
		buf.Truncate(buf.Len() - 2)
	}

	buf.WriteByte('}')

	return buf.String()
}

// FileChunk represents a chunk of a file that ends at a newline.
type FileChunk struct {
	FS         fs.FS
	Filename   string
	StartIndex int64
	Size       int64
}

// getChunkedResults gathers station summaries for each file chunk.
func getChunkedResults(fileChunks []FileChunk) ([]Results, error) {
	chunkedResults := make([]Results, len(fileChunks))
	wg := &sync.WaitGroup{}
	errChan := make(chan error, 1)

	for i, fileChunk := range fileChunks {
		wg.Add(1)
		go func(i int, fileChunk FileChunk) {
			defer wg.Done()
			chunkedResult, err := readChunkForResults(fileChunk)
			if err != nil {
				select {
				case errChan <- err:
				default:
				}
				return
			}
			chunkedResults[i] = chunkedResult
		}(i, fileChunk)
	}

	wg.Wait()

	select {
	case err := <-errChan:
		return nil, err
	default:
		return chunkedResults, nil
	}
}

// splitFile divides a file into chunks that end at newlines.
func splitFile(fsys fs.FS, filename string, maxChunks int) ([]FileChunk, error) {
	file, err := fsys.Open(filename)
	if err != nil {
		return nil, err
	}

	seeker, ok := file.(io.Seeker)
	if !ok {
		return nil, fmt.Errorf("file does not support seeking")
	}

	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}

	const minChunkSize = 1 << 10

	fileSize := fileInfo.Size()
	chunkSize := max(minChunkSize, int64(math.Ceil(float64(fileSize)/float64(maxChunks))))
	lineBuf := make([]byte, 1<<7)
	fileChunks := make([]FileChunk, 0, int(math.Ceil(float64(fileSize)/float64(chunkSize))))

	var offset int64

	for offset < fileSize {
		fileChunk := FileChunk{
			FS:         fsys,
			Filename:   filename,
			StartIndex: int64(offset),
		}

		if offset+chunkSize > fileSize {
			fileChunk.Size = fileSize - offset
			fileChunks = append(fileChunks, fileChunk)
			break
		}

		_, err := seeker.Seek(offset+chunkSize, io.SeekStart)
		if err != nil {
			return nil, err
		}

		n, _ := io.ReadFull(file, lineBuf)
		chunk := lineBuf[:n]
		newlineIndex := bytes.IndexByte(chunk, '\n')

		if newlineIndex == -1 {
			return nil, fmt.Errorf("no newline found in chunk")
		}

		fileChunk.Size = chunkSize + int64(newlineIndex) + 1
		fileChunks = append(fileChunks, fileChunk)
		offset += fileChunk.Size
	}

	return fileChunks, nil
}

// readChunkForResults gets the station summaries for a file chunk.
func readChunkForResults(fileChunk FileChunk) (Results, error) {
	stationSummaryMap := &SummaryMap{}

	err := processChunkByLine(fileChunk, func(buf []byte) ([]byte, error) {
		return parseLineIntoSummaryMap(buf, stationSummaryMap)
	})
	if err != nil {
		return nil, err
	}

	results := make(Results)

	for _, item := range stationSummaryMap.Buckets {
		if item != nil {
			results[string(item.Station)] = item
		}
	}

	return results, nil
}

// parseLineIntoSummaryMap reads a line from a buffer and adds the temperature
// measurement to the summary map. It returns the remaining buffer.
func parseLineIntoSummaryMap(buf []byte, stationSummaryMap *SummaryMap) ([]byte, error) {
	stationName, hash, n, err := readStationName(buf)
	if err != nil {
		return nil, err
	}

	temperature, m, err := readTemperature(buf[n:])
	if err != nil {
		return nil, err
	}

	if err := stationSummaryMap.Add(stationName, hash, temperature); err != nil {
		return nil, err
	}

	return buf[n+m:], nil
}

// processChunkByLine reads a file chunk and processes each line with a worker.
// The lineWorker function should return the remaining buffer.
func processChunkByLine(fileChunk FileChunk, lineWorker func(buf []byte) ([]byte, error)) error {
	file, err := fileChunk.FS.Open(fileChunk.Filename)
	if err != nil {
		return err
	}

	defer file.Close()

	readerAt, ok := file.(io.ReaderAt)
	if !ok {
		return errors.New("file does not support reading at an offset")
	}

	reader := io.NewSectionReader(readerAt, fileChunk.StartIndex, fileChunk.Size)
	fileBuffer := make([]byte, 1<<20)
	offset := 0

	for {
		n, err := reader.Read(fileBuffer[offset:])
		n += offset
		if err != nil && err != io.EOF {
			return err
		}

		if n == 0 {
			break
		}

		lastNewline := bytes.LastIndexByte(fileBuffer[:n], '\n')

		buf := fileBuffer[:lastNewline+1]
		for len(buf) > 0 {
			buf, err = lineWorker(buf)
			if err != nil {
				return err
			}
		}

		copy(fileBuffer, fileBuffer[lastNewline+1:n])
		offset = n - lastNewline - 1
	}

	return nil
}

// readStationName reads a station name from a buffer and returns the number of
// bytes read.
//
// It computes the hash while reading the station name, as an optimization.
func readStationName(b []byte) (name []byte, hash uint32, n int, err error) {
	const (
		// FNV-1a constants.
		offset32 uint32 = 2166136261
		prime32  uint32 = 16777619
	)

	hash = offset32

	for i, ch := range b {
		if ch == ';' {
			return b[:i], hash, i + 1, nil
		}

		hash *= prime32
		hash ^= uint32(ch)
	}

	return nil, 0, 0, errors.New("invalid station name")
}

// readTemperature reads a temperature from a buffer and returns the number of
// bytes read.
func readTemperature(b []byte) (temperature FixedInt16, n int, err error) {
	for i, ch := range b {
		switch ch {
		case '-':
			continue
		case '.':
			temperature += FixedInt16(b[i+1] - '0')
			if b[0] == '-' {
				temperature *= -1
			}
			return temperature, i + 3, nil
		default:
			temperature += FixedInt16(ch - '0')
			temperature *= 10
		}
	}

	return 0, 0, errors.New("invalid temperature")
}
