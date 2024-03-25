package parallelreaders

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/fs"
	"math"
	"runtime"
	"sort"
	"strconv"
	"sync"
)

// FixedNumber represents a fixed-point number with 1 decimal place.
type FixedNumber int

func ParseFixedNumber(b []byte) FixedNumber {
	dot := bytes.IndexByte(b, '.')
	whole, _ := strconv.Atoi(string(b[:dot]))
	dec, _ := strconv.Atoi(string(b[dot+1:]))

	if b[0] == '-' {
		dec *= -1
	}

	return FixedNumber(whole*10 + dec)
}

func (f FixedNumber) String() string {
	return fmt.Sprintf("%0.1f", float64(f)/10)
}

func (f FixedNumber) Divide(n int) FixedNumber {
	q := int(f) / n
	r := int(f) % n

	if r == 0 {
		return FixedNumber(q)
	}

	if r < 0 {
		q--
		r += n
	}

	if r*2 >= n {
		q++
	}

	return FixedNumber(q)
}

type Record struct {
	Min   FixedNumber
	Max   FixedNumber
	Total FixedNumber
	N     int
}

const BucketCount = 1 << 17

type CityRecord struct {
	City   []byte
	Record *Record
}

type CitiesMap struct {
	items [BucketCount]CityRecord
	size  int
}

func (c *CitiesMap) Add(line []byte) {
	const (
		// FNV-1a constants.
		offset64 uint64 = 14695981039346656037
		prime64  uint64 = 1099511628211
	)

	hash := offset64

	for i, ch := range line {
		if ch == ';' {
			city := line[:i]
			measurement := ParseFixedNumber(line[i+1:])
			hashIndex := int(hash & (BucketCount - 1))

			for {
				if c.items[hashIndex].Record == nil {
					cityCopy := make([]byte, len(city))
					copy(cityCopy, city)
					c.items[hashIndex] = CityRecord{
						City:   cityCopy,
						Record: &Record{Min: measurement, Max: measurement, Total: measurement, N: 1},
					}
					c.size++
					if c.size > BucketCount/2 {
						panic("too many items")
					}
					return
				}

				if bytes.Equal(c.items[hashIndex].City, city) {
					record := c.items[hashIndex].Record
					record.Min = min(record.Min, measurement)
					record.Max = max(record.Max, measurement)
					record.Total += measurement
					record.N++

					return
				}

				hashIndex = (hashIndex + 1) & (BucketCount - 1)
			}
		}

		hash ^= uint64(ch)
		hash *= prime64
	}

	panic("invalid line")
}

type Cities map[string]*Record

func (c Cities) String() string {
	type cityRecord struct {
		city   string
		record *Record
	}

	records := make([]cityRecord, 0, len(c))

	for city, record := range c {
		records = append(records, cityRecord{city, record})
	}

	sort.Slice(records, func(i, j int) bool {
		return records[i].city < records[j].city
	})

	var buf bytes.Buffer

	buf.WriteByte('{')

	for _, cityRecord := range records {
		buf.WriteString(fmt.Sprintf(
			"%s=%s/%s/%s, ",
			cityRecord.city,
			cityRecord.record.Min,
			cityRecord.record.Total.Divide(cityRecord.record.N),
			cityRecord.record.Max,
		))
	}

	if len(records) > 0 {
		buf.Truncate(buf.Len() - 2)
	}

	buf.WriteByte('}')

	return buf.String()
}

type ChunkDefinition struct {
	StartIndex int64
	Size       int64
}

func ChunkFile(fsys fs.FS, filename string, maxChunks int) ([]ChunkDefinition, error) {
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

	fileSize := fileInfo.Size()
	chunkSize := int64(math.Ceil(float64(fileSize) / float64(maxChunks)))
	lineBuf := make([]byte, 1<<7)
	chunkDefinitions := make([]ChunkDefinition, 0, maxChunks)

	var offset int64

	for offset < fileSize {
		chunkDefinition := ChunkDefinition{StartIndex: int64(offset)}

		if offset+chunkSize > fileSize {
			chunkDefinition.Size = fileSize - offset
			chunkDefinitions = append(chunkDefinitions, chunkDefinition)
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

		chunkDefinition.Size = chunkSize + int64(newlineIndex) + 1
		chunkDefinitions = append(chunkDefinitions, chunkDefinition)
		offset += chunkDefinition.Size
	}

	return chunkDefinitions, nil
}

func Solution(fsys fs.FS, filename string) (string, error) {
	chunkDefinitions, err := ChunkFile(fsys, filename, runtime.GOMAXPROCS(0))
	if err != nil {
		return "", err
	}

	out := make(chan Cities, len(chunkDefinitions))
	wg := &sync.WaitGroup{}

	for _, chunkDefinition := range chunkDefinitions {
		wg.Add(1)
		go func(chunkDefinition ChunkDefinition) {
			defer wg.Done()
			out <- processChunk(fsys, filename, chunkDefinition)
		}(chunkDefinition)
	}

	wg.Wait()
	close(out)

	cities := mergeCities(out)

	return cities.String(), nil
}

func mergeCities(cities <-chan Cities) Cities {
	result := make(Cities)

	for c := range cities {
		for city, record := range c {
			if r, ok := result[city]; ok {
				if record.Min < r.Min {
					r.Min = record.Min
				}

				if record.Max > r.Max {
					r.Max = record.Max
				}

				r.Total += record.Total
				r.N += record.N

				result[city] = r
			} else {
				result[city] = record
			}
		}
	}

	return result
}

func processChunk(fsys fs.FS, filename string, chunkDefinition ChunkDefinition) Cities {
	citiesMap := &CitiesMap{}

	file, err := fsys.Open(filename)
	if err != nil {
		panic(err)
	}

	defer file.Close()

	readerAt, ok := file.(io.ReaderAt)
	if !ok {
		panic("file does not support reading at")
	}

	scanner := bufio.NewScanner(io.NewSectionReader(readerAt, chunkDefinition.StartIndex, chunkDefinition.Size))

	for scanner.Scan() {
		citiesMap.Add(scanner.Bytes())
	}

	if err := scanner.Err(); err != nil {
		panic(err)
	}

	cities := make(Cities)

	for _, item := range citiesMap.items {
		if item.Record != nil {
			cities[string(item.City)] = item.Record
		}
	}

	return cities
}
