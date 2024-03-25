package custommap

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
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

func Solution(reader io.Reader, writer io.Writer) error {
	workerCount := runtime.GOMAXPROCS(0)
	chunkChan := make(chan []byte, workerCount)
	out := make(chan Cities, workerCount)
	wg := &sync.WaitGroup{}

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			processChunks(chunkChan, out)
		}()
	}

	readChunked(reader, chunkChan)
	wg.Wait()
	close(out)

	cities := mergeCities(out)
	_, err := writer.Write([]byte(cities.String()))

	return err
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

func readChunked(reader io.Reader, chunks chan<- []byte) {
	defer close(chunks)

	var carryOver []byte

	for {
		chunk := BufferPool.Get().([]byte)[:BufferSize]
		copy(chunk, carryOver)
		n, err := reader.Read(chunk[len(carryOver):])
		n += len(carryOver)
		if n == 0 {
			BufferPool.Put(chunk)
			return
		}

		if err != nil {
			panic(err)
		}

		lastNewline := bytes.LastIndexByte(chunk[:n], '\n')

		if lastNewline == -1 {
			carryOver = carryOver[:0]
		} else {
			carryOver = append(carryOver[:0], chunk[lastNewline+1:n]...)
			n = lastNewline
		}

		chunks <- chunk[:n]
	}
}

func processChunks(chunkChan <-chan []byte, out chan<- Cities) {
	citiesMap := &CitiesMap{}

	for chunk := range chunkChan {
		scanner := bufio.NewScanner(bytes.NewReader(chunk))

		for scanner.Scan() {
			citiesMap.Add(scanner.Bytes())
		}

		if err := scanner.Err(); err != nil {
			panic(err)
		}

		BufferPool.Put(chunk)
	}

	cities := make(Cities)

	for _, item := range citiesMap.items {
		if item.Record != nil {
			cities[string(item.City)] = item.Record
		}
	}

	out <- cities
}

const BufferSize = 1 << 13

var BufferPool = sync.Pool{
	New: func() any {
		return make([]byte, BufferSize)
	},
}
