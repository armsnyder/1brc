package naivefixed

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"sort"
	"strconv"
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

type Cities map[string]Record

func (c Cities) Add(city string, measurement FixedNumber) {
	if record, ok := c[city]; ok {
		if measurement < record.Min {
			record.Min = measurement
		}

		if measurement > record.Max {
			record.Max = measurement
		}

		record.Total += measurement
		record.N++

		c[city] = record

		return
	}

	c[city] = Record{
		Min:   measurement,
		Max:   measurement,
		Total: measurement,
		N:     1,
	}
}

func (c Cities) String() string {
	type cityRecord struct {
		city   string
		record Record
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
	cities := make(Cities)
	scanner := bufio.NewScanner(reader)

	for scanner.Scan() {
		line := scanner.Bytes()
		semi := bytes.IndexByte(line, ';')
		city := string(line[:semi])
		measurement := ParseFixedNumber(line[semi+1:])

		cities.Add(city, measurement)
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	if _, err := writer.Write([]byte(cities.String())); err != nil {
		return err
	}

	return nil
}
