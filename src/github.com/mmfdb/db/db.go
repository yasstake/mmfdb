package db

import (
	"compress/gzip"
	"encoding/binary"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

// Action
const PARTIAL = 1
const UPDATE_SELL = 2
const UPDATE_BUY = 3

// trade
const TRADE_BUY = 4
const TRADE_BUY_LIQUID = 5

const TRADE_SELL = 6
const TRADE_SELL_LIQUID = 7

// Open Interest
// action, time, 0,, volume,
const OPEN_INTEREST = 10

// funding rate
// action, time(next funding), 0, rate,, checksum
const FUNDING_RATE = 11

var DB_ROOT = "./LOG"

func open_gzip(file string) io.ReadCloser {
	f, err := os.Open(file)
	if err != nil {
		log.Fatal(err)
	}
	gzipreader, _ := gzip.NewReader(f)

	return gzipreader
}

type Board struct {
	order map[int]int
}

func (c *Board) init() {
	c.order = make(map[int]int)
}

func (board *Board) set(price int, volume int) {
	if volume == 0 {
		delete(board.order, price)
	} else {
		board.order[price] = volume
	}
}

func (board Board) copy() Board {
	var copy_board Board

	copy_board.order = make(map[int]int)

	for key, value := range board.order {
		copy_board.order[key] = value
	}

	return copy_board
}

type boardBuf struct {
	Price int16
	Vol   uint32
}

func (board Board) save(stream io.WriteCloser, start_price int64) {
	length := uint16(len(board.order))
	binary.Write(stream, binary.LittleEndian, &length)

	var buf boardBuf

	for price, _ := range board.order {
		buf.Price = int16(int64(price) - start_price)
		buf.Vol = uint32(board.order[price])
		binary.Write(stream, binary.LittleEndian, &buf)
	}
}

func (board Board) load(stream io.ReadCloser, start_price int64) Board {
	var len16 uint16
	binary.Read(stream, binary.LittleEndian, &len16)
	len := int(len16)

	var buf boardBuf

	board.init()
	for i := 0; i < len; i++ {
		binary.Read(stream, binary.LittleEndian, &buf)
		board.order[int(int64(buf.Price)+int64(start_price))] = int(buf.Vol)
	}
	return board
}

func (board Board) depth() int {
	return len(board.order)
}

type Record struct {
	time  TimeMs
	price uint64
	vol   uint64
}

type recordBuf struct {
	Time  uint16
	Price int16
	Vol   uint32
}

func (c Record) save(stream io.WriteCloser, start_time TimeMs, start_price int64) {
	var buf recordBuf

	buf.Time = uint16(c.time - start_time)
	buf.Price = int16(int64(c.price) - int64(start_price))
	buf.Vol = uint32(c.vol)
	binary.Write(stream, binary.LittleEndian, &buf)
}

func (c Record) load(stream io.ReadCloser, start_time TimeMs, start_price int64) Record {
	buf := recordBuf{}
	binary.Read(stream, binary.LittleEndian, &buf)

	c.time = TimeMs(uint64(buf.Time) + uint64(start_time))
	c.price = uint64(int64(buf.Price) + int64(start_price))
	c.vol = uint64(buf.Vol)

	return c
}

type Records []Record

func (rec Records) save(stream io.WriteCloser, start_time TimeMs, start_price int64) {
	len16 := uint16(len(rec))
	binary.Write(stream, binary.LittleEndian, &len16)

	for _, r := range rec {
		r.save(stream, start_time, int64(start_price))
	}
}

func (rec Records) load(stream io.ReadCloser, start_time TimeMs, start_price int64) Records {
	var len16 uint16
	binary.Read(stream, binary.LittleEndian, &len16)
	len := int(len16)

	var r Record
	rec = make(Records, len)

	for i := 0; i < len; i++ {
		r = r.load(stream, start_time, start_price)
		rec[i] = r
	}
	return rec
}

func (c *Records) init() {
	*c = make(Records, 0)
}

type TransactionLog struct {
	start_time   TimeMs
	end_time     TimeMs
	start_price  int64
	bit          Board
	bit_start    Board
	bit_delta    Records
	ask          Board
	ask_start    Board
	ask_delta    Records
	buy          Records
	sell         Records
	current_time TimeMs
}

func (c *TransactionLog) init() {
	c.bit.init()
	c.bit_start.init()
	c.ask.init()
	c.ask_start.init()
	c.start_time = 0
	c.end_time = 0
	c.start_price = 0
	c.current_time = 0
}

func (c *TransactionLog) reset() {
	c.start_time = 0
	c.end_time = 0
	c.start_price = 0
	c.bit_delta.init()
	c.ask_delta.init()
	c.bit_start = c.bit.copy()
	c.ask_start = c.ask.copy()
}

func (c *TransactionLog) save(stream io.WriteCloser) {
	// start_time     int
	start := uint64(c.start_time)
	binary.Write(stream, binary.LittleEndian, &start)
	// start_price       int
	start_price := uint64(c.start_price)
	binary.Write(stream, binary.LittleEndian, &start_price)
	// bit       Board
	c.bit_start.save(stream, c.start_price)
	// bit_delta []Record
	c.bit_delta.save(stream, c.start_time, c.start_price)
	// ask       Board
	c.ask_start.save(stream, c.start_price)
	// ask_delta []Record
	c.ask_delta.save(stream, c.start_time, c.start_price)
	// buy       []Record
	c.buy.save(stream, c.start_time, c.start_price)
	// sell      []Record
	c.sell.save(stream, c.start_time, c.start_price)
}

func (c TransactionLog) dump_to_directory(basepath string) {
	dir_name, file_name := make_path(c.start_time)

	dir_path := basepath + string(os.PathSeparator) + dir_name
	os.MkdirAll(dir_path, 0777)

	fw, _ := os.Create(dir_path + string(os.PathSeparator) + file_name)
	defer fw.Close()
	gw := gzip.NewWriter(fw)
	defer gw.Flush()
	defer gw.Close()
	c.save(gw)
}

func (c TransactionLog) load(stream io.ReadCloser) TransactionLog {
	// start_time     int
	var start_time uint64
	binary.Read(stream, binary.LittleEndian, &start_time)
	c.start_time = TimeMs(start_time)

	// start_price       int
	start_price := uint64(c.start_price)
	binary.Read(stream, binary.LittleEndian, &start_price)
	c.start_price = int64(start_price)

	// bit       Board
	c.bit = c.bit.load(stream, c.start_price)

	// bit_delta []Record
	c.bit_delta = c.bit_delta.load(stream, c.start_time, c.start_price)

	// ask       Board
	c.ask = c.ask.load(stream, c.start_price)
	// ask_delta []Record
	c.ask_delta = c.ask_delta.load(stream, c.start_time, c.start_price)
	// buy       []Record
	c.buy = c.buy.load(stream, c.start_time, c.start_price)
	// sell      []Record
	c.sell = c.sell.load(stream, c.start_time, c.start_price)

	return c
}

type TransactionRecord struct {
	action int
	time   TimeMs
	seq    int
	price  int
	vol    int
}

func (rec TransactionRecord) to_string() string {
	s := strconv.Itoa(rec.action) + " " + /*strconv.Itoa(rec.time) + */ " " +
		strconv.Itoa(rec.seq) + " " + strconv.Itoa(rec.price) + " " + strconv.Itoa(rec.vol)
	return s
}

func (c *TransactionLog) set(action int, time TimeMs, seq int, price uint64, vol uint64) {
	if c.start_time == 0 {
		c.start_time = time
	}
	c.end_time = time

	switch action {
	case PARTIAL:
		fmt.Println("----PARTIAL-------")
		c.bit.init()
		c.ask.init()
	case UPDATE_BUY:
		c.bit.set(int(price), int(vol))
		c.bit_delta = append(c.bit_delta, Record{time: time, price: price, vol: vol})
	case UPDATE_SELL:
		c.ask.set(int(price), int(vol))
		c.ask_delta = append(c.ask_delta, Record{time: time, price: price, vol: vol})
	case TRADE_BUY:
		c.buy = append(c.buy, Record{time: time, price: price, vol: vol})
	case TRADE_BUY_LIQUID:
		c.buy = append(c.buy, Record{time: time, price: price, vol: vol})
	case TRADE_SELL:
		c.sell = append(c.sell, Record{time: time, price: price, vol: vol})
	case TRADE_SELL_LIQUID:
		c.sell = append(c.sell, Record{time: time, price: price, vol: vol})
	}

}

/*
func load_log(file string) {
	var transaction TransactionLog

	wf, _ := os.Open(file)

	if strings.HasSuffix(file, ".gz") {
		wf, _ := gzip.NewReader(wf)
	}

	transaction = transaction.load(wf)
}
*/

func load_log(file string) TransactionLog {
	compress := strings.HasSuffix(file, ".gz")

	f, err := os.Open(file)

	if err != nil {
		log.Fatal(err)
	}

	var r *csv.Reader
	if compress {
		gzipfile, _ := gzip.NewReader(f)
		r = csv.NewReader(gzipfile)
	} else {
		r = csv.NewReader(f)
	}

	var transaction TransactionLog
	transaction.init()

	var record TransactionRecord
	last_min := -1

	for {
		row, err := r.Read()
		if err == io.EOF {
			break
		}
		for i, v := range row {
			switch i {
			case 0: // Action
				record.action, _ = strconv.Atoi(v)
			case 1: // Time
				t, _ := strconv.ParseUint(v, 10, 64)
				record.time = TimeMs(t / 1000) // convert ns to ms
			case 2: // Seq
				record.seq, _ = strconv.Atoi(v)
			case 3: // Price
				record.price, _ = strconv.Atoi(v)
			case 4: // volume
				record.vol, _ = strconv.Atoi(v)
			}
		}

		if record.action == UPDATE_BUY /*|| record.action == UPDATE_SELL*/ {
			//min := int64(record.time / 1000)
			//min = int64(min / 60)
			hour, min, sec := record.time.HHMMSS()

			if min != last_min {
				if last_min != -1 && sec == 0 {
					_, _, ss := transaction.start_time.HHMMSS()
					if ss < 1 {
						transaction.dump_to_directory(DB_ROOT) // Save
					}
				}
				transaction.reset()
				last_min = min

				fmt.Println(hour, min, sec, record.time.str(), row)
			}
		}

		transaction.set(record.action, record.time, record.seq, uint64(record.price), uint64(record.vol))
	}

	transaction.start_price = int64(record.price)

	fmt.Println(transaction.start_time)
	fmt.Println(record.time)
	fmt.Println(record.time - transaction.start_time)
	fmt.Println(uint32(2 * 2 * 2 * 2 * 2 * 2 * 2 * 2 * 2 * 2 * 2 * 2 * 2 * 2 * 2 * 2))

	sec := int64(transaction.start_time / 1000)
	s := time.Unix(sec, int64(transaction.start_time)-sec*1000000)
	fmt.Println(s)

	sec = int64(record.time / 1000)
	s = time.Unix(sec, int64(record.time)-sec*1000000)
	fmt.Println(s)

	return transaction
}
