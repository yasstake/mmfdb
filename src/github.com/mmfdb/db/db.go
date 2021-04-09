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

type boardBuf struct {
	price uint32
	vol   uint16
}

func (board *Board) save(stream io.WriteCloser) {
	fmt.Print("depth=")
	fmt.Println(len(board.order))
	len := uint16(len(board.order))
	binary.Write(stream, binary.LittleEndian, &len)

	var buf boardBuf

	board.init()
	for price, _ := range board.order {
		buf.price = uint32(price)
		buf.vol = uint16(board.order[price])
		binary.Write(stream, binary.LittleEndian, &buf)
	}
}

func (board *Board) load(stream io.ReadCloser) {
	var len16 uint16
	binary.Read(stream, binary.LittleEndian, &len16)
	len := int(len16)

	var buf boardBuf

	board.init()
	for i := 0; i < len; i++ {
		binary.Read(stream, binary.LittleEndian, &buf)
		board.order[int(buf.price)] = int(buf.vol)
	}
}

type Record struct {
	time  int
	price int
	vol   int
}

type recordBuf struct {
	Time  uint16
	Price uint16
	Vol   uint16
}

func (c Record) save(stream io.WriteCloser, start_time int, start_price int) {
	var buf recordBuf

	buf.Time = uint16(c.time - start_time)
	buf.Price = uint16(c.price - start_price)
	buf.Vol = uint16(c.vol)
	binary.Write(stream, binary.LittleEndian, &buf)
}

func (c Record) load(stream io.ReadCloser, start_time int, start_price int) Record {
	buf := recordBuf{}
	binary.Read(stream, binary.LittleEndian, &buf)

	c.time = int(buf.Time) + start_time
	c.price = int(buf.Price) + start_price
	c.vol = int(buf.Vol)

	return c
}

type Records []Record

func (rec Records) save(stream io.WriteCloser) {
	start_time := uint32(rec[0].time)
	start_price := uint32(rec[0].price)

	binary.Write(stream, binary.LittleEndian, &start_time)
	binary.Write(stream, binary.LittleEndian, &start_price)

	len16 := uint16(len(rec))
	binary.Write(stream, binary.LittleEndian, &len16)

	for _, r := range rec {
		r.save(stream, int(start_time), int(start_price))
	}
}

func (rec Records) load(stream io.ReadCloser) Records {
	var start_time uint32
	var start_price uint32

	binary.Read(stream, binary.LittleEndian, &start_time)
	binary.Read(stream, binary.LittleEndian, &start_price)

	var len16 uint16
	binary.Read(stream, binary.LittleEndian, &len16)
	len := int(len16)

	var r Record

	for i := 0; i < len; i++ {
		r = r.load(stream, int(start_time), int(start_price))
		rec = append(rec, r)
	}
	return rec
}

type TransactionLog struct {
	start     int
	end       int
	bit       Board
	bit_delta Records
	ask       Board
	ask_delta Records
	buy       Records
	sell      Records
}

func (c *TransactionLog) reset() {
	c.init()
	c.start = 0
	c.end = 0
}

func (c *TransactionLog) init() {
	c.bit.init()
	c.ask.init()
}

func (c *TransactionLog) save(stream io.WriteCloser) {
	fmt.Println(c.start)
	fmt.Println(c.end)

	// start     int
	start := uint64(c.start)
	binary.Write(stream, binary.LittleEndian, &start)

	// end       int
	end := uint64(c.end)
	binary.Write(stream, binary.LittleEndian, &end)

	// bit       Board
	c.bit.save(stream)

	// bit_delta []Record
	len16 := int16(len(c.bit_delta))
	binary.Write(stream, binary.LittleEndian, &len16)

	c.bit_delta.save(stream)
	//binary.Write(stdoutDumper, binary.LittleEndian, &c.end)

	// ask       Board
	c.ask.save(stream)
	// ask_delta []Record
	c.ask_delta.save(stream)
	// buy       []Record
	c.buy.save(stream)
	// sell      []Record
	c.sell.save(stream)
}

type TransactionRecord struct {
	action int
	time   int
	seq    int
	price  int
	vol    int
}

func (rec TransactionRecord) to_string() string {
	s := strconv.Itoa(rec.action) + " " + strconv.Itoa(rec.time) + " " +
		strconv.Itoa(rec.seq) + " " + strconv.Itoa(rec.price) + " " + strconv.Itoa(rec.vol)
	return s
}

func (c *TransactionLog) load_binary() {

}

func (c TransactionLog) set(action int, time int, seq int, price int, vol int) {
	switch action {
	case PARTIAL:
		c.init()
		c.start = time
	case UPDATE_BUY:
		c.bit.set(price, vol)
	case UPDATE_SELL:
		c.ask.set(price, price)
	case TRADE_BUY:
		c.buy = append(c.buy, Record{time: time, price: price, vol: vol})
	case TRADE_SELL:
		c.sell = append(c.sell, Record{time: time, price: price, vol: vol})
	}
}

func load(file string) TransactionLog {
	f, err := os.Open(file)

	if err != nil {
		log.Fatal(err)
	}

	gzipfile, err := gzip.NewReader(f)

	r := csv.NewReader(gzipfile)

	var transaction TransactionLog
	transaction.init()

	lineno := 0

	for {
		row, err := r.Read()
		if err == io.EOF {
			break
		}

		var record TransactionRecord

		for i, v := range row {
			switch i {
			case 0: // Action
				record.action, _ = strconv.Atoi(v)
			case 1: // Time
				record.time, _ = strconv.Atoi(v)
			case 2: // Seq
				record.seq, _ = strconv.Atoi(v)
			case 3: // Price
				record.price, _ = strconv.Atoi(v)
			case 4: // volume
				record.vol, _ = strconv.Atoi(v)
			}
		}

		switch record.action {
		case PARTIAL:
			transaction.init()
		case UPDATE_BUY:
			transaction.bit.set(record.price, record.vol)
		case UPDATE_SELL:
			transaction.ask.set(record.price, record.price)
		}

		// fmt.Println(record.to_string() + "\n")
		lineno++
	}
	return transaction
}
