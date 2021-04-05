package db

import (
	"compress/gzip"
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

func (board *Board) init() {
	board.order = make(map[int]int)
}

func (board *Board) set(price int, volume int) {
	if volume == 0 {
		delete(board.order, price)
	} else {
		board.order[price] = volume
	}
}

func (board Board) dump() {
	fmt.Print("depth=")
	fmt.Println(len(board.order))

	for price, _ := range board.order {
		fmt.Println(price, board.order[price])
	}
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

type Record struct {
	time  int
	price int
	vol   int
}

type TransactionLog struct {
	start     int
	end       int
	bit       Board
	bit_delta []Record
	ask       Board
	ask_delta []Record
	buy       []Record
	sell      []Record
}

func (c TransactionLog) save_binary() {

}

func (c TransactionLog) load_binary() {

}

func (c TransactionLog) set(action int, time int, seq int, price int, vol int) {
	switch action {
	case PARTIAL:
		c.bit.init()
		c.ask.init()
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

func load(file string) (bit Board, ask Board) {
	f, err := os.Open(file)

	if err != nil {
		log.Fatal(err)
	}

	gzipfile, err := gzip.NewReader(f)

	r := csv.NewReader(gzipfile)

	lineno := 0
	var bit_board, ask_board Board
	bit_board.init()
	ask_board.init()

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
			bit_board.init()
			ask_board.init()
		case UPDATE_BUY:
			bit_board.set(record.price, record.vol)
		case UPDATE_SELL:
			ask_board.set(record.price, record.price)
		}

		// fmt.Println(record.to_string() + "\n")
		lineno++
	}

	return bit_board, ask_board
}
