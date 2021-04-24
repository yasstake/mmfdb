package trans

import (
	"compress/gzip"
	"encoding/binary"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
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

const DB_ROOT = "/tmp/BITLOG"

const SEC_IN_NS = 1_000_000_000 // ns = sec

func date_time(nsec int64) time.Time {
	t := time.Unix(0, nsec).UTC()
	return t
}

// make log file path from Time
func make_path(time time.Time) (dir, path string) {
	yy := time.Year()
	mm := time.Month()
	dd := time.Day()
	h := time.Hour()
	m := time.Minute()

	dir_name := fmt.Sprintf("%04d-%02d-%02d", yy, mm, dd) + string(os.PathSeparator)
	file_name := fmt.Sprintf("%02d-%02d.log.gz", h, m)

	return dir_name, file_name
}

func make_full_path(time time.Time) (path string) {
	dir_name, file_name := make_path(time)
	return filepath.Join(DB_ROOT, dir_name, file_name)
}

func create_db_file(time time.Time) (db_file io.WriteCloser) {
	dir_name, file_name := make_path(time)

	dir_path := filepath.Join(DB_ROOT, dir_name)
	os.MkdirAll(dir_path, 0777)

	db_path := filepath.Join(dir_path, file_name)
	fw, _ := os.Create(db_path)
	gw := gzip.NewWriter(fw)
	return gw
}

// parse file name and return timestamp in TimeMs
// reverse function of make_path
func file_to_time(file_path string) time.Time {
	// path_exp := `(\d{4})-(\d{2})-(\d{2})` + os.PathSeparator + `(\d{2})-(\d{2}).log.gz$`
	//re := regexp.MustCompile(path_exp)
	re := regexp.MustCompile(`(\d{4})-(\d{2})-(\d{2})/(\d{2})-(\d{2}).log.gz$`)
	res := re.FindStringSubmatch(file_path)

	yy, _ := strconv.Atoi(res[1])
	mm, _ := strconv.Atoi(res[2])
	dd, _ := strconv.Atoi(res[3])
	h, _ := strconv.Atoi(res[4])
	m, _ := strconv.Atoi(res[5])

	return time.Date(yy, time.Month(mm), dd, h, m, 0, 0, time.UTC)
}

type Transaction struct {
	Action     int8
	Time_stamp int64
	Price      int32
	Volume     int32
}

func (c *Transaction) info_string() (result string) {
	result += date_time(c.Time_stamp).String()
	result += "{Action:" + strconv.Itoa(int(c.Action)) + "}"
	result += "{Price:" + strconv.Itoa(int(c.Price)) + "}"
	result += "{vol:" + strconv.Itoa(int(c.Volume)) + "}"

	return result
}

func (t *Transaction) save(stream io.WriteCloser) {
	binary.Write(stream, binary.LittleEndian, t)
}

func (t *Transaction) load(stream io.ReadCloser) Transaction {
	binary.Read(stream, binary.LittleEndian, t)
	return *t
}

type Transactions []Transaction

func (t *Transactions) init() {
	*t = make(Transactions, 0, 1000)
}

func (t Transactions) save(stream io.WriteCloser) {
	length := int32(len(t))
	binary.Write(stream, binary.LittleEndian, &length)
	for i := 0; i < int(length); i++ {
		t[i].save(stream)
	}
}

func (t *Transactions) load(stream io.ReadCloser) Transactions {
	var length int32
	binary.Read(stream, binary.LittleEndian, &length)

	re := make(Transactions, length)

	for i := 0; i < int(length); i++ {
		re[i] = re[i].load(stream)
	}
	*t = re

	return *t
}

type Board map[int]int

func (c *Board) init() {
	*c = make(Board)
}

func (board *Board) set(price int, volume int) {
	if volume == 0 {
		delete(*board, price)
	} else {
		(*board)[price] = volume
	}
}

func (board *Board) copy() Board {
	var copy_board Board

	copy_board = make(Board)

	for key, value := range *board {
		copy_board[key] = value
	}

	return copy_board
}

type boardBuf struct {
	Price uint32
	Vol   uint32
}

func (board *Board) save(stream io.WriteCloser) {
	length := uint16(len(*board))
	binary.Write(stream, binary.LittleEndian, &length)

	var buf boardBuf

	for price, _ := range *board {
		buf.Price = uint32(price)
		buf.Vol = uint32((*board)[price])
		binary.Write(stream, binary.LittleEndian, &buf)
	}
}

func (board *Board) load(stream io.ReadCloser) Board {
	var len16 uint16
	binary.Read(stream, binary.LittleEndian, &len16)
	len := int(len16)

	var buf boardBuf

	board.init()

	for i := 0; i < len; i++ {
		binary.Read(stream, binary.LittleEndian, &buf)
		(*board)[int(buf.Price)] = int(buf.Vol)
	}

	return *board
}

func (board Board) depth() int {
	return len(board)
}

type Chunk struct {
	bit_board Board
	ask_board Board
	trans     Transactions
}

func (c Chunk) info_string() string {
	bit_len := len(c.bit_board)
	ask_len := len(c.ask_board)
	trans_len := len(c.trans)

	start := c.trans[0]
	end := c.trans[trans_len-1]

	result :=
		"Start->" + start.info_string() +
			" End->" + end.info_string() +
			" BIT->" + strconv.Itoa(bit_len) +
			" ASK->" + strconv.Itoa(ask_len) +
			" Trans->" + strconv.Itoa(trans_len)

	return result
}

func (c *Chunk) init() {
	c.bit_board.init()
	c.ask_board.init()
	c.trans.init()
}

func (c *Chunk) append(r Transaction) {
	c.trans = append(c.trans, r)
}

func (c *Chunk) dump() {
	time := date_time(c.trans[0].Time_stamp)
	stream := create_db_file(time)
	defer stream.Close()

	c.bit_board.save(stream)
	c.ask_board.save(stream)
	c.trans.save(stream)
}

func (c *Chunk) load_file(path string) {
	stream, err := os.Open(path)
	if err != nil {
		log.Fatal("cannot open file", path, err)
	}

	gzip_reader, _ := gzip.NewReader(stream)
	defer gzip_reader.Close()

	c.bit_board.load(gzip_reader)
	c.ask_board.load(gzip_reader)
	c.trans.load(gzip_reader)
}

func (c *Chunk) load_time(time time.Time) {
	path := make_full_path(time)
	c.load_file(path)
}

type Ohlcv struct {
	open     int
	high     int
	low      int
	close    int
	buy_vol  int
	sell_vol int
	vol      int
}

func (c *Ohlcv) init() {
	c.open = 0
	c.high = 0
	c.low = 0
	c.close = 0
	c.buy_vol = 0
	c.sell_vol = 0
	c.vol = 0
}

func (c *Ohlcv) add(ohlcv Ohlcv) (result Ohlcv) {
	result.open = c.open

	if c.high < ohlcv.high {
		result.high = ohlcv.high
	} else {
		result.high = c.high
	}

	if c.low < ohlcv.low {
		result.low = c.low
	} else {
		result.low = ohlcv.low
	}

	result.close = ohlcv.close

	result.buy_vol += c.buy_vol + ohlcv.buy_vol
	result.sell_vol += c.sell_vol + ohlcv.sell_vol

	result.vol = result.buy_vol + result.sell_vol

	return result
}

func (c *Ohlcv) buy(price int, volume int) {
	c.sell_buy(price, volume, true)
}

func (c *Ohlcv) sell(price int, volume int) {
	c.sell_buy(price, volume, false)
}

func (c *Ohlcv) sell_buy(price int, volume int, buy bool) {
	if c.open == 0 {
		c.open = price
	}

	if c.high < price || c.high == 0 {
		c.high = price
	}

	if price < c.low || c.low == 0 {
		c.low = price
	}

	c.close = price

	if buy {
		c.buy_vol += volume
		c.vol += volume
	} else {
		c.sell_vol += volume
		c.vol += volume
	}
}

func (c *Chunk) ohlcv(from time.Time, end time.Time) (result Ohlcv, err bool) {
	result.init()

	for i, _ := range c.trans {

		time_stamp := date_time(c.trans[i].Time_stamp)

		if time_stamp.Before(from) {
			continue
		}

		if end.Before(time_stamp) {
			break
		}

		action := c.trans[i].Action

		if action == TRADE_BUY || action == TRADE_BUY_LIQUID {
			result.buy(int(c.trans[i].Price), int(c.trans[i].Volume))
		} else if action == TRADE_SELL || action == TRADE_SELL_LIQUID {
			result.sell(int(c.trans[i].Price), int(c.trans[i].Volume))
		}
	}

	if result.open == 0 {
		err = true
	}

	return result, err
}

func (c *Chunk) ohlcvSec() (result []Ohlcv) {
	start_time := int64((c.trans[0].Time_stamp+SEC_IN_NS/10)/SEC_IN_NS) * SEC_IN_NS
	current_end := start_time + SEC_IN_NS

	var ohlcv Ohlcv
	trans_len := len(c.trans)

	for i := 0; i < trans_len; i++ {
		time_stamp := c.trans[i].Time_stamp

		if time_stamp <= current_end || trans_len-i < 100 {
			action := c.trans[i].Action

			if action == TRADE_BUY || action == TRADE_BUY_LIQUID {
				ohlcv.buy(int(c.trans[i].Price), int(c.trans[i].Volume))
			} else if action == TRADE_SELL || action == TRADE_SELL_LIQUID {
				ohlcv.sell(int(c.trans[i].Price), int(c.trans[i].Volume))
			}
		} else {
			current_end += SEC_IN_NS
			result = append(result, ohlcv)
			ohlcv.init()
		}
	}

	return result
}

func load_log(file string) (chunk Chunk) {
	f, err := os.Open(file)
	if err != nil {
		log.Fatal(err)
	}

	compress := strings.HasSuffix(file, ".gz")
	var r *csv.Reader
	if compress {
		gzipfile, _ := gzip.NewReader(f)
		r = csv.NewReader(gzipfile)
	} else {
		r = csv.NewReader(f)
	}

	var record Transaction

	last_min := int(-1)

	var bit_board Board
	bit_board.init()
	var ask_board Board
	ask_board.init()

	chunk.init()

	for {
		row, err := r.Read()
		if err == io.EOF {
			break
		}
		for i, v := range row {
			switch i {
			case 0: // Action
				r, _ := strconv.Atoi(v)
				record.Action = int8(r)
			case 1: // Time(us)
				t, _ := strconv.ParseInt(v, 10, 64)
				record.Time_stamp = t * 1000 // convert to ns
			/*
				case 2: // Seq
					record.seq, _ = strconv.Atoi(v)
			*/
			case 3: // Price
				r, _ := strconv.Atoi(v)
				record.Price = int32(r)
			case 4: // volume
				// TODO: FIX omit under floating point
				r, _ := strconv.ParseFloat(v, 64)
				record.Volume = int32(r)
			}
		}

		if record.Action == PARTIAL {
			bit_board.init()
			ask_board.init()
		} else if record.Action == UPDATE_BUY || record.Action == UPDATE_SELL {
			time := date_time(record.Time_stamp)
			min := time.Minute()
			sec := time.Second()

			if min != last_min {
				if sec <= 1 {
					last_min = min
					tr_len := len(chunk.trans)
					if 100 < tr_len {
						duration := chunk.trans[tr_len-1].Time_stamp - chunk.trans[0].Time_stamp

						if 30*1000000 <= duration {
							chunk.dump()
							fmt.Println("DUMP", chunk.info_string())
						}
					}
				}

				chunk.bit_board = bit_board.copy() // CopyBuffer
				chunk.ask_board = ask_board.copy()
				chunk.trans.init()
			}

			if record.Action == UPDATE_BUY {
				bit_board.set(int(record.Price), int(record.Volume))
			} else if record.Action == UPDATE_SELL {
				ask_board.set(int(record.Price), int(record.Volume))
			} else {
				log.Fatal("Unknown action")
			}
		}

		chunk.append(record)

		if record.Action == TRADE_BUY {
			fmt.Println(record)
		}
	}

	return chunk
}
