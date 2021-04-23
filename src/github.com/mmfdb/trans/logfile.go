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
	"testing"
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

const DB_ROOT = "/tmp"

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

func TestReadWriteTransaction(t *testing.T) {
	tran := Transaction{1, 2, 3, 4}

	wf, _ := os.Create("/tmp/test2.bin")
	tran.save(wf)
	wf.Close()

	wf2, _ := os.Open("/tmp/test2.bin")
	var r2 Transaction
	r2 = r2.load(wf2)

	fmt.Println(tran, r2)
}

func TestReadWriteTransactions(t *testing.T) {
	tran1 := Transaction{1, 2, 3, 4}
	tran2 := Transaction{2, 2, 3, 4}
	tran3 := Transaction{3, 2, 3, 4}

	tr := Transactions{tran1, tran2, tran3}

	wf, _ := os.Create("/tmp/test2.bin")
	tr.save(wf)
	wf.Close()

	wf2, _ := os.Open("/tmp/test2.bin")
	var r2 Transactions
	r2 = r2.load(wf2)

	fmt.Println(tr, r2)
}

/*
func open_gzip(file string) io.ReadCloser {
	f, err := os.Open(file)
	if err != nil {
		log.Fatal(err)
	}
	gzipreader, _ := gzip.NewReader(f)

	return gzipreader
}
*/

func load_log(file string) (tr Transactions) {
	tr = make([]Transaction, 0, 1000000)
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

	var bit_board_snapshot Board
	bit_board_snapshot.init()
	var ask_board_snapshot Board
	ask_board_snapshot.init()
	tr.init()

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
				r, _ := strconv.Atoi(v)
				record.Volume = int32(r)
			}
		}

		if record.Action == PARTIAL {
			bit_board.init()
			ask_board.init()
			tr.init()
		} else if record.Action == UPDATE_BUY || record.Action == UPDATE_SELL {
			time := date_time(record.Time_stamp)
			min := time.Minute()
			sec := time.Second()

			if min != last_min {
				if sec <= 1 {
					last_min = min
					tr_len := len(tr)
					if 100 < tr_len {
						fmt.Println(date_time(tr[0].Time_stamp))
						fmt.Println(date_time(tr[len(tr)-1].Time_stamp))

						duration := tr[tr_len-1].Time_stamp - tr[0].Time_stamp

						if 30*1000000 <= duration {
							fmt.Println("DUMP", len(tr), bit_board_snapshot.depth(), ask_board_snapshot.depth())
						}
					}
				}

				bit_board_snapshot = bit_board.copy() // CopyBuffer
				ask_board_snapshot = ask_board.copy()
				tr.init()
			}

			if record.Action == UPDATE_BUY {
				bit_board.set(int(record.Price), int(record.Volume))
			} else if record.Action == UPDATE_SELL {
				ask_board.set(int(record.Price), int(record.Volume))
			} else {
				log.Fatal("Unknown action")
			}
		}
		tr = append(tr, record)
	}

	return tr
}
