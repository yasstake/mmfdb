package db

import (
	"fmt"
	"os"
	"testing"
)

/*
func TestLoad(t *testing.T) {
	//transaction :=
	load_log("../../../../DATA/BB2-2021-02-24T23-05-04.750033Z.log.gz")
}
*/
func TestLoadBig(t *testing.T) {
	//transaction :=
	load_log("../../../../DATA/BB2-2021-02-20T23-45-52.008914Z.log.gz")
}

/*
func TestLoadMiddle(t *testing.T) {
	//transaction :=
	load_log("../../../../DATA/BB2-2021-01-27T20-44-18.729622Z.log.gz")
}

func TestLoadMiddleBB(t *testing.T) {
	//transaction :=
	load_log("../../../../DATA/BB-2020-05-31T16-11-07.927390Z.log.gz")
}

func TestLoadBigFrombin(t *testing.T) {
	wf, _ := os.Open("/tmp/save_transactionbig.bin")
	var transaction TransactionLog
	transaction = transaction.load(wf)
	wf.Close()
}
*/
/*
func TestSaveAndLoad(t *testing.T) {
	transaction := load_log("../../../../DATA/BB2-2021-02-24T23-05-04.750033Z.log.gz")

	fmt.Println("hello")

	wf, _ := os.Create("/tmp/save_transaction.bin")
	transaction.save(wf)
	wf.Close()

	wf, _ = os.Open("/tmp/save_transaction.bin")
	var transaction2 TransactionLog
	transaction2 = transaction2.load(wf)

	transaction2.dump_to_directory("/tmp/")
}
*/

/*
func TestLoad2(t *testing.T) {

	transaction := load_log("../../../../DATA/BB2-2021-02-24T23-05-04.750033Z.log.gz")

	stdoutDumper := hex.Dumper(os.Stdout)
	defer stdoutDumper.Close()

	fmt.Println("test")
	transaction.save(stdoutDumper)
	fmt.Println("hello")
}
*/

/*
func TestDump(t *testing.T) {
	type A struct {
		Int32Field int32
		ByteField  byte
	}

	fmt.Println("start")

	// 構造体つくる
	a := A{Int32Field: 0x123456, ByteField: 0xFF}
	stdoutDumper := hex.Dumper(os.Stdout)
	defer stdoutDumper.Close()

	// 構造体をバイナリにする
	binary.Write(stdoutDumper, binary.LittleEndian, &a)

	fmt.Println("end")
}
*/

/*
func TestSaveRecord(t *testing.T) {
	transaction := load_log("../../../../DATA/BB2-2021-02-24T23-05-04.750033Z.log.gz")

	stdoutDumper := hex.Dumper(os.Stdout)
	defer stdoutDumper.Close()

	fmt.Println("test")
	transaction.save(stdoutDumper)
	fmt.Println("hello")

}
*/

func TestSaveLoadRecord(t *testing.T) {
	var rec Record

	rec.price = 100
	rec.time = 1
	rec.vol = 50

	wf, _ := os.Create("/tmp/test.bin")
	rec.save(wf, 1, 100)
	wf.Close()

	wf2, _ := os.Open("/tmp/test.bin")
	var rec2 Record
	rec2 = rec2.load(wf2, 1, 100)
	wf2.Close()

	fmt.Println(rec2.price)
	fmt.Println(rec2.time)
	fmt.Println(rec2.vol)
}

func TestSaveLoadRecords(t *testing.T) {
	rec1 := Record{time: 10, price: 8, vol: 20}
	rec2 := Record{time: 11, price: 9, vol: 21}
	rec3 := Record{time: 12, price: 10, vol: 22}
	rec4 := Record{time: 13, price: 11, vol: 23}

	var rec Records

	rec = append(rec, rec1)
	rec = append(rec, rec2)
	rec = append(rec, rec3)
	rec = append(rec, rec4)

	wf, _ := os.Create("/tmp/test2.bin")
	rec.save(wf, 1, 2)
	wf.Close()

	wf2, _ := os.Open("/tmp/test2.bin")
	var r2 Records
	r2 = r2.load(wf2, 1, 2)
	wf2.Close()

	fmt.Println(r2)

	if rec[0].price != r2[0].price ||
		rec[0].time != r2[0].time ||
		rec[0].vol != r2[0].vol {
		t.Error("Load Does not match")
	}
}

func TestCopyBoard(t *testing.T) {
	var board Board
	board.init()
	board.set(100, 100)
	board.set(101, 101)
	board.set(102, 102)
	board.set(104, 104)
	fmt.Println("org", board)

	copy_board := board.copy()
	fmt.Println("copy", copy_board)
}

func TestSaveLoadBoards(t *testing.T) {
	var board Board
	board.init()
	board.set(100, 100)
	board.set(101, 101)
	board.set(102, 102)
	board.set(104, 104)

	wf, _ := os.Create("/tmp/test3.bin")
	board.save(wf, 10)
	wf.Close()

	wf2, _ := os.Open("/tmp/test3.bin")
	var b Board
	b = b.load(wf2, 10)
	wf2.Close()

	fmt.Println(b)
}

func TestLoadSimpleFile(t *testing.T) {
	transaction := load_log("../../../../DATA/bb2.log")

	fmt.Println(transaction)
}
