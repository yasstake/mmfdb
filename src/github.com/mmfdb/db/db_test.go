package db

import (
	"fmt"
	"os"
	"testing"
)

func TestLoad(t *testing.T) {
	load("../../../../DATA/BB2-2021-02-24T23-05-04.750033Z.log.gz")

	fmt.Println("hello")
}

/*
func TestLoad2(t *testing.T) {
	transaction := load("../../../../DATA/BB2-2021-02-24T23-05-04.750033Z.log.gz")

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
	transaction := load("../../../../DATA/BB2-2021-02-24T23-05-04.750033Z.log.gz")

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
