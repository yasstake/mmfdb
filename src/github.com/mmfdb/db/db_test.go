package db

import (
	"fmt"
	"testing"
)

func TestLoad(t *testing.T) {
	load("../../../../DATA/BB2-2021-02-24T23-05-04.750033Z.log.gz")

	fmt.Println("hello")
}

func TestLoad2(t *testing.T) {
	bit, ask := load("../../../../DATA/BB2-2021-02-24T23-05-04.750033Z.log.gz")

	bit.dump()
	ask.dump()

	fmt.Println("hello")
}
