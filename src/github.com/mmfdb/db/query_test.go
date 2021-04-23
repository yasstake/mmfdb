package db

import (
	"fmt"
	"testing"
)

func TestOpenDbFile(t *testing.T) {
	time := YYMMDDhms(2021, 2, 21, 1, 53, 0)
	log := open_db(time)

	fmt.Println(log)
}
