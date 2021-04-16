package db

import (
	"fmt"
	"testing"
)

func TestInfo(t *testing.T) {
	list := file_list(DB_ROOT)
	fmt.Println("DBLIST")
	fmt.Println(list)
}
