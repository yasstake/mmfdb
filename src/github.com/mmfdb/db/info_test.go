package db

import (
	"fmt"
	"regexp"
	"testing"
	"time"
)

func TestInfo(t *testing.T) {
	list := file_list(DB_ROOT)
	fmt.Println("DBLIST")
	fmt.Println(list)
}

func TestTimeChumks(t *testing.T) {
	chunks := time_chunks(DB_ROOT)
	fmt.Println(chunks)

	for _, c := range chunks {
		fmt.Println(c.start)
		fmt.Println(c.end)
	}

}

func TestRegEx(t *testing.T) {
	str := "2021-04-02/00-01.log.gz"

	re := regexp.MustCompile(`(\d{4})-(\d{2})-(\d{2})/(\d{2})-(\d{2}).log.gz$`)

	res := re.FindStringSubmatch(str)
	fmt.Println(res)
}

func TestFileToTimeMs(t *testing.T) {
	str := "2021-04-02/00-01.log.gz"
	time := file_to_time(str)
	fmt.Println(time)
}
func TestAppendTimeChunk(t *testing.T) {
	t1 := time.Date(2001, 1, 1, 1, 1, 0, 0, time.UTC)
	t2 := time.Date(2001, 1, 1, 1, 2, 0, 0, time.UTC)
	t3 := time.Date(2001, 1, 1, 1, 3, 0, 0, time.UTC)
	t4 := time.Date(2001, 1, 1, 1, 4, 0, 0, time.UTC)
	t5 := time.Date(2001, 1, 1, 1, 5, 0, 0, time.UTC)
	t6 := time.Date(2001, 1, 1, 1, 8, 0, 0, time.UTC)
	t7 := time.Date(2001, 1, 1, 1, 9, 0, 0, time.UTC)

	org := append_time_chunks(nil, t1)
	fmt.Println(org)

	org = append_time_chunks(org, t5)
	fmt.Println(org)

	org = []TimeFrame{{t1, t2}, {t3, t4}}
	org = append_time_chunks(org, t5)
	fmt.Println(org)

	org = []TimeFrame{{t1, t2}, {t3, t4}}
	org = append_time_chunks(org, t6)
	fmt.Println(org)
	org = append_time_chunks(org, t7)
	fmt.Println(org)
}
