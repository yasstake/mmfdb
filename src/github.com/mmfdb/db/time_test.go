package db

import (
	"fmt"
	"testing"
)

func TestUnix1(t *testing.T) {
	ms := TimeMs(59999)
	unix := ms.Unix()
	fmt.Println(unix, 59999, ms.str())
}
func TestUnix2(t *testing.T) {
	ms := TimeMs(60001)
	unix := ms.Unix()
	fmt.Println(unix, 60001, ms.str())
}

func TestMinMs(t *testing.T) {
	ms := TimeMs(59999)
	min := ms.Min()
	if min != 0 {
		t.Error()
	}
	ms = TimeMs(60000)
	min = ms.Min()
	if min != 1 {
		t.Error()
	}
	ms = TimeMs(60001)
	min = ms.Min()
	if min != 1 {
		t.Error()
	}
	ms = TimeMs(60000 * 2)
	min = ms.Min()
	if min != 2 {
		t.Error()
	}
}

func TestYYMMDD(t *testing.T) {
	ms := TimeMs(59999)
	yy, mm, dd := ms.YYMMDD()
	if yy != 1970 && mm != 1 && dd != 1 {
		t.Error()
	}
}

func TestHHMMSS(t *testing.T) {
	ms := TimeMs(59999)
	hh, mm, ss := ms.HHMMSS()
	if hh != 0 && mm != 0 && ss != 59 {
		t.Error(hh, mm, ss)
	}
}
