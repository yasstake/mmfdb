package db

import (
	"time"
)

type TimeMs uint64

func (ms TimeMs) Unix() time.Time {
	return time.Unix(0, int64(ms*1000000)).UTC()
}

// Convert time in ms to MIN
func (ms TimeMs) Min() int {
	s := ms.Unix()
	min := s.Minute()

	return min
}

func (ms TimeMs) str() string {
	s := ms.Unix()
	return s.String()
}

func (ms TimeMs) YYMMDD() (int, int, int) {
	s := ms.Unix()

	yy := s.Year()
	mm := int(s.Month())
	dd := s.Day()

	return yy, mm, dd
}

func (ms TimeMs) HHMMSS() (int, int, int) {
	s := ms.Unix()

	hh := s.Hour()
	mm := s.Minute()
	ss := s.Second()

	return hh, mm, ss
}
