package db

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"time"
)

func make_path(time TimeMs) (dir, path string) {
	yy, mm, dd := time.YYMMDD()
	h, m, _ := time.HHMMSS()

	dir_name := fmt.Sprintf("%04d-%02d-%02d", yy, mm, dd) + string(os.PathSeparator)
	file_name := fmt.Sprintf("%02d-%02d.log.gz", h, m)

	return dir_name, file_name
}

// parse file name and return timestamp in TimeMs
// reverse function of make_path
func file_to_time(file_path string) time.Time {
	re := regexp.MustCompile(`(\d{4})-(\d{2})-(\d{2})/(\d{2})-(\d{2}).log.gz$`)
	res := re.FindStringSubmatch(file_path)

	yy, _ := strconv.Atoi(res[1])
	mm, _ := strconv.Atoi(res[2])
	dd, _ := strconv.Atoi(res[3])
	h, _ := strconv.Atoi(res[4])
	m, _ := strconv.Atoi(res[5])

	return YYMMDDhms(yy, mm, dd, h, m, 0)
}

type FileInfo struct {
	files      []string
	start_time TimeMs
	end_time   TimeMs
}

func (c FileInfo) open(base_path string) FileInfo {
	c.files = file_list(base_path)

	return c
}

func (c FileInfo) next() {

}

func (c FileInfo) rewind() {

}

func info() {
	//DB_ROOT

}

func file_list(base_path string) []string {
	files, err := ioutil.ReadDir(base_path)

	if err != nil {
		fmt.Println("Error")
	}

	var paths []string
	for _, file := range files {
		if file.IsDir() {
			paths = append(paths, file_list(filepath.Join(base_path, file.Name()))...)
		} else {
			paths = append(paths, filepath.Join(base_path, file.Name()))
		}
	}

	return sort.StringSlice(paths)
}

type TimeFrame struct {
	start time.Time
	end   time.Time
}

// Append time chunks. Add new time to old TimeFrame
func append_time_chunks(org []TimeFrame, now time.Time) []TimeFrame {
	if org == nil {
		org = []TimeFrame{{now, now}}
		return org
	}

	end := org[len(org)-1].end
	diff := now.Sub(end)

	min := diff.Seconds()

	switch {
	case min < 0:
		fmt.Println("ERROR in append_time_chunks")
	case min <= 120:
		pos := len(org) - 1
		org[pos].end = now
	case 120 < min:
		org = append(org, TimeFrame{now, now})
	default:
		fmt.Println("Unexpected case append_time_chunks")
	}

	return org
}

// Search DB directory and find time frame series chunks
// [{Start, end} {start, end},,,,,]
// Direcotry
//     BASE_PATH
//         +-----YYYY-MM-DD
//                    +------ HH-MM.log.gz
func time_chunks(base_path string) (times []TimeFrame) {
	// Open LogDir and sort
	files, err := ioutil.ReadDir(base_path)

	if err != nil {
		fmt.Println("Error")
	}

	var dirs []string
	for _, file := range files {
		if file.IsDir() {
			dirs = append(dirs, filepath.Join(base_path, file.Name()))
		}
	}
	dirs = sort.StringSlice(dirs)

	// Open each log dir and sort each logs
	var file_paths []string
	for _, dir_path := range dirs {
		files, err := ioutil.ReadDir(dir_path)
		if err != nil {
			fmt.Println("Error")
		}

		for _, file := range files {
			name := file.Name()
			file_paths = append(file_paths, filepath.Join(dir_path, name))
		}
		file_paths = sort.StringSlice(file_paths)

		for _, file := range file_paths {
			time := file_to_time(file)
			times = append_time_chunks(times, time)
		}
	}

	return times
}

// (base_dir)/yyyy-mm-dd/hh-mm.log.gz
//
//
func start_end_time(base_path string) (start int, end int) {
	files, err := ioutil.ReadDir(base_path)

	if err != nil {
		fmt.Println("Error")
	}

	var paths []string
	for _, file := range files {
		if file.IsDir() {
			paths = append(paths, file_list(filepath.Join(base_path, file.Name()))...)
		} else {
			paths = append(paths, filepath.Join(base_path, file.Name()))
		}
	}

	return start, end
}
