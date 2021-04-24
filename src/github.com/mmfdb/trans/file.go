package trans

import (
	"fmt"
	"io/ioutil"
	"log"
	"path/filepath"
	"sort"
	"time"
)

type FileInfo struct {
	files      []string
	start_time time.Time
	end_time   time.Time
}

/*
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
*/

func file_list(base_path string) []string {
	files, err := ioutil.ReadDir(base_path)

	if err != nil {
		log.Fatal("File canot open", base_path, err)
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

func (c TimeFrame) to_string() string {
	return c.start.String() + "->" + c.end.String()
}

type TimeFrames []TimeFrame

func (c TimeFrames) to_string() string {
	frames := len(c)
	result := ""
	for i := 0; i < frames; i++ {
		result = result + "/" + c[i].to_string()
	}
	return result
}

// Search DB directory and find time frame series chunks
// [{Start, end} {start, end},,,,,]
// Direcotry
//     BASE_PATH
//         +-----YYYY-MM-DD
//                    +------ HH-MM.log.gz
func time_chunks(base_path string) (times TimeFrames) {
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
	for _, dir_path := range dirs {
		var file_paths []string

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

const TIME_GAP = 60 + 5

// Append time chunks. Add new time to old TimeFrame
func append_time_chunks(org TimeFrames, now time.Time) []TimeFrame {
	if org == nil {
		org = []TimeFrame{{now, now}}
		return org
	}

	end := org[len(org)-1].end
	diff := now.Sub(end)

	diff_sec := diff.Seconds()

	switch {
	case diff_sec < 0:
		fmt.Println("ERROR in append_time_chunks", now.String(), end.String(), diff)
	case diff_sec <= TIME_GAP:
		pos := len(org) - 1
		org[pos].end = now
	case TIME_GAP < diff_sec:
		org = append(org, TimeFrame{now, now})
	default:
		fmt.Println("Unexpected case append_time_chunks")
	}

	return org
}
