package db

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
)

func make_path(time TimeMs) (dir, path string) {
	yy, mm, dd := time.YYMMDD()
	h, m, _ := time.HHMMSS()

	dir_name := fmt.Sprintf("%04d-%02d-%02d", yy, mm, dd) + string(os.PathSeparator)
	file_name := fmt.Sprintf("%02d-%02d.log.gz", h, m)

	return dir_name, file_name
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
