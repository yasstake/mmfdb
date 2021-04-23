package db

import (
	"fmt"
	"path/filepath"
	"time"
)

func open_db(time time.Time) (log TransactionLog) {
	dir, file := make_path(to_time_ms(time))
	path := filepath.Join(DB_ROOT, dir, file)
	fmt.Println(path)
	return open_db_file(path)
}

func open_db_file(path string) (log TransactionLog) {
	reader := open_gzip(path)
	defer reader.Close()

	log = log.load(reader)

	return log
}
