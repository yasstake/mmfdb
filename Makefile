
test:
	go test -bench src/github.com/mmfdb/trans/ -benchmem
	#go test src/github.com/mmfdb/db/db_test.go

clean:
	rm -rf src/github.com/mmfdb/db/LOG
