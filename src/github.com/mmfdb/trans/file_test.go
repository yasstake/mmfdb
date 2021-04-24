package trans

import (
	"fmt"
	"testing"
)

func TestFileList(t *testing.T) {
	files := file_list(DB_ROOT)
	fmt.Println(files)
}

func TestTimeChunks(t *testing.T) {
	chunks := time_chunks(DB_ROOT)
	fmt.Println(chunks[0].start, chunks[0].end)
	fmt.Println(chunks.to_string())
}
