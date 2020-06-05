package exporter

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestNewDumpWriter(t *testing.T) {
	os.RemoveAll(filepath.Join("testdata/test1"))
	os.MkdirAll(filepath.Join("testdata/test1"), 0755)
	dw := NewExporter(filepath.Join("testdata/test1"))

	for j := 0; j < 1000000; j++ {
		for i := 0; i < 3; i++ {
			_ = dw.Append([]byte(fmt.Sprintf(`{"hello":"world %d","project":"project-%d"}`, j, i)))
		}
	}

	dw.Close()
}
