package actions

import (
	"compress/gzip"
	"fmt"
	"github.com/guoyk93/esbridge/pkg/exporter"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestWorkspaceGzip(t *testing.T) {
	dir := filepath.Join(os.TempDir(), "esbridge-test", fmt.Sprintf("test-%d", time.Now().Unix()))
	t.Log(dir)
	_ = os.MkdirAll(dir, 0755)
	dw := exporter.NewExporter(dir)

	for j := 0; j < 100000; j++ {
		for i := 0; i < 3; i++ {
			_ = dw.Append([]byte(fmt.Sprintf(`{"hello":"world %d","project":"project-%d"}`, j, i)))
		}
	}

	dw.Close()

	if err := WorkspaceGzipAll(dir, gzip.BestSpeed); err != nil {
		t.Fatal(err)
	}
}
