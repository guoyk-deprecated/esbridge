package exporter

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNewDumpWriter(t *testing.T) {
	os.RemoveAll(filepath.Join("testdata/test1"))
	os.MkdirAll(filepath.Join("testdata/test1"), 0755)
	dw := NewExporter(filepath.Join("testdata/test1"))
	dw.Append([]byte(`{"hello":"world","project":"001"}`))
	dw.Append([]byte(`{"hello":"world","project":"002"}`))
	dw.Append([]byte(`{"hello":"world","project":"003"}`))
	dw.Append([]byte(`{"hello":"world","project":"001"}`))
	dw.Append([]byte(`{"hello":"world","project":"002"}`))
	dw.Append([]byte(`{"hello":"world","project":"003"}`))
	dw.Append([]byte(`{"hello":"world","project":"001"}`))
	dw.Append([]byte(`{"hello":"world","project":"002"}`))
	dw.Append([]byte(`{"hello":"world"}`))
	dw.Close()
}
