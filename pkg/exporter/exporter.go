package exporter

import (
	"compress/gzip"
	"encoding/json"
	"os"
	"path/filepath"
)

const (
	Ext = ".ndjson.gz"
)

var (
	newLine = []byte{'\n'}
)

// Exporter is NOT concurrent-safe
type Exporter struct {
	dir   string
	files map[string]*os.File
	zips  map[string]*gzip.Writer
}

func (x *Exporter) Append(rm json.RawMessage) (err error) {
	var m map[string]interface{}
	if err = json.Unmarshal(rm, &m); err != nil {
		return
	}
	p, _ := m["project"].(string)
	if p == "" {
		p = "unknown"
	}
	return x.append(rm, p)
}

func (x *Exporter) append(rm json.RawMessage, p string) (err error) {
	f := x.files[p]
	if f == nil {
		if f, err = os.OpenFile(filepath.Join(x.dir, p+Ext), os.O_CREATE|os.O_RDWR, 0640); err != nil {
			return
		}
		x.files[p] = f
	}
	z := x.zips[p]
	if z == nil {
		if z, err = gzip.NewWriterLevel(f, gzip.BestCompression); err != nil {
			return
		}
		x.zips[p] = z
	}
	if _, err = z.Write(rm); err != nil {
		return
	}
	if _, err = z.Write(newLine); err != nil {
		return err
	}
	return
}

func (x *Exporter) Close() (err error) {
	// close zips
	zips := x.zips
	x.zips = nil
	for _, z := range zips {
		_ = z.Close()
	}

	// close files
	files := x.files
	x.files = nil
	for _, f := range files {
		_ = f.Close()
	}
	return
}

func NewExporter(dir string) *Exporter {
	return &Exporter{
		dir:   dir,
		files: make(map[string]*os.File),
		zips:  make(map[string]*gzip.Writer),
	}
}
