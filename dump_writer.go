package main

import (
	"compress/gzip"
	"encoding/json"
	"os"
	"path/filepath"
)

var (
	newLine = []byte{'\n'}
)

// DumpWriter is NOT concurrent-safe
type DumpWriter struct {
	dir   string
	files map[string]*os.File
	zips  map[string]*gzip.Writer
}

func (w *DumpWriter) Append(rm json.RawMessage) (err error) {
	var m map[string]interface{}
	if err = json.Unmarshal(rm, &m); err != nil {
		return
	}
	p, _ := m["project"].(string)
	if p == "" {
		p = "unknown"
	}
	return w.append(rm, p)
}

func (w *DumpWriter) append(rm json.RawMessage, p string) (err error) {
	f := w.files[p]
	if f == nil {
		if f, err = os.OpenFile(filepath.Join(w.dir, p+".ndjson.gz"), os.O_CREATE|os.O_RDWR, 0640); err != nil {
			return
		}
		w.files[p] = f
	}
	z := w.zips[p]
	if z == nil {
		if z, err = gzip.NewWriterLevel(f, gzip.BestCompression); err != nil {
			return
		}
		w.zips[p] = z
	}
	if _, err = z.Write(rm); err != nil {
		return
	}
	if _, err = z.Write(newLine); err != nil {
		return err
	}
	return
}

func (w *DumpWriter) Close() (err error) {
	// close zips
	zips := w.zips
	w.zips = nil
	for _, z := range zips {
		_ = z.Close()
	}

	// close files
	files := w.files
	w.files = nil
	for _, f := range files {
		_ = f.Close()
	}
	return
}

func NewDumpWriter(dir string) *DumpWriter {
	return &DumpWriter{
		dir:   dir,
		files: make(map[string]*os.File),
		zips:  make(map[string]*gzip.Writer),
	}
}
