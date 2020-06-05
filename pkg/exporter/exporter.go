package exporter

import (
	"encoding/json"
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
	dir string
	ws  map[string]*Writer
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
	w := x.ws[p]
	if w == nil {
		if w, err = NewWriter(filepath.Join(x.dir, p+Ext)); err != nil {
			return
		}
		x.ws[p] = w
	}
	if err = w.Write(rm); err != nil {
		return
	}
	return
}

func (x *Exporter) Close() (err error) {
	ws := x.ws
	x.ws = make(map[string]*Writer)
	for _, f := range ws {
		_ = f.Close()
	}
	return
}

func NewExporter(dir string) *Exporter {
	return &Exporter{
		dir: dir,
		ws:  make(map[string]*Writer),
	}
}
