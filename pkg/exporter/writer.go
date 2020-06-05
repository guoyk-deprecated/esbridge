package exporter

import (
	"compress/gzip"
	"errors"
	"os"
)

var (
	newLine = []byte{'\n'}
)

type Writer struct {
	file *os.File
	zip  *gzip.Writer
	data chan []byte
	done chan interface{}
	err  error
}

func NewWriter(filename string, level int) (w *Writer, err error) {
	var file *os.File
	if file, err = os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0640); err != nil {
		return
	}
	var zip *gzip.Writer
	if zip, err = gzip.NewWriterLevel(file, level); err != nil {
		_ = file.Close()
		return
	}

	w = &Writer{
		file: file,
		zip:  zip,
		data: make(chan []byte),
		done: make(chan interface{}),
	}
	go w.run()
	return
}

func (w *Writer) Write(p []byte) error {
	if w.err != nil {
		return w.err
	}
	select {
	case w.data <- p:
		return nil
	case <-w.done:
		return errors.New("writer already closed")
	}
}

func (w *Writer) run() {
	var err error
	for buf := range w.data {
		if _, err = w.zip.Write(buf); err != nil {
			w.err = err
			continue
		}
		if _, err = w.zip.Write(newLine); err != nil {
			w.err = err
			continue
		}
	}
	close(w.done)
}

func (w *Writer) Close() (err error) {
	close(w.data)
	<-w.done

	if err = w.zip.Close(); err != nil {
		_ = w.file.Close()
		return
	}
	err = w.file.Close()
	return
}
