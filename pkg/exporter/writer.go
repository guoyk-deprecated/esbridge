package exporter

import (
	"errors"
	"os"
)

var (
	newLine = []byte{'\n'}
)

type Writer struct {
	file *os.File
	data chan []byte
	done chan interface{}
	err  error
}

func NewWriter(filename string) (w *Writer, err error) {
	var file *os.File
	if file, err = os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0640); err != nil {
		return
	}

	w = &Writer{
		file: file,
		data: make(chan []byte, 100),
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
		if buf == nil {
			break
		}
		if _, err = w.file.Write(buf); err != nil {
			w.err = err
			continue
		}
		if _, err = w.file.Write(newLine); err != nil {
			w.err = err
			continue
		}
	}
	close(w.done)
}

func (w *Writer) Close() (err error) {
	w.data <- nil
	<-w.done

	if err = w.file.Close(); err != nil {
		return
	}
	return
}
