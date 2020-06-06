package iocount

import (
	"io"
	"sync/atomic"
)

type Writer interface {
	io.Writer
	WriteCount() int64
	Writer() io.Writer
}

type writer struct {
	r io.Writer
	c int64
}

func (w *writer) Write(p []byte) (n int, err error) {
	n, err = w.r.Write(p)
	atomic.AddInt64(&w.c, int64(n))
	return
}

func (w *writer) WriteCount() int64 {
	return w.c
}

func (w *writer) Writer() io.Writer {
	return w.r
}

func NewWriter(r io.Writer) Writer {
	return &writer{r: r}
}
