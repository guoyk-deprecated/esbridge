package iocount

import (
	"io"
	"sync/atomic"
)

type WriteCloser interface {
	io.WriteCloser
	WriteCount() int64
	WriteCloser() io.WriteCloser
}

type writerCloser struct {
	r io.WriteCloser
	c int64
}

func (w *writerCloser) Write(p []byte) (n int, err error) {
	n, err = w.r.Write(p)
	atomic.AddInt64(&w.c, int64(n))
	return
}

func (w *writerCloser) WriteCount() int64 {
	return w.c
}

func (w *writerCloser) WriteCloser() io.WriteCloser {
	return w.r
}

func (w *writerCloser) Close() error {
	return w.r.Close()
}

func NewWriteCloser(r io.WriteCloser) WriteCloser {
	return &writerCloser{r: r}
}
