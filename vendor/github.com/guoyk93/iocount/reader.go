package iocount

import (
	"io"
	"sync/atomic"
)

type Reader interface {
	io.Reader
	ReadCount() int64
	Reader() io.Reader
}

type reader struct {
	r io.Reader
	c int64
}

func (r *reader) Read(p []byte) (n int, err error) {
	n, err = r.r.Read(p)
	atomic.AddInt64(&r.c, int64(n))
	return
}

func (r *reader) ReadCount() int64 {
	return r.c
}

func (r *reader) Reader() io.Reader {
	return r.r
}

func NewReader(r io.Reader) Reader {
	return &reader{r: r}
}
