package iocount

import (
	"io"
	"sync/atomic"
)

type ReadCloser interface {
	io.ReadCloser
	ReadCount() int64
	ReadCloser() io.ReadCloser
}

type readCloser struct {
	r io.ReadCloser
	c int64
}

func (r *readCloser) Read(p []byte) (n int, err error) {
	n, err = r.r.Read(p)
	atomic.AddInt64(&r.c, int64(n))
	return
}

func (r *readCloser) ReadCount() int64 {
	return r.c
}

func (r *readCloser) ReadCloser() io.ReadCloser {
	return r.r
}

func (r *readCloser) Close() error {
	return r.r.Close()
}

func NewReadCloser(r io.ReadCloser) ReadCloser {
	return &readCloser{r: r}
}
