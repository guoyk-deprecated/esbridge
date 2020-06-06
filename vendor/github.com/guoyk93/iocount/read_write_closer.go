package iocount

import (
	"io"
	"sync/atomic"
)

type ReadWriteCloser interface {
	io.ReadWriteCloser
	ReadCount() int64
	WriteCount() int64
	ReadWriteCloser() io.ReadWriteCloser
}

type readWriteCloser struct {
	rw io.ReadWriteCloser
	rc int64
	wc int64
}

func (rw *readWriteCloser) Read(p []byte) (n int, err error) {
	n, err = rw.rw.Read(p)
	atomic.AddInt64(&rw.rc, int64(n))
	return
}

func (rw *readWriteCloser) Write(p []byte) (n int, err error) {
	n, err = rw.rw.Write(p)
	atomic.AddInt64(&rw.wc, int64(n))
	return
}

func (rw *readWriteCloser) ReadCount() int64 {
	return rw.rc
}

func (rw *readWriteCloser) WriteCount() int64 {
	return rw.wc
}

func (rw *readWriteCloser) ReadWriteCloser() io.ReadWriteCloser {
	return rw.rw
}

func (rw *readWriteCloser) Close() error {
	return rw.rw.Close()
}

func NewReadWriteCloser(r io.ReadWriteCloser) ReadWriteCloser {
	return &readWriteCloser{rw: r}
}
