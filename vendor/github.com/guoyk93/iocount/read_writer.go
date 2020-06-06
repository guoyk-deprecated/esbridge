package iocount

import (
	"io"
	"sync/atomic"
)

type ReadWriter interface {
	io.ReadWriter
	ReadCount() int64
	WriteCount() int64
	ReadWriter() io.ReadWriter
}

type readWriter struct {
	rw io.ReadWriter
	rc int64
	wc int64
}

func (rw *readWriter) Read(p []byte) (n int, err error) {
	n, err = rw.rw.Read(p)
	atomic.AddInt64(&rw.rc, int64(n))
	return
}

func (rw *readWriter) Write(p []byte) (n int, err error) {
	n, err = rw.rw.Write(p)
	atomic.AddInt64(&rw.wc, int64(n))
	return
}

func (rw *readWriter) ReadCount() int64 {
	return rw.rc
}

func (rw *readWriter) WriteCount() int64 {
	return rw.wc
}

func (rw *readWriter) ReadWriter() io.ReadWriter {
	return rw.rw
}

func NewReadWriter(r io.ReadWriter) ReadWriter {
	return &readWriter{rw: r}
}
