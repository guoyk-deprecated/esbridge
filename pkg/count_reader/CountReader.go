package count_reader

import (
	"io"
	"sync/atomic"
)

type CountReader struct {
	r io.Reader
	c int64
}

func (c *CountReader) Count() int64 {
	return c.c
}

func (c *CountReader) Read(p []byte) (n int, err error) {
	n, err = c.r.Read(p)
	atomic.AddInt64(&c.c, int64(n))
	return
}

func New(r io.Reader) *CountReader {
	return &CountReader{r: r}
}
