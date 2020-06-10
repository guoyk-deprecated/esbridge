package tasks

import (
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"
)

func TestChanClose(t *testing.T) {
	total := 10
	count := 0
	ch := make(chan int, 10)
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		for range ch {
			time.Sleep(time.Millisecond * 100)
			count++
		}
		wg.Done()
	}()
	for i := 0; i < total; i++ {
		ch <- i
	}
	close(ch)
	wg.Wait()
	assert.Equal(t, total, count)
}
