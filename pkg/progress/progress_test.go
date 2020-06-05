package progress

import "testing"

func TestNewProgress(t *testing.T) {
	p := NewProgress(500, "测试进度")
	for i := 0; i < 500; i++ {
		p.Incr()
	}
}
