package main

import "testing"

func TestNewProgress(t *testing.T) {
	p := NewProgress(500)
	for i := 0; i < 500; i++ {
		p.Incr()
	}
}
