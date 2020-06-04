package main

import "log"

type Progress struct {
	total    int64
	count    int64
	progress int64
}

func (p *Progress) Incr() {
	p.count++
	np := p.count * 100 / p.total
	if np != p.progress {
		log.Printf("Progress: %02d%%", np)
	}
	p.progress = np
}

func NewProgress(total int64) *Progress {
	return &Progress{total: total}
}
