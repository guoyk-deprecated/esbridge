package progress

import "log"

type Progress struct {
	title    string
	total    int64
	count    int64
	progress int64
}

func (p *Progress) Incr() {
	p.count++
	np := p.count * 100 / p.total
	if np != p.progress {
		log.Printf("%s: %02d%%", p.title, np)
	}
	p.progress = np
}

func NewProgress(total int64, title string) *Progress {
	return &Progress{total: total, title: title}
}
