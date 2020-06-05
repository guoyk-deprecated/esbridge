package progress

import "log"

type Progress struct {
	title    string
	total    int64
	count    int64
	progress int64
}

func (p *Progress) Set(c int64) {
	p.count = c
	np := p.count * 100 / p.total
	if np != p.progress {
		log.Printf("%s [%3d%%]", p.title, np)
	}
	p.progress = np
}

func (p *Progress) Add(c int64) {
	p.Set(p.count + c)
}

func (p *Progress) Incr() {
	p.Add(1)
}

func NewProgress(total int64, title string) *Progress {
	return &Progress{total: total, title: title}
}
