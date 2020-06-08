package logutil

var (
	ProgressInvalid = ^int64(0)
)

type Progress interface {
	SetTotal(total int64)
	SetCount(count int64)
	Add(d int64)
	Incr()
}

type progress struct {
	label  string
	total  int64
	count  int64
	value  int64
	logger Logger
}

func (p *progress) set(count int64, total int64) {
	var value int64
	if total == 0 {
		value = ProgressInvalid
	} else {
		value = count * 100 / total
	}
	if value != p.value {
		if value == ProgressInvalid {
			p.logger.Printf("%s [???%%]", p.label)
		} else {
			p.logger.Printf("%s [%3d%%]", p.label, value)
		}
	}
	p.count = count
	p.total = total
	p.value = value
}

func (p *progress) SetTotal(total int64) {
	p.set(p.count, total)
}

func (p *progress) SetCount(count int64) {
	p.set(count, p.total)
}

func (p *progress) Add(d int64) {
	p.SetCount(p.count + d)
}

func (p *progress) Incr() {
	p.Add(1)
}

func NewProgress(logger Logger, label string) Progress {
	if logger == nil {
		logger = DiscardLogger
	}
	return &progress{
		label:  label,
		logger: logger,
	}
}
