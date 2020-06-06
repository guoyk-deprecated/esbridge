package progress

type Logger func(layout string, items ...interface{})

type Progress interface {
	Set(c int64)
	Add(d int64)
	Incr()
}

var (
	discardLogger Logger = func(layout string, items ...interface{}) {}
)

type progress struct {
	label  string
	total  int64
	count  int64
	perc   int64
	logger Logger
}

func (p *progress) Set(nc int64) {
	np := nc * 100 / p.total
	if np != p.perc {
		p.logger("%s [%3d%%]", p.label, np)
	}
	p.count = nc
	p.perc = np
}

func (p *progress) Add(c int64) {
	p.Set(p.count + c)
}

func (p *progress) Incr() {
	p.Add(1)
}

func NewProgress(total int64, label string, logger Logger) Progress {
	if total <= 0 {
		total = 100
	}
	if logger == nil {
		logger = discardLogger
	}
	return &progress{
		total:  total,
		label:  label,
		logger: logger,
	}
}
