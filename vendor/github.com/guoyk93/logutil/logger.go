package logutil

type Logger interface {
	Printf(layout string, items ...interface{})
}

type LoggerFunc func(layout string, items ...interface{})

func (l LoggerFunc) Printf(layout string, items ...interface{}) {
	l(layout, items...)
}

var (
	DiscardLogger LoggerFunc = func(layout string, items ...interface{}) {}
)
