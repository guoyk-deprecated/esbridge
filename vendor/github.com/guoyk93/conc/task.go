package conc

import "context"

type Task interface {
	Run(ctx context.Context) error
}

type TaskFunc func(ctx context.Context) error

func (t TaskFunc) Run(ctx context.Context) error {
	return t(ctx)
}
