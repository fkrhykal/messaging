package contract

import (
	"context"
)

type Publisher[E Event] interface {
	Publish(ctx context.Context, event E) error
}

type HandlerFn[E Event] func(ctx context.Context, event *E) error

type Consumer[E Event] interface {
	Consume(ctx context.Context, handler HandlerFn[E]) error
}
