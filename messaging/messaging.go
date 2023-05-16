package messaging

import "context"

type Producer interface {
	Produce(ctx context.Context, msg string) error
}

type Consumer interface {
	Consume(ctx context.Context) (<-chan string, <-chan error)
}
