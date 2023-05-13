package redis

import (
	"context"

	"github.com/redis/go-redis/v9"
)

const (
	key = "message"
)

type RedisStream struct {
	client     redis.Cmdable
	streamName string
	lastReadId string
}

func NewRedisStream(client redis.Cmdable, streamName string) *RedisStream {
	return &RedisStream{client, streamName, "0"}
}

func (r *RedisStream) Produce(ctx context.Context, msg string) error {
	return r.client.XAdd(ctx, &redis.XAddArgs{
		Stream: r.streamName,
		Values: map[string]interface{}{
			key: msg,
		},
	}).Err()
}

func (r *RedisStream) Consume(ctx context.Context) (<-chan string, <-chan error) {
	ch := make(chan string)
	errCh := make(chan error)
	go func() {
		streams, err := r.client.XRead(ctx, &redis.XReadArgs{
			Streams: []string{r.streamName, r.lastReadId},
			Count:   1,
			Block:   0,
		}).Result()
		if err != nil {
			errCh <- err
		} else {
			r.lastReadId = streams[0].Messages[0].ID
			ch <- streams[0].Messages[0].Values[key].(string)
		}
	}()
	return ch, errCh
}
