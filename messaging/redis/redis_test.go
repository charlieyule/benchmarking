package redis

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
)

const streamName = "mq"
const msg = "The quick brown fox jumps over the lazy dog"

func TestRedisStream(t *testing.T) {
	ctx := context.Background()
	client := createClient()
	defer cleanup(ctx, client)
	r := NewRedisStream(client, streamName)
	res, err := produceAndConsume(ctx, r, msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res != msg {
		t.Fatalf("expected %v, got %v", msg, res)
	}
	res, err = produceAndConsume(ctx, r, msg+msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res != msg+msg {
		t.Fatalf("expected %v, got %v", msg+msg, res)
	}
}

func Benchmark(b *testing.B) {
	ctx := context.Background()
	client := createClient()
	defer cleanup(ctx, client)
	r := NewRedisStream(client, streamName)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		produceAndConsume(ctx, r, msg)
	}
}

func createClient() *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
}

func cleanup(ctx context.Context, client *redis.Client) error {
	return client.Del(ctx, streamName).Err()
}

func produceAndConsume(ctx context.Context, r *RedisStream, msg string) (string, error) {
	msgCh, errCh := r.Consume(ctx)
	if err := r.Produce(ctx, msg); err != nil {
		return "", err
	}
	select {
	case m := <-msgCh:
		return m, nil
	case e := <-errCh:
		return "", e
	}
}
