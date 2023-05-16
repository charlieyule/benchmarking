package kafka_test

import (
	"context"
	"testing"

	"github.com/charlieyule/benchmarking/messaging/kafka"
	kafkaGo "github.com/segmentio/kafka-go"
)

const (
	host  = "127.0.0.1:9092"
	topic = "mq"
	msg   = "The quick brown fox jumps over the lazy dog"
)

func TestKafkaTopic(t *testing.T) {
	ctx := context.Background()

	cleanup(ctx)
	startup(ctx)
	defer cleanup(ctx)
	kt := kafka.NewTopic(host, topic)
	res, err := produceAndConsume(ctx, kt, msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res != msg {
		t.Fatalf("expected %v, got %v", msg, res)
	}

	res, err = produceAndConsume(ctx, kt, msg+msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res != msg+msg {
		t.Fatalf("expected %v, got %v", msg+msg, res)
	}
}

func Benchmark(b *testing.B) {
	ctx := context.Background()

	cleanup(ctx)
	startup(ctx)
	defer cleanup(ctx)
	kt := kafka.NewTopic(host, topic)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		produceAndConsume(ctx, kt, msg)
	}
}

func startup(ctx context.Context) error {
	_, err := kafkaGo.DialLeader(context.Background(), "tcp", host, topic, 0)
	if err != nil {
		return err
	}
	return nil
}

func cleanup(ctx context.Context) error {
	conn, err := kafkaGo.Dial("tcp", host)
	if err != nil {
		return err
	}
	if err := conn.DeleteTopics(topic); err != nil {
		return err
	}
	return nil
}

func produceAndConsume(ctx context.Context, kt *kafka.Topic, msg string) (string, error) {
	msgCh, errCh := kt.Consume(ctx)
	if err := kt.Produce(ctx, msg); err != nil {
		return "", err
	}
	select {
	case m := <-msgCh:
		return m, nil
	case e := <-errCh:
		return "", e
	}
}
