package kafka

import (
	"context"

	kafkaGo "github.com/segmentio/kafka-go"
)

var (
	key = []byte("message")
)

type Topic struct {
	r *kafkaGo.Reader
	w *kafkaGo.Writer
}

func NewTopic(host, topic string) *Topic {
	brokers := []string{host}
	r := kafkaGo.NewReader(kafkaGo.ReaderConfig{
		Brokers:   brokers,
		Topic:     topic,
		Partition: 0,
		MinBytes:  10e3,
		MaxBytes:  10e3,
	})
	w := kafkaGo.NewWriter(kafkaGo.WriterConfig{
		Brokers:  brokers,
		Topic:    topic,
		Balancer: &kafkaGo.LeastBytes{},
	})
	return &Topic{
		r: r,
		w: w,
	}
}

func (kt *Topic) Produce(ctx context.Context, msg string) error {
	return kt.w.WriteMessages(
		ctx,
		kafkaGo.Message{
			Key:   key,
			Value: []byte(msg),
		},
	)
}

func (kt *Topic) Consume(ctx context.Context) (<-chan string, <-chan error) {
	ch := make(chan string)
	errCh := make(chan error)
	go func() {
		for {
			m, err := kt.r.ReadMessage(ctx)
			if err != nil {
				errCh <- err
			} else {
				if err := kt.r.SetOffset(m.Offset); err != nil {
					panic(err)
				}
				ch <- string(m.Value)
			}
		}
	}()
	return ch, errCh
}

func (kt *Topic) Close() {
	kt.w.Close()
	kt.r.Close()
}
