package main

import (
	"context"
	"errors"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	topicMovieEvents   = "movie-events"
	topicUserEvents    = "user-events"
	topicPaymentEvents = "payment-events"

	// kafkaWriteTimeout bounds a dead broker well under Newman's 10s request limit.
	kafkaWriteTimeout = 3 * time.Second
	kafkaBatchTimeout = 50 * time.Millisecond
)

// publishAck carries the partition and offset from the writer's Completion
// callback. WriteMessages does not copy those fields back to the caller.
type publishAck struct {
	partition int
	offset    int64
}

// producer writes event envelopes synchronously so the HTTP response can
// return the broker's partition and offset. The writer dials on the first
// WriteMessages call, not when it is constructed.
type producer struct {
	writer *kafka.Writer
}

func newProducer(brokers []string) *producer {
	writer := &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		Balancer:               &kafka.Hash{},
		MaxAttempts:            1,
		BatchSize:              1,
		BatchTimeout:           kafkaBatchTimeout,
		ReadTimeout:            kafkaWriteTimeout,
		WriteTimeout:           kafkaWriteTimeout,
		RequiredAcks:           kafka.RequireOne,
		Async:                  false,
		AllowAutoTopicCreation: true,
		Transport: &kafka.Transport{
			DialTimeout: kafkaWriteTimeout,
		},
	}
	writer.Completion = completePublish
	return &producer{writer: writer}
}

func completePublish(messages []kafka.Message, err error) {
	if err != nil {
		return
	}
	for _, msg := range messages {
		ack, ok := msg.WriterData.(chan publishAck)
		if !ok {
			continue
		}
		ack <- publishAck{partition: msg.Partition, offset: msg.Offset}
	}
}

func (p *producer) Publish(ctx context.Context, topic string, key string, value []byte) (int, int64, error) {
	if topic == "" || key == "" || len(value) == 0 {
		return 0, 0, errors.New("invalid kafka message")
	}

	ack := make(chan publishAck, 1)
	err := p.writer.WriteMessages(ctx, kafka.Message{
		Topic:      topic,
		Key:        []byte(key),
		Value:      value,
		WriterData: ack,
	})

	// A completed batch already has the broker offset, even if the caller
	// context expired in the same moment.
	select {
	case result := <-ack:
		return result.partition, result.offset, nil
	default:
	}
	if err != nil {
		return 0, 0, err
	}
	return 0, 0, errors.New("kafka write missing partition and offset")
}

// Close releases the producer writer. Readers in the consume loop use their own
// kafka-go connections and are closed separately.
func (p *producer) Close() error {
	return p.writer.Close()
}
