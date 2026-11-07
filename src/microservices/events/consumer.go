package main

import (
	"context"
	"errors"
	"io"
	"log"
	"os"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	consumerGroupID = "events-service"

	consumeMinBytes = 1
	consumeMaxBytes = 1_000_000
	consumeMaxWait  = 10 * time.Second

	reconnectInitial = 1 * time.Second
	reconnectMax     = 10 * time.Second

	// consumerDialTimeout bounds a dial while the broker is down so FetchMessage
	// can return and the consume loop can apply its own backoff.
	consumerDialTimeout = 3 * time.Second
)

// consumeLog is the stdout line for each consumed message.
var consumeLog = log.New(os.Stdout, "", log.LstdFlags)

// startConsumers launches one reader goroutine per topic and returns without
// dialing. The caller can listen for HTTP while these goroutines reconnect.
func startConsumers(ctx context.Context, brokers []string, wg *sync.WaitGroup) {
	topics := []string{topicMovieEvents, topicUserEvents, topicPaymentEvents}
	for _, topic := range topics {
		wg.Add(1)
		go func(topic string) {
			defer wg.Done()
			consumeTopic(ctx, brokers, topic)
		}(topic)
	}
}

func consumeTopic(ctx context.Context, brokers []string, topic string) {
	log.Printf("consuming %s group=%s", topic, consumerGroupID)

	var reader *kafka.Reader
	defer func() {
		closeTopicReader(topic, reader)
	}()

	delay := reconnectInitial
	for ctx.Err() == nil {
		if reader == nil {
			reader = newTopicReader(brokers, topic)
		}

		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, io.EOF) {
				return
			}
			reader, delay, err = reconnectReader(ctx, topic, reader, delay, err)
			if err != nil {
				return
			}
			continue
		}

		delay = reconnectInitial
		logConsumed(msg)
		if commitMessage(ctx, reader, msg) != nil {
			return
		}
	}
}

func newTopicReader(brokers []string, topic string) *kafka.Reader {
	// A fresh Dialer keeps this reader off the producer Writer transport.
	// FirstOffset is used only when this group has no committed offset.
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:          brokers,
		GroupID:          consumerGroupID,
		Topic:            topic,
		StartOffset:      kafka.FirstOffset,
		MinBytes:         consumeMinBytes,
		MaxBytes:         consumeMaxBytes,
		MaxWait:          consumeMaxWait,
		MaxAttempts:      1,
		JoinGroupBackoff: reconnectInitial,
		Dialer: &kafka.Dialer{
			Timeout:   consumerDialTimeout,
			DualStack: true,
		},
	})
}

// reconnectReader closes the failed reader before waiting, so dials pause for
// the backoff instead of continuing in the background.
func reconnectReader(ctx context.Context, topic string, reader *kafka.Reader, delay time.Duration, cause error) (*kafka.Reader, time.Duration, error) {
	log.Printf("consume %s: %v", topic, cause)
	closeTopicReader(topic, reader)
	if err := waitBackoff(ctx, delay); err != nil {
		return nil, delay, err
	}
	return nil, nextBackoff(delay), nil
}

func logConsumed(msg kafka.Message) {
	consumeLog.Printf("consumed topic=%s partition=%d offset=%d key=%s body=%s",
		msg.Topic, msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
}

func commitMessage(ctx context.Context, reader *kafka.Reader, msg kafka.Message) error {
	err := reader.CommitMessages(ctx, msg)
	if err == nil {
		return nil
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	log.Printf("commit %s partition=%d offset=%d: %v", msg.Topic, msg.Partition, msg.Offset, err)
	return nil
}

func closeTopicReader(topic string, reader *kafka.Reader) {
	if reader == nil {
		return
	}
	if err := reader.Close(); err != nil {
		log.Printf("close reader %s: %v", topic, err)
	}
}

func waitBackoff(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func nextBackoff(current time.Duration) time.Duration {
	next := current * 2
	if next > reconnectMax {
		return reconnectMax
	}
	return next
}
