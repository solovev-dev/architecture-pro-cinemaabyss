package main

import (
	"os"
	"strings"
)

const (
	defaultPort         = "8082"
	defaultKafkaBrokers = "kafka:9092"
)

// Config is read once at startup. The producer uses KafkaBrokers when a
// request is published; loadConfig does not dial the broker.
type Config struct {
	Port         string
	KafkaBrokers []string
}

func loadConfig() Config {
	port := os.Getenv("PORT")
	if port == "" {
		port = defaultPort
	}

	brokersRaw := os.Getenv("KAFKA_BROKERS")
	if brokersRaw == "" {
		brokersRaw = defaultKafkaBrokers
	}

	return Config{
		Port:         port,
		KafkaBrokers: parseBrokers(brokersRaw),
	}
}

func parseBrokers(raw string) []string {
	parts := strings.Split(raw, ",")
	brokers := make([]string, 0, len(parts))
	for _, part := range parts {
		broker := strings.TrimSpace(part)
		if broker == "" {
			continue
		}
		brokers = append(brokers, broker)
	}
	if len(brokers) == 0 {
		return []string{defaultKafkaBrokers}
	}
	return brokers
}
