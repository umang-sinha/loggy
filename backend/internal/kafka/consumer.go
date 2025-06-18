package kafka

import (
	"context"
	"log"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/umang-sinha/loggy/backend/internal/config"
	"github.com/umang-sinha/loggy/backend/internal/models"
)

func StartConsumerGroup(cfg config.KafkaConfig, handler func(models.LogEntry) error) error {

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  cfg.Brokers,
		GroupID:  cfg.GroupID,
		Topic:    cfg.Topic,
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})

	for {
		m, err := r.ReadMessage(context.Background())

		if err != nil {
			log.Printf("Error reading message: %v", err)
			time.Sleep(time.Second)
			continue
		}

		logEntry, err := models.FromBytes(m.Value)

		if err != nil {
			log.Printf("Invalid log format: %v", err)
			continue
		}

		if err := handler(logEntry); err != nil {
			log.Printf("Handler error: %v", err)
		}
	}
}
