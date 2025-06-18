package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/elastic/go-elasticsearch/v8"

	"github.com/umang-sinha/loggy/backend/internal/config"
	es "github.com/umang-sinha/loggy/backend/internal/elasticsearch"
	"github.com/umang-sinha/loggy/backend/internal/kafka"
	"github.com/umang-sinha/loggy/backend/internal/models"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("Usage: ./loggy-consumer <config-file-path>")
	}

	cfgPath := os.Args[1]

	cfg, err := config.Load(cfgPath)

	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	fmt.Printf("Loaded config: %v\n", cfg)

	ctx := context.Background()

	esClient, _ := es.NewClient(elasticsearch.Config{
		Addresses: []string{cfg.Elasticsearch.URL},
	})

	if err := esClient.EnsureIndexTemplate(ctx); err != nil {
		log.Fatalf("ES template setup failed: %v", err)
	}

	logEntryChan := make(chan models.LogEntry, 10000)
	done := make(chan struct{})

	go esClient.StartBulkInserter(ctx, logEntryChan, done)

	go func() {
		err = kafka.StartConsumerGroup(cfg.Kafka, func(entry models.LogEntry) error {
			logEntryChan <- entry
			return nil
		})

		if err != nil {
			log.Fatalf("Kafka consumer failed: %v", err)
		}
	}()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	<-sigs
	close(logEntryChan)
	<-done
	log.Println("Shutdown complete")
}
