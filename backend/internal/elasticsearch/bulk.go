package elasticsearch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/umang-sinha/loggy/backend/internal/models"
)

type Client struct {
	es *elasticsearch.Client
}

func NewClient(cfg elasticsearch.Config) (*Client, error) {
	es, err := elasticsearch.NewClient(cfg)

	if err != nil {
		return nil, fmt.Errorf("failed to create elastic client: %w", err)
	}

	return &Client{es: es}, nil
}

func (c *Client) bulkPushLogs(ctx context.Context, logs []models.LogEntry) error {

	if len(logs) == 0 {
		fmt.Println("exiting because 0 logs")
		return nil
	}

	indexBuckets := make(map[string][]models.LogEntry)

	for _, log := range logs {
		idx := getIndexName(log.Timestamp)
		indexBuckets[idx] = append(indexBuckets[idx], log)
	}

	for indexName, bucket := range indexBuckets {
		var buf bytes.Buffer

		for _, log := range bucket {
			meta := map[string]any{
				"index": map[string]any{
					"_index": indexName,
				},
			}
			metaLine, _ := json.Marshal(meta)
			logLine, _ := json.Marshal(log)
			buf.Write(metaLine)
			buf.WriteByte('\n')
			buf.Write(logLine)
			buf.WriteByte('\n')
		}

		res, err := c.es.Bulk(bytes.NewReader(buf.Bytes()), c.es.Bulk.WithContext(ctx))

		body, _ := io.ReadAll(res.Body)
		fmt.Println("\nBulk response body:", string(body))

		if err != nil {
			return fmt.Errorf("bulk push error: %w", err)
		}

		defer res.Body.Close()

		if res.IsError() {
			return fmt.Errorf("bulk push failed: %s", res.String())
		}
	}

	return nil
}

func getIndexName(t time.Time) string {
	return fmt.Sprintf("loggy-logs-%s", t.Format("2006.01.02"))
}

const (
	bulkSize    = 500
	flushPeriod = 2 * time.Second
)

func (c *Client) StartBulkInserter(ctx context.Context, ch <-chan models.LogEntry, done chan<- struct{}) {
	defer close(done)
	buffer := make([]models.LogEntry, 0, bulkSize)
	ticker := time.NewTicker(flushPeriod)
	defer ticker.Stop()

	flush := func() {
		if len(buffer) == 0 {
			return
		}
		toPush := buffer
		buffer = nil
		if err := c.bulkPushLogs(ctx, toPush); err != nil {
			log.Printf("Failed to push %d logs to ES: %v", len(toPush), err)
		}
		log.Printf("PUSHED %d logs to ES", len(toPush))
	}

	for {
		select {
		case logEntry, ok := <-ch:
			if !ok {
				flush()
				return
			}
			buffer = append(buffer, logEntry)
			if len(buffer) >= bulkSize {
				log.Printf("BUFFER FULL, FLUSHING\n")
				flush()
			}

		case <-ticker.C:
			log.Printf("TIMER ELAPSED, FLUSHING\n")
			flush()
		}
	}
}
