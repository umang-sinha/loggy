package elasticsearch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
)

const indexTemplateName = "loggy-template"

func (c *Client) EnsureIndexTemplate(ctx context.Context) error {
	template := map[string]any{
		"index_patterns": []string{"loggy-logs-*"},
		"template": map[string]any{
			"mappings": map[string]any{
				"properties": map[string]any{
					"id":        map[string]any{"type": "keyword"},
					"requestId": map[string]any{"type": "keyword"},
					"level":     map[string]any{"type": "keyword"},
					"message":   map[string]any{"type": "text"},
					"timestamp": map[string]any{"type": "date"},
					"metadata":  map[string]any{"type": "object", "enabled": true},
				},
			},
		},
	}

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(template); err != nil {
		return fmt.Errorf("failed to encode index template: %w", err)
	}

	res, err := c.es.Indices.PutIndexTemplate(indexTemplateName, &buf)

	if err != nil {
		return fmt.Errorf("failed to PUT index template: %w", err)
	}

	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("failed to apply index template: %s", res.String())
	}

	return nil
}
