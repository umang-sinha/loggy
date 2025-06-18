package models

import (
	"encoding/json"
	"time"
)

type LogEntry struct {
	RequestId string         `json:"requestId"`
	Level     string         `json:"level"`
	Message   string         `json:"message"`
	Metadata  map[string]any `json:"metadata"`
	Timestamp time.Time      `json:"timestamp"`
}

func FromBytes(data []byte) (LogEntry, error) {
	var logEntry LogEntry
	err := json.Unmarshal(data, &logEntry)
	return logEntry, err
}
