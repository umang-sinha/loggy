package config

type KafkaConfig struct {
	Brokers []string `mapstructure:"brokers"`
	Topic   string   `mapstructure:"topic"`
	GroupID string   `mapstructure:"groupID"`
}

type ElasticsearchConfig struct {
	URL   string `mapstructure:"url"`
	Index string `mapstructure:"index"`
}

type Config struct {
	Kafka         KafkaConfig         `mapstructure:"kafka"`
	Elasticsearch ElasticsearchConfig `mapstructure:"elasticsearch"`
	BatchSize     int                 `mapstructure:"batchSize"`
	FlushInterval int                 `mapstructure:"flushInterval"`
}
