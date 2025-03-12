export interface KafkaConfig {
  brokers: string[];
  topic: string;
  partitions?: number;
  replicas?: number;
  clientId?: string;
}

export type logLevel = "INFO" | "DEBUG" | "WARN" | "ERROR";

export interface LogEntry {
  requestId: string;
  level: logLevel;
  message: string;
  metadata?: object;
  timestamp: string;
}

export interface LoggyConfig {
  kafkaConfig: KafkaConfig;
}
