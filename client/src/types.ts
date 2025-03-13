export interface KafkaConfig {
  brokers: string[];
  topic: string;
  partitions?: number;
  replicas?: number;
  clientId?: string;
}

export type LogLevel = "INFO" | "DEBUG" | "WARN" | "ERROR" | "TRACE" | "FATAL";

export interface LogEntry {
  requestId: string;
  level: LogLevel;
  message: string;
  metadata?: object;
  timestamp: string;
}

export interface LoggyConfig {
  kafkaConfig: KafkaConfig;
  numSendWorkers?: number;
  maxBufferSize?: number;
}
