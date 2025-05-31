import { KafkaConfig as KafkaJSConfig } from "kafkajs";
import { ClientOptions } from "cassandra-driver";

export interface KafkaConfig extends KafkaJSConfig {
  topic: string;
  partitions?: number;
  replicationFactor?: number;
}

export type LogLevel = "INFO" | "DEBUG" | "WARN" | "ERROR" | "TRACE" | "FATAL";

export interface LogEntry {
  requestId: string;
  level: LogLevel;
  message: string;
  metadata?: object;
  timestamp: string;
  id?: string;
}

export interface LoggyConfig {
  kafkaConfig: KafkaConfig;
  scyllaConfig?: ClientOptions;
  numSendWorkers?: number;
  maxBufferSize?: number;
  fallback?: boolean;
}

