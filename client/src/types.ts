import { KafkaConfig as KafkaJSConfig } from "kafkajs";

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
}

export interface LoggyConfig {
  kafkaConfig: KafkaConfig;
  scyllaConfig?: ScyllaConfig;
  numSendWorkers?: number;
  maxBufferSize?: number;
  fallback?: boolean;
}

export interface ScyllaConfig {
  contactPoints: string[];
  keyspace: string;
  username?: string;
  password?: string;
  connectionPoolSize?: number;
}
