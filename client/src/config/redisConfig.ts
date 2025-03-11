export interface RedisConfig {
  url: string;
  maxRetries?: number;
  initialDelay?: number;
  bufferSize?: number;
}
