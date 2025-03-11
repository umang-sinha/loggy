import { LoggyConfig } from "./config/loggyConfig";
import { RedisClient } from "./services/redisClient";

export class Loggy {
  private apiUrl: string;
  private redis;
  private bufferSize: number;
  private redisKey = "loggy_logs";
  private retries: number;

  constructor(config: LoggyConfig) {
    this.apiUrl = config.apiUrl;
    this.bufferSize = config.redisConfig.bufferSize || 100;
    this.redis = RedisClient.getInstance(config.redisConfig);
    this.retries = config.redisConfig.maxRetries || 5;
  }

  public async log(
    level: "INFO" | "WARN" | "DEBUG" | "ERROR",
    message: string,
    metadata?: object,
    service?: string
  ) {
    const logEntry = {
      message,
      level,
      metadata,
      service,
      timestamp: new Date().toISOString(),
    };

    try {
      await this.redis.addLog(this.redisKey, logEntry);
    } catch (err) {
      console.error(`Failed to add log to redis. Retrying...`);
      let numRetries = this.retries;
      while (numRetries > 0) {
        try {
          await this.redis.addLog(this.redisKey, logEntry);
          console.log(`Log added to Redis after retry`);
          break;
        } catch (retryErr) {
          numRetries--;
          if (numRetries === 0) {
            console.log(
              `Failed to add log to Redis after ${this.retries} retries`
            );
          }
        }
      }
    }
  }

  private async flush() {}
}
