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

    //fire and forget
    (async () => {
      try {
        await this.redis.addLog(this.redisKey, logEntry);
      } catch (err) {
        console.error(`Log failed`, err);
      }
    })();
  }

  private async flush() {}
}
