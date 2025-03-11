import Redis from "ioredis";
import { RedisConfig } from "../config/redisConfig";
import { REDIS_DEFAULTS } from "../constants/redisDefaults";

export class RedisClient {
  private static instance: RedisClient;
  private redis: Redis;

  private config: {
    url: string;
    maxRetries: number;
    initialDelay: number;
    bufferSize: number;
  };

  private constructor(config: RedisConfig, redisClient?: Redis) {
    this.config = {
      url: config.url ?? REDIS_DEFAULTS.URL,
      maxRetries: config.maxRetries ?? REDIS_DEFAULTS.MAX_RETRIES,
      initialDelay: config.initialDelay ?? REDIS_DEFAULTS.INITIAL_DELAY,
      bufferSize: config.bufferSize ?? REDIS_DEFAULTS.BUFFER_SIZE,
    };

    this.redis = redisClient || new Redis(config.url);

    this.redis.on("connect", () => {
      console.log(`Loggy connected to Redis!`);
    });

    this.redis.on("error", (err) => {
      console.error(`Redis client error:`, err);
    });
  }

  public static getInstance(
    config: RedisConfig,
    redisClient?: Redis
  ): RedisClient {
    if (!RedisClient.instance) {
      RedisClient.instance = new RedisClient(config, redisClient);
    }

    return RedisClient.instance;
  }

  private async retry<T>(
    operation: () => Promise<T>,
    retries: number,
    delay: number
  ): Promise<T | null> {
    while (retries >= 0) {
      try {
        return await operation();
      } catch (err) {
        if (retries === 0) {
          console.error(`Redis operation failed after retries`);
          return null;
        }

        console.warn(
          `Redis operation failed. Retrying in ${delay}ms. ${retries} retries left`
        );

        await new Promise((resolve) => setTimeout(resolve, delay));
        delay *= 2; //exponential backoff
        retries--;
      }
    }
    return null;
  }

  public async addLog(key: string, log: object): Promise<boolean> {
    const result = await this.retry(
      () => this.redis.rpush(key, JSON.stringify(log)),
      this.config.maxRetries,
      this.config.initialDelay
    );
    return result !== null;
  }

  public async close() {
    await this.redis.quit();
  }
}
