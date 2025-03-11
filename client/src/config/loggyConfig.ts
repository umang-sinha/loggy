import { RedisConfig } from "./redisConfig";

export interface LoggyConfig {
  apiUrl: string;
  redisConfig: RedisConfig;
}
