export const REDIS_DEFAULTS = {
  MAX_RETRIES: 5,
  INITIAL_DELAY: 100,
  BUFFER_SIZE: 10,
  URL: 'redis://localhost:6379'
} as const;
