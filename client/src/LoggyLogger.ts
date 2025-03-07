export class LoggyLogger {
  static log(level: string, message: string, metadata: object = {}) {
    const logEntry = {
      timestamp: new Date().toISOString(),
      level,
      message,
      metadata,
    };
    console.log(JSON.stringify(logEntry));
  }
}
