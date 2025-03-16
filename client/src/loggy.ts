import { LoggyConfig, LogLevel, LogEntry, KafkaConfig } from "./types";
import { LOGGY_DEFAULTS } from "./constants/loggyDefaults";
import { Worker } from "worker_threads";

class Loggy {
  private static instance: Loggy;
  private kafkaConfig: KafkaConfig;
  private buffer: LogEntry[] = [];
  private sendWorkers: Worker[];
  private maxBufferSize: number;
  private numSendWorkers: number;
  private isFlushing = false;

  private constructor(config: LoggyConfig) {
    this.kafkaConfig = config.kafkaConfig;

    this.maxBufferSize = config.maxBufferSize ?? LOGGY_DEFAULTS.MAX_BUFFER_SIZE;
    this.numSendWorkers =
      config.numSendWorkers ?? LOGGY_DEFAULTS.NUM_SEND_WORKERS;

    this.sendWorkers = Array.from(
      { length: this.numSendWorkers },
      () =>
        new Worker("../workers/sendWorker.js", {
          workerData: { loggyConfig: config },
        })
    );
  }

  public static getInstance(config: LoggyConfig): Loggy {
    if (config.fallback && !config.scyllaConfig) {
      throw new Error(`ScyllaConfig is required when fallback is true`);
    }

    if (!Loggy.instance) {
      Loggy.instance = new Loggy(config);
    }
    return Loggy.instance;
  }

  public log(
    requestId: string,
    level: LogLevel,
    message: string,
    metadata?: object
  ) {
    const logEntry: LogEntry = {
      requestId,
      level,
      message,
      metadata,
      timestamp: new Date().toISOString(),
    };
    this.buffer.push(logEntry);
    if (this.buffer.length >= this.maxBufferSize) {
      this.flush();
    }
  }

  private flush() {
    if (this.buffer.length === 0 || this.isFlushing) return;
    this.isFlushing = true;
    const bufferCopy = this.buffer.slice(); //safety net in case worker.postMessage fails
    this.buffer.length = 0;
    this.isFlushing = false;
    this.splitAndSend(bufferCopy);
  }

  private splitAndSend(bufferCopy: LogEntry[]) {
    const batchSize = Math.ceil(bufferCopy.length / this.sendWorkers.length);
    for (let i = 0; i < this.sendWorkers.length; i++) {
      const start = i * batchSize;
      const end = Math.min(start + batchSize, bufferCopy.length);
      const batch = bufferCopy.slice(start, end);
      if (batch.length > 0) {
        try {
          this.sendWorkers[i].postMessage(batch);
        } catch (err) {
          //TODO: Eventually add some fallback
          console.error(`Send worker ${i} failed:`, err);
        }
      }
    }
  }

  async shutdown() {
    this.flush();
    for (const worker of this.sendWorkers) {
      await worker.terminate();
    }
  }
}

export default Loggy;
