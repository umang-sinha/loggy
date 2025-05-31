import { parentPort, workerData } from "worker_threads";
import { KafkaClient } from "../services/kafkaClient";
import { LogEntry, LoggyConfig } from "../types";
import { ScyllaFallback } from "../fallbacks/scylladb/scyllaFallback";

if (!parentPort) throw new Error("Send worker must run in a worker thread");

(async () => {
  const loggyConfig: LoggyConfig = workerData.loggyConfig;

  const kafkaClient = await KafkaClient.create(loggyConfig.kafkaConfig);

  await kafkaClient.connect();

  let scyllaFallback: ScyllaFallback | undefined;

  if (loggyConfig.fallback && loggyConfig.scyllaConfig) {
    scyllaFallback = await ScyllaFallback.getInstance(
      loggyConfig.scyllaConfig!
    );
  }

  parentPort.on("message", async (batch: LogEntry[]) => {
    try {
      await kafkaClient.sendBatch(batch);
    } catch (err) {
      if (scyllaFallback) await scyllaFallback.saveBatch(batch);
    }
  });
})().catch((err) => {
  console.error(`Send worker setup failed`, err);
  process.exit(1);
});
