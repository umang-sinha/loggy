import { parentPort, workerData } from "worker_threads";
import { KafkaClient } from "../services/kafkaClient";
import { LoggyConfig } from "../types";
import { ScyllaFallback } from "../fallbacks/scylladb/scyllaFallback";
import { LOGGY_DEFAULTS } from "../constants/loggyDefaults";

if (!parentPort) throw new Error("Retry worker must run in a worker thread");

let lastDeleteTime = 0;

async function retryUnsentLogs() {
  const loggyConfig: LoggyConfig = workerData.loggyConfig;

  const kafkaClient = await KafkaClient.create(loggyConfig.kafkaConfig);

  await kafkaClient.connect();

  const scyllaFallback = await ScyllaFallback.getInstance(
    loggyConfig.scyllaConfig!
  );

  while (true) {
    try {
      const unsentLogs = await scyllaFallback.fetchUnsentLogs();

      const ids = unsentLogs.map((log) => log.id!).filter(Boolean);

      try {
        await kafkaClient.sendBatch(unsentLogs);
        await scyllaFallback.markSent(ids);
      } catch (sendError) {
        console.error(`Failed to send logs or mark them sent`, sendError);
        await sleep(LOGGY_DEFAULTS.RETRY_INTERVAL_MS * 2);
        continue;
      }

      const now = Date.now();

      if (now - lastDeleteTime >= LOGGY_DEFAULTS.DELETE_INTERVAL_MS) {
        await scyllaFallback.cleanupSentLogs();
        lastDeleteTime = now;
      }
    } catch (error) {
      console.error(`Unexpected error in retry worker`, error);
    }

    await sleep(LOGGY_DEFAULTS.RETRY_INTERVAL_MS);
  }
}

function sleep(ms: number) {
  return new Promise((res) => setTimeout(res, ms));
}

retryUnsentLogs();
