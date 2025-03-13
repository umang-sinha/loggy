import { parentPort, workerData } from "worker_threads";
import { KafkaClient } from "../services/kafkaClient";
import { KafkaConfig, LogEntry } from "../types";

if (!parentPort) throw new Error("Send worker must run in a worker thread");

(async () => {
  const kafkaConfig: KafkaConfig = workerData.kafkaConfig;

  const kafkaClient = await KafkaClient.create(kafkaConfig);

  await kafkaClient.connect();

  parentPort.on("message", async (batch: LogEntry[]) => {
    try {
      await kafkaClient.sendBatch(batch);
    } catch (err) {
      console.error(`Kafka send failed:`, err);
    }
  });
})().catch((err) => {
  console.error(`Send worker setup failed`, err);
  process.exit(1);
});
