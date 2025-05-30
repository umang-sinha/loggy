import {
  Kafka,
  Message,
  Partitioners,
  Producer,
  Admin,
  CompressionTypes,
} from "kafkajs";
import { KAFKA_DEFAULTS } from "../constants/kafkaDefaults";
import { LogEntry, KafkaConfig } from "../types";

export class KafkaClient {
  private static instance: KafkaClient;
  private kafka: Kafka;
  private producer: Producer;
  private topic: string;
  private admin: Admin;
  private numPartitions: number;
  private replicationFactor: number;

  private constructor(config: KafkaConfig) {
    this.kafka = new Kafka(config);

    config.retry = {
      retries: 0, // enforce 0 retries
    };

    this.producer = this.kafka.producer({
      /**
       * If key is present, hash it and assign to a partition
       * If key is not present, assign partition in round-robin fashion
       */
      createPartitioner: Partitioners.DefaultPartitioner,
    });
    this.topic = config.topic;
    this.admin = this.kafka.admin();
    this.numPartitions = config.partitions ?? KAFKA_DEFAULTS.PARTITIONS;
    this.replicationFactor = config.replicationFactor ?? KAFKA_DEFAULTS.REPLICATION_FACTOR;
  }

  public static async create(config: KafkaConfig): Promise<KafkaClient> {
    if (!KafkaClient.instance) {
      const client = new KafkaClient(config);
      await client.createTopic(
        client.topic,
        client.numPartitions,
        client.replicationFactor
      );
      KafkaClient.instance = client;
    }

    return KafkaClient.instance;
  }

  private async createTopic(
    topic: string,
    numPartitions: number,
    replicationFactor: number
  ) {
    await this.admin.connect();

    const topics = await this.admin.listTopics();

    if (topics.includes(topic)) {
      console.log(`Topic ${topic} already exists.`);
    } else {
      await this.admin.createTopics({
        topics: [
          {
            topic,
            numPartitions,
            replicationFactor,
          },
        ],
      });
      console.log(
        `Topic ${topic} created with ${numPartitions} partitions and ${replicationFactor} replicas`
      );
    }

    await this.admin.disconnect();
  }

  public async sendBatch(logEntries: LogEntry[]) {
    const messages: Message[] = logEntries.map((log) => ({
      key: log.requestId,
      value: JSON.stringify(log),
    }));

    await this.producer.send({
      topic: this.topic,
      messages,
      compression: CompressionTypes.GZIP, // compress for efficiency
    });
  }

  public async connect() {
    await this.producer.connect();
  }

  public async disconnect() {
    await this.producer.disconnect();
  }
}
