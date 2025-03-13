import {
  Kafka,
  Message,
  Partitioners,
  Producer,
  Admin,
  CompressionTypes,
} from "kafkajs";
import { KAFKA_DEFAULTS } from "../constants/kafkaDefaults";
import { KafkaConfig, LogEntry } from "../types";

export class KafkaClient {
  private static instance: KafkaClient;
  private kafka: Kafka;
  private producer: Producer;
  private topic: string;
  private admin: Admin;
  private numPartitions: number;
  private replicationFactor: number;

  private constructor(config: KafkaConfig) {
    this.validateConfig(config);

    this.kafka = new Kafka({
      brokers: config.brokers,
      clientId: config.clientId,
    });
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
    this.replicationFactor = config.replicas ?? KAFKA_DEFAULTS.REPLICAS;
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

  private validateConfig(config: KafkaConfig) {
    if (!config.brokers || config.brokers.length === 0) {
      throw new Error(`Atleast one Kafka broker must be provided.`);
    }

    if (!config.topic || config.topic.trim() === "") {
      throw new Error(`Topic name must be a non-empty string`);
    }

    if (config.partitions !== undefined && config.partitions < 1) {
      console.warn(
        `Invalid number of partitions provided. Defaulting to ${KAFKA_DEFAULTS.PARTITIONS}`
      );
      config.partitions = KAFKA_DEFAULTS.PARTITIONS; //Explicitly modifying config
    }

    if (
      config.replicas !== undefined &&
      (config.replicas < 1 || config.replicas > config.brokers.length)
    ) {
      console.warn(
        `Invalid replication factor provided. Defaulting to ${KAFKA_DEFAULTS.REPLICAS}`
      );
      config.replicas = KAFKA_DEFAULTS.REPLICAS; //Explicitly modifying config
    }
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
