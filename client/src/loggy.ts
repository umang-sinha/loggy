import { KafkaClient } from "./services/kafkaClient";
import { LoggyConfig, logLevel } from "./types";

class Loggy {
  private static instance: Loggy;
  private kafkaClient: KafkaClient;

  private constructor(kafkalient: KafkaClient) {
    this.kafkaClient = kafkalient;
  }

  public static async getInstance(config: LoggyConfig): Promise<Loggy> {
    if (!Loggy.instance) {
      const kafkaClient = await KafkaClient.create(config.kafkaConfig);
      Loggy.instance = new Loggy(kafkaClient);
    }
    return Loggy.instance;
  }

  public async log(
    requestId: string,
    level: logLevel,
    message: string,
    metadata?: object
  ) {
    await this.kafkaClient.sendLog(requestId, level, message, metadata);
  }
}

export default Loggy;
