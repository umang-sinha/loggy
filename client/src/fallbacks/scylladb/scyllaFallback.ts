import { Client, ClientOptions } from "cassandra-driver";
import { LogEntry } from "../../types";
import { SCYLLA_DEFAULTS } from "../../constants/scyllaDefaults";

export class ScyllaFallback {
  private static instance: ScyllaFallback;
  private client: Client;
  private readonly clientOptions: ClientOptions;

  private constructor(config: ClientOptions) {
    if (!config.keyspace) {
      throw new Error(`keyspace should be provided!`);
    }

    if (!config.localDataCenter) {
      config.localDataCenter = SCYLLA_DEFAULTS.LOCAL_DATACENTER;
    }

    this.clientOptions = {
      ...config,
    };

    this.client = new Client(this.clientOptions);
  }

  public static async getInstance(
    config: ClientOptions
  ): Promise<ScyllaFallback> {
    if (!ScyllaFallback.instance) {
      ScyllaFallback.instance = new ScyllaFallback(config);
      await ScyllaFallback.instance.init();
    }
    return ScyllaFallback.instance;
  }

  private async init() {
    const setupClient = new Client({
      ...this.clientOptions,
      keyspace: undefined,
    });

    await setupClient.connect();

    await setupClient.execute(
      `
      CREATE KEYSPACE IF NOT EXISTS ${this.clientOptions.keyspace} WITH REPLICATION = {
          'class': 'SimpleStrategy',
          'replication_factor': 1
        }
    `,
      [],
      { prepare: false }
    );

    await setupClient.shutdown();

    await this.client.connect();

    await this.createTable();
  }

  private async createTable() {
    const tableCreationQuery = `
        CREATE TABLE IF NOT EXISTS logs (
          sent boolean,
          id timeuuid,
          request_id text,
          level text,
          message text,
          metadata text,
          timestamp text,
          PRIMARY KEY ((sent), id)
        ) WITH CLUSTERING ORDER BY (id ASC);
    `;

    await this.client.execute(tableCreationQuery, [], {
      prepare: false,
    });
  }

  public async saveBatch(batch: LogEntry[]) {
    const queries = batch.map((log) => ({
      query: `INSERT INTO logs (sent, id, request_id, level, message, metadata, timestamp) VALUES (?, NOW(), ?, ?, ?, ?, ?)`,
      params: [
        false,
        log.requestId,
        log.level,
        log.message,
        JSON.stringify(log.metadata),
        log.timestamp,
      ],
    }));
    await this.client.batch(queries, { prepare: true });
  }
}
