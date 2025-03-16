import { Client, ClientOptions, types } from "cassandra-driver";
import { LogEntry, ScyllaConfig } from "../../types";
import { SCYLLA_DEFAULTS } from "../../constants/scyllaDefaults";

export class ScyllaFallback {
  private static instance: ScyllaFallback;
  private client: Client;
  private readonly clientOptions: ClientOptions;
  private connectionPoolSize: number;

  private constructor(config: ScyllaConfig) {
    this.validateConfig(config);

    this.connectionPoolSize =
      config.connectionPoolSize ?? SCYLLA_DEFAULTS.CONNECTION_POOL_SIZE;

    this.clientOptions = {
      contactPoints: config.contactPoints,
      localDataCenter: "datacenter1",
      keyspace: config.keyspace,
      credentials:
        config.username && config.password
          ? { username: config.username, password: config.password }
          : undefined,
      pooling: {
        coreConnectionsPerHost: {
          [types.distance.local]: this.connectionPoolSize,
        },
      },
    };
    this.client = new Client(this.clientOptions);
  }

  private validateConfig(config: ScyllaConfig) {
    if (!config.contactPoints || config.contactPoints.length === 0) {
      throw new Error(`Atleast one contact point must be provided`);
    }

    if (!config.keyspace || config.keyspace.trim() === "") {
      throw new Error(`Keyspace name must be a non-empty string`);
    }

    if (
      config.connectionPoolSize !== undefined &&
      config.connectionPoolSize < 1
    ) {
      console.warn(
        `Invalid connection pool size provided. Defaulting to ${SCYLLA_DEFAULTS.CONNECTION_POOL_SIZE}`
      );
      config.connectionPoolSize = SCYLLA_DEFAULTS.CONNECTION_POOL_SIZE;
    }
  }

  public static async getInstance(
    config: ScyllaConfig
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

    await this.client.execute(tableCreationQuery, [], { prepare: false });
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
