import { Client, ClientOptions, types } from "cassandra-driver";
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
    const logTableCreationQuery = `
      CREATE TABLE IF NOT EXISTS logs ( 
        bucket int,
        id timeuuid,
        request_id text,
        level text,
        message text,
        metadata text,
        timestamp text,
        sent boolean,
        PRIMARY KEY ((bucket), id)
      );`;

    const unsentLogIDsQueueCreationQuery = `
        CREATE TABLE IF NOT EXISTS unsent_log_ids_queue (
          bucket int,
          id timeuuid,
          PRIMARY KEY (bucket, id)
        ) WITH CLUSTERING ORDER BY (id ASC);
    `;

    await this.client.execute(logTableCreationQuery, [], { prepare: false });

    await this.client.execute(unsentLogIDsQueueCreationQuery, [], {
      prepare: false,
    });
  }

  public async saveBatch(batch: LogEntry[]) {
    const queries = batch
      .map((log) => {
        const id = types.TimeUuid.now();

        const bucket = this.getBucket(id.toString());

        return [
          {
            query: `INSERT INTO logs (bucket, id, request_id, level, message, metadata, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?)`,
            params: [
              this.getBucket(id.toString()),
              id,
              log.requestId,
              log.level,
              log.message,
              JSON.stringify(log.metadata),
              log.timestamp,
            ],
          },
          {
            query: `INSERT INTO unsent_log_ids_queue (bucket, id) VALUES (?, ?)`,
            params: [bucket, id],
          },
        ];
      })
      .flat();

    await this.client.batch(queries, { prepare: true });
  }

  public async fetchUnsentLogs(): Promise<LogEntry[]> {
    const CHUNK_SIZE = 10;
    const allLogs: LogEntry[] = [];

    for (let bucket = 0; bucket < SCYLLA_DEFAULTS.NUM_BUCKETS; bucket++) {
      const queueQuery = `SELECT id FROM unsent_log_ids_queue WHERE bucket = ? LIMIT 50`;

      const queueResult = await this.client.execute(queueQuery, [bucket], {
        prepare: true,
        consistency: types.consistencies.quorum,
      });

      const ids = queueResult.rows.map((row) => row.id);

      if (ids.length === 0) continue;

      const chunks = [];
      for (let i = 0; i < ids.length; i += CHUNK_SIZE) {
        chunks.push(ids.slice(i, i + CHUNK_SIZE));
      }

      const chunkResults = await Promise.all(
        chunks.map(async (chunk) => {
          const placeholders = chunk.map(() => "?").join(", ");
          const logsQuery = `SELECT * FROM logs WHERE id IN (${placeholders})`;
          const result = await this.client.execute(logsQuery, chunk, {
            prepare: true,
            consistency: types.consistencies.quorum,
          });
          return result.rows.map((row) => ({
            requestId: row.request_id,
            level: row.level,
            message: row.message,
            metadata: row.metadata,
            timestamp: row.timestamp,
            id: row.id.toString(),
          }));
        })
      );

      allLogs.push(...chunkResults.flat());
    }

    console.log(`Fetched total ${allLogs.length} unsent logs`);
    return allLogs;
  }

  public async markSent(ids: string[]) {
    if (ids.length === 0) return;

    const queries = ids.map((id) => {
      const bucket = this.getBucket(id);

      return {
        query: `DELETE FROM unsent_log_ids_queue WHERE bucket = ? AND id = ?`,
        params: [bucket, id],
      };
    });

    await this.client.batch(queries, { prepare: true });
    console.log(`Marked ${ids.length} logs as sent (deleted from queue)`);
  }

  public async cleanupSentLogs() {
    let totalDeleted = 0;

    for (let bucket = 0; bucket < SCYLLA_DEFAULTS.NUM_BUCKETS; bucket++) {
      const fetchLogsQuery = `SELECT id FROM logs WHERE bucket = ? LIMIT 1000`;
      const fetchQueueQuery = `SELECT id FROM unsent_log_ids_queue WHERE bucket = ? LIMIT 1000`;

      const [logsResult, queueResult] = await Promise.all([
        this.client.execute(fetchLogsQuery, [bucket], {
          prepare: true,
          consistency: types.consistencies.quorum,
        }),
        this.client.execute(fetchQueueQuery, [bucket], {
          prepare: true,
          consistency: types.consistencies.quorum,
        }),
      ]);

      const logRows = logsResult.rows;
      if (logRows.length === 0) {
        console.log(`No logs to clean in bucket ${bucket}`);
        continue;
      }

      const logIds = logRows.map((row) => row.id.toString());
      const unsentIds = new Set(
        queueResult.rows.map((row) => row.id.toString())
      );

      const sentIds = logIds.filter((id) => !unsentIds.has(id));
      if (sentIds.length === 0) {
        console.log(`No sent logs to clean in bucket ${bucket}`);
        continue;
      }

      const deleteBatch = sentIds.map((id) => ({
        query: `DELETE FROM logs WHERE bucket = ? AND id = ?`,
        params: [bucket, id],
      }));

      await this.client.batch(deleteBatch, {
        prepare: true,
        consistency: types.consistencies.quorum,
      });

      console.log(`Deleted ${sentIds.length} sent logs from bucket ${bucket}`);
      totalDeleted += sentIds.length;
    }

    console.log(`Total deleted sent logs: ${totalDeleted}`);
  }

  private getBucket(id: string): number {
    let hash = 0;
    for (let i = 0; i < id.length; i++) {
      hash = (hash * 31 + id.charCodeAt(i)) >>> 0;
    }
    return hash % SCYLLA_DEFAULTS.NUM_BUCKETS;
  }
}
