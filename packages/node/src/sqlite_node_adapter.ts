import { mkdir } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { DatabaseSync, type SQLInputValue } from "node:sqlite";

import type { CompiledQuery, Kysely, QueryResult } from "kysely";

import type {
  ChimpbaseBlobListMetaResult,
  ChimpbaseBlobMetaRow,
  ChimpbaseBlobPartRow,
  ChimpbaseBlobUploadListMetaResult,
  ChimpbaseBlobUploadRow,
  ChimpbaseEngineAdapter,
  ChimpbaseEventRecord,
  ChimpbasePlatformShim,
  ChimpbaseProjectConfig,
  ChimpbaseQueueJobRecord,
} from "@chimpbase/core";
import { createChimpbaseEventDeliveryPayloads } from "@chimpbase/core";
import type {
  ChimpbaseBlobListOptions,
  ChimpbaseBlobUploadListOptions,
  ChimpbaseCollectionFilter,
  ChimpbaseCollectionFindOptions,
  ChimpbaseCollectionPatch,
  ChimpbaseValidator,
  ChimpbaseKvListOptions,
  ChimpbaseQueueEnqueueOptions,
  ChimpbaseStreamEvent,
  ChimpbaseStreamReadOptions,
} from "@chimpbase/runtime";
import { isArrayValue, isJsonObject, parseJson, parseJsonObject, parseStringRecord, v } from "@chimpbase/runtime";

import { createSqliteKysely } from "./kysely.ts";

type SqliteBinding = unknown;

interface SqliteRunResult {
  changes: number;
  lastInsertRowid?: bigint | number;
}

interface SqliteStatement {
  readonly columnNames: string[];
  all(...params: SqliteBinding[]): unknown[];
  run(...params: SqliteBinding[]): SqliteRunResult;
}

interface SqliteDatabase {
  close(): void;
  exec(sql: string): unknown;
  query(sql: string): SqliteStatement;
}

interface PersistedCollectionDocument {
  document_id: string;
  document_json: string;
}

interface PersistedCronScheduleRow {
  cron_expression: string;
  next_fire_at_ms: number;
  schedule_name: string;
}
const blobMetadataRowValidator = v.object({
  bucket: v.string(),
  content_type: v.string(),
  created_at: v.string(),
  driver_ref: v.string(),
  etag: v.string(),
  key: v.string(),
  metadata_json: v.string(),
  size: v.number(),
  updated_at: v.string(),
});
const blobPartRowValidator = v.object({
  created_at: v.string(),
  driver_ref: v.string(),
  etag: v.string(),
  part_number: v.number(),
  size: v.number(),
  upload_id: v.string(),
});
const blobUploadRowValidator = v.object({
  bucket: v.string(),
  content_type: v.string().nullable(),
  created_at_ms: v.number(),
  driver_ref: v.string(),
  expires_at_ms: v.number(),
  key: v.string(),
  metadata_json: v.string(),
  upload_id: v.string(),
});
const collectionDocumentRowValidator = v.object({
  document_id: v.string(),
  document_json: v.string(),
});
const collectionNameRowValidator = v.object({ collection_name: v.string() });
const cronScheduleRowValidator = v.object({
  cron_expression: v.string(),
  next_fire_at_ms: v.number(),
  schedule_name: v.string(),
});
const idRowValidator = v.object({ id: v.number() });
const keyRowValidator = v.object({ key: v.string() });
const payloadJsonRowValidator = v.object({ payload_json: v.string() });
const queueJobRowValidator = v.object({
  attempt_count: v.number(),
  id: v.number(),
  payload_json: v.string(),
  queue_name: v.string(),
});
const streamEventRowValidator = v.object({
  created_at: v.string(),
  event_name: v.string(),
  id: v.number(),
  payload_json: v.string(),
  stream_name: v.string(),
});
const uploadIdRowValidator = v.object({ upload_id: v.string() });
const valueJsonRowValidator = v.object({ value_json: v.string() });

function parseRows<TValue>(
  rows: unknown,
  validator: ChimpbaseValidator<TValue>,
  label: string,
): TValue[] {
  return validator.array().parse(rows, label);
}

function isKyselyRows<TResult>(rows: unknown): rows is TResult[] {
  return isArrayValue(rows) && rows.every((row) => isJsonObject(row));
}

function parseKyselyRows<TResult>(rows: unknown): TResult[] {
  if (!isKyselyRows<TResult>(rows)) {
    throw new Error("Kysely query rows must be database records");
  }
  return rows;
}


function buildSqliteQueueNameFilter(
  queueNames: readonly string[],
  startingParamIndex: number,
): { params: SqliteBinding[]; sql: string } {
  if (queueNames.length === 0) {
    return { params: [], sql: "1 = 0" };
  }

  return {
    params: [...queueNames],
    sql: `queue_name IN (${queueNames.map((_, index) => `?${startingParamIndex + index}`).join(", ")})`,
  };
}

export async function openSqliteDatabase(
  projectDir: string,
  config: ChimpbaseProjectConfig,
): Promise<SqliteDatabase> {
  if (config.storage.engine === "memory" || !(config.storage.path !== null && config.storage.path.length > 0) || config.storage.path === ":memory:") {
    return createSqliteDatabase(new DatabaseSync(":memory:"));
  }

  const databasePath = resolve(projectDir, config.storage.path);
  await mkdir(dirname(databasePath), { recursive: true });
  return createSqliteDatabase(new DatabaseSync(databasePath));
}

export async function applySqlMigrations(db: SqliteDatabase, migrations: readonly string[]): Promise<void> {
  for (const migration of migrations) {
    db.exec(migration);
  }
}

export async function applyInlineSqlMigrations(db: SqliteDatabase, migrations: string[]): Promise<void> {
  for (const migration of migrations) {
    db.exec(migration);
  }
}

export async function ensureSqliteInternalTables(db: SqliteDatabase): Promise<void> {
  db.exec(
    `
      CREATE TABLE IF NOT EXISTS _chimpbase_events (
        id INTEGER PRIMARY KEY,
        event_name TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
      );
    `,
  );

  db.exec(
    `
      CREATE TABLE IF NOT EXISTS _chimpbase_kv (
        key TEXT PRIMARY KEY,
        value_json TEXT NOT NULL,
        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        expires_at TEXT DEFAULT NULL
      );
    `,
  );

  db.exec(
    `
      CREATE TABLE IF NOT EXISTS _chimpbase_collections (
        collection_name TEXT NOT NULL,
        document_id TEXT NOT NULL,
        document_json TEXT NOT NULL,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (collection_name, document_id)
      );
    `,
  );

  db.exec(
    `
      CREATE TABLE IF NOT EXISTS _chimpbase_stream_events (
        id INTEGER PRIMARY KEY,
        stream_name TEXT NOT NULL,
        event_name TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
      );
    `,
  );

  db.exec(
    `
      CREATE TABLE IF NOT EXISTS _chimpbase_queue_jobs (
        id INTEGER PRIMARY KEY,
        queue_name TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        status TEXT NOT NULL,
        available_at_ms INTEGER NOT NULL,
        attempt_count INTEGER NOT NULL DEFAULT 0,
        lease_expires_at_ms INTEGER,
        last_error TEXT,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        completed_at TEXT
      );
    `,
  );

  db.exec(
    `
      CREATE TABLE IF NOT EXISTS _chimpbase_cron_schedules (
        schedule_name TEXT PRIMARY KEY,
        cron_expression TEXT NOT NULL,
        next_fire_at_ms INTEGER NOT NULL,
        lease_token TEXT,
        lease_expires_at_ms INTEGER,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
      );
    `,
  );

  db.exec(
    `
      CREATE TABLE IF NOT EXISTS _chimpbase_cron_runs (
        schedule_name TEXT NOT NULL,
        fire_at_ms INTEGER NOT NULL,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (schedule_name, fire_at_ms)
      );
    `,
  );

  db.exec(
    `
      CREATE TABLE IF NOT EXISTS _chimpbase_workflow_instances (
        workflow_id TEXT PRIMARY KEY,
        workflow_name TEXT NOT NULL,
        workflow_version INTEGER NOT NULL,
        status TEXT NOT NULL,
        input_json TEXT NOT NULL,
        state_json TEXT NOT NULL,
        current_step_id TEXT,
        current_step_index INTEGER NOT NULL DEFAULT 0,
        wake_at_ms INTEGER,
        last_error TEXT,
        lease_token TEXT,
        lease_expires_at_ms INTEGER,
        completed_at TEXT,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
      );
    `,
  );

  db.exec(
    `
      CREATE TABLE IF NOT EXISTS _chimpbase_workflow_signals (
        id INTEGER PRIMARY KEY,
        workflow_id TEXT NOT NULL,
        signal_name TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        consumed_at TEXT,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
      );
    `,
  );

  db.exec(
    `
      CREATE TABLE IF NOT EXISTS _chimpbase_logs (
        id INTEGER PRIMARY KEY,
        level TEXT NOT NULL,
        message TEXT NOT NULL,
        attributes_json TEXT,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
      );
    `,
  );

  db.exec(
    `
      CREATE TABLE IF NOT EXISTS _chimpbase_metrics (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        value REAL NOT NULL,
        attributes_json TEXT,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
      );
    `,
  );

  db.exec(
    `
      CREATE TABLE IF NOT EXISTS _chimpbase_traces (
        id INTEGER PRIMARY KEY,
        trace_id TEXT NOT NULL,
        span_id TEXT NOT NULL,
        parent_span_id TEXT,
        name TEXT NOT NULL,
        scope_kind TEXT,
        scope_name TEXT,
        started_at TEXT NOT NULL,
        ended_at TEXT NOT NULL,
        attributes_json TEXT,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
      );
    `,
  );

  db.exec(
    "CREATE INDEX IF NOT EXISTS idx_chimpbase_stream_events_stream_id ON _chimpbase_stream_events (stream_name, id)",
  );
  db.exec(
    "CREATE INDEX IF NOT EXISTS idx_chimpbase_queue_pending ON _chimpbase_queue_jobs (status, available_at_ms, id)",
  );
  db.exec(
    "CREATE INDEX IF NOT EXISTS idx_chimpbase_queue_leased ON _chimpbase_queue_jobs (status, lease_expires_at_ms, id)",
  );
  db.exec(
    "CREATE INDEX IF NOT EXISTS idx_chimpbase_logs_created_at ON _chimpbase_logs (created_at)",
  );
  db.exec(
    "CREATE INDEX IF NOT EXISTS idx_chimpbase_metrics_created_at ON _chimpbase_metrics (created_at)",
  );
  db.exec(
    "CREATE INDEX IF NOT EXISTS idx_chimpbase_traces_created_at ON _chimpbase_traces (created_at)",
  );

  db.exec(
    `
      CREATE TABLE IF NOT EXISTS _chimpbase_blobs (
        bucket TEXT NOT NULL,
        key TEXT NOT NULL,
        size INTEGER NOT NULL,
        etag TEXT NOT NULL,
        content_type TEXT NOT NULL DEFAULT 'application/octet-stream',
        metadata_json TEXT NOT NULL DEFAULT '{}',
        driver_ref TEXT NOT NULL,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (bucket, key)
      );
    `,
  );
  db.exec(
    "CREATE INDEX IF NOT EXISTS idx_chimpbase_blobs_bucket_prefix ON _chimpbase_blobs (bucket, key)",
  );
  db.exec(
    `
      CREATE TABLE IF NOT EXISTS _chimpbase_blob_uploads (
        upload_id TEXT PRIMARY KEY,
        bucket TEXT NOT NULL,
        key TEXT NOT NULL,
        content_type TEXT,
        metadata_json TEXT NOT NULL DEFAULT '{}',
        driver_ref TEXT NOT NULL,
        created_at_ms INTEGER NOT NULL,
        expires_at_ms INTEGER NOT NULL
      );
    `,
  );
  db.exec(
    "CREATE INDEX IF NOT EXISTS idx_chimpbase_blob_uploads_expires ON _chimpbase_blob_uploads (expires_at_ms)",
  );
  db.exec(
    "CREATE INDEX IF NOT EXISTS idx_chimpbase_blob_uploads_bucket_key ON _chimpbase_blob_uploads (bucket, key)",
  );
  db.exec(
    `
      CREATE TABLE IF NOT EXISTS _chimpbase_blob_upload_parts (
        upload_id TEXT NOT NULL,
        part_number INTEGER NOT NULL,
        size INTEGER NOT NULL,
        etag TEXT NOT NULL,
        driver_ref TEXT NOT NULL,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (upload_id, part_number),
        FOREIGN KEY (upload_id) REFERENCES _chimpbase_blob_uploads(upload_id) ON DELETE CASCADE
      );
    `,
  );
}

export function createSqliteEngineAdapter(
  db: SqliteDatabase,
  platform: ChimpbasePlatformShim,
): ChimpbaseEngineAdapter {

  return {
    async advanceCronSchedule(
      scheduleName: string,
      fireAtMs: number,
      nextFireAtMs: number,
      leaseToken: string,
    ): Promise<void> {
      db.query(
        `
          UPDATE _chimpbase_cron_schedules
          SET
            next_fire_at_ms = ?1,
            lease_token = NULL,
            lease_expires_at_ms = NULL,
            updated_at = CURRENT_TIMESTAMP
          WHERE schedule_name = ?2
            AND next_fire_at_ms = ?3
            AND lease_token = ?4
        `,
      ).run(nextFireAtMs, scheduleName, fireAtMs, leaseToken);
    },
    async beginTransaction() {
      db.exec("BEGIN IMMEDIATE");
    },
    async claimNextCronSchedule(leaseMs: number): Promise<(PersistedCronScheduleRow & { lease_token: string }) | null> {
      const now = platform.now();
      const leaseToken = platform.randomUUID();
      const leaseExpiresAtMs = now + leaseMs;

      db.exec("BEGIN IMMEDIATE");
      try {
        const [schedule] = parseRows(db.query(
          `
            SELECT
              schedule_name,
              cron_expression,
              next_fire_at_ms
            FROM _chimpbase_cron_schedules
            WHERE next_fire_at_ms <= ?1
              AND (
                lease_token IS NULL
                OR lease_expires_at_ms IS NULL
                OR lease_expires_at_ms <= ?1
              )
            ORDER BY next_fire_at_ms ASC, schedule_name ASC
            LIMIT 1
          `,
        ).all(now), cronScheduleRowValidator, "claimed cron schedule rows");

        if (!(schedule !== null && schedule !== undefined)) {
          db.exec("COMMIT");
          return null;
        }

        db.query(
          `
            UPDATE _chimpbase_cron_schedules
            SET
              lease_token = ?1,
              lease_expires_at_ms = ?2,
              updated_at = CURRENT_TIMESTAMP
            WHERE schedule_name = ?3
          `,
        ).run(leaseToken, leaseExpiresAtMs, schedule.schedule_name);

        db.exec("COMMIT");
        return {
          ...schedule,
          lease_token: leaseToken,
        };
      } catch (error) {
        try {
          db.exec("ROLLBACK");
        } catch {
        }

        throw error;
      }
    },
    async claimNextQueueJob(
      leaseMs: number,
      queueNames: readonly string[],
    ): Promise<ChimpbaseQueueJobRecord | null> {
      const now = platform.now();
      const leaseExpiresAtMs = now + leaseMs;
      const queueFilter = buildSqliteQueueNameFilter(queueNames, 2);

      db.exec("BEGIN IMMEDIATE");
      try {
        const [job] = parseRows(db.query(
          `
            SELECT
              id,
              queue_name,
              payload_json,
              attempt_count
            FROM _chimpbase_queue_jobs
            WHERE ${queueFilter.sql}
              AND (
                (status = 'pending' AND available_at_ms <= ?1)
                OR
                (status = 'processing' AND lease_expires_at_ms IS NOT NULL AND lease_expires_at_ms <= ?1)
              )
            ORDER BY id ASC
            LIMIT 1
          `,
        ).all(now, ...queueFilter.params), queueJobRowValidator, "claimed queue job rows");

        if (!(job !== null && job !== undefined)) {
          db.exec("COMMIT");
          return null;
        }

        db.query(
          `
            UPDATE _chimpbase_queue_jobs
            SET
              status = 'processing',
              attempt_count = attempt_count + 1,
              lease_expires_at_ms = ?1,
              updated_at = CURRENT_TIMESTAMP
            WHERE id = ?2
          `,
        ).run(leaseExpiresAtMs, job.id);

        db.exec("COMMIT");
        return {
          ...job,
          attempt_count: job.attempt_count + 1,
        };
      } catch (error) {
        try {
          db.exec("ROLLBACK");
        } catch {
        }

        throw error;
      }
    },
    async collectionDelete(name: string, filter: ChimpbaseCollectionFilter = {}): Promise<number> {
      const matched = findCollectionDocuments(db, name, filter);
      for (const row of matched) {
        db.query(
          "DELETE FROM _chimpbase_collections WHERE collection_name = ?1 AND document_id = ?2",
        ).run(name, row.document_id);
      }
      return matched.length;
    },
    async collectionFind<TDocument>(
      name: string,
      filter: ChimpbaseCollectionFilter,
      options: ChimpbaseCollectionFindOptions | undefined,
      validator: ChimpbaseValidator<TDocument>,
    ): Promise<TDocument[]> {
      return findCollectionDocuments(db, name, filter, options).map((row) =>
        validator.parse(parseJson(row.document_json, `collection ${name} document ${row.document_id}`), `collection ${name} document ${row.document_id}`)
      );
    },
    async collectionFindOne<TDocument>(
      name: string,
      filter: ChimpbaseCollectionFilter,
      validator: ChimpbaseValidator<TDocument>,
    ): Promise<TDocument | null> {
      const [row] = findCollectionDocuments(db, name, filter, { limit: 1 });
      return row === undefined
        ? null
        : validator.parse(parseJson(row.document_json, `collection ${name} document ${row.document_id}`), `collection ${name} document ${row.document_id}`);
    },
    async collectionInsert<TDocument extends Record<string, unknown>>(name: string, document: TDocument): Promise<string> {
      const documentId = platform.randomUUID();
      const payload = JSON.stringify({ ...document, id: documentId });
      db.query(
        `
          INSERT INTO _chimpbase_collections (
            collection_name,
            document_id,
            document_json,
            created_at,
            updated_at
          ) VALUES (?1, ?2, ?3, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        `,
      ).run(name, documentId, payload);
      return documentId;
    },
    async collectionList(): Promise<string[]> {
      const rows = parseRows(db.query(
        `
          SELECT DISTINCT collection_name
          FROM _chimpbase_collections
          ORDER BY collection_name ASC
        `,
      ).all(), collectionNameRowValidator, "collection name rows");
      return rows.map((row) => row.collection_name);
    },
    async collectionUpdate(name: string, filter: ChimpbaseCollectionFilter, patch: ChimpbaseCollectionPatch): Promise<number> {
      const matched = findCollectionDocuments(db, name, filter);
      for (const row of matched) {
        const current = parseJsonObject(row.document_json, "collection document");
        const next = JSON.stringify({ ...current, ...patch });
        db.query(
          `
            UPDATE _chimpbase_collections
            SET document_json = ?1, updated_at = CURRENT_TIMESTAMP
            WHERE collection_name = ?2 AND document_id = ?3
          `,
        ).run(next, name, row.document_id);
      }
      return matched.length;
    },
    async commitTransaction(events: ChimpbaseEventRecord[]) {
      persistEvents(db, events);
      const availableAtMs = platform.now();
      const statement = db.query(
        `INSERT INTO _chimpbase_queue_jobs (
          queue_name, payload_json, status, available_at_ms, attempt_count
        ) VALUES (?1, ?2, 'pending', ?3, 0)`,
      );
      for (const event of events) {
        for (const payload of createChimpbaseEventDeliveryPayloads(event)) {
          statement.run(
            "__chimpbase.subscription.run",
            JSON.stringify(payload),
            availableAtMs,
          );
        }
      }
      db.exec("COMMIT");
    },
    async completeQueueJob(jobId: number) {
      db.query(
        `
          UPDATE _chimpbase_queue_jobs
          SET
            status = 'completed',
            completed_at = CURRENT_TIMESTAMP,
            lease_expires_at_ms = NULL,
            updated_at = CURRENT_TIMESTAMP
          WHERE id = ?1
        `,
      ).run(jobId);
    },
    async deleteCronSchedule(scheduleName: string): Promise<void> {
      db.query(
        "DELETE FROM _chimpbase_cron_schedules WHERE schedule_name = ?1",
      ).run(scheduleName);
    },
    async getQueueJobPayload(jobId: number): Promise<string | null> {
      const [job] = parseRows(db.query(
        "SELECT payload_json FROM _chimpbase_queue_jobs WHERE id = ?1 LIMIT 1",
      ).all(jobId), payloadJsonRowValidator, "queue payload rows");
      return job?.payload_json ?? null;
    },
    async insertCronRun(scheduleName: string, fireAtMs: number): Promise<boolean> {
      const result = db.query(
        `
          INSERT OR IGNORE INTO _chimpbase_cron_runs (
            schedule_name,
            fire_at_ms
          ) VALUES (?1, ?2)
        `,
      ).run(scheduleName, fireAtMs);

      return result.changes > 0;
    },
    async kvDelete(key: string) {
      db.query("DELETE FROM _chimpbase_kv WHERE key = ?1").run(key);
    },
    async kvGet<TValue>(
      key: string,
      validator: ChimpbaseValidator<TValue>,
    ): Promise<TValue | null> {
      const [row] = parseRows(db.query(
        `
          SELECT value_json
          FROM _chimpbase_kv
          WHERE key = ?1 AND (expires_at IS NULL OR expires_at > datetime('now'))
          LIMIT 1
        `,
      ).all(key), valueJsonRowValidator, "key-value rows");
      return row === undefined
        ? null
        : validator.parse(parseJson(row.value_json, `key-value entry ${key}`), `key-value entry ${key}`);
    },
    async kvList(options?: ChimpbaseKvListOptions): Promise<string[]> {
      const prefix = options?.prefix ?? "";
      const rows = parseRows(db.query(
        `
          SELECT key
          FROM _chimpbase_kv
          WHERE key LIKE ?1 AND (expires_at IS NULL OR expires_at > datetime('now'))
          ORDER BY key ASC
        `,
      ).all(`${prefix}%`), keyRowValidator, "key list rows");
      return rows.map((row) => row.key);
    },
    async kvSet<TValue = unknown>(key: string, value: TValue, ttlMs?: number) {
      const expiresAt = ttlMs !== undefined ? new Date(Date.now() + ttlMs).toISOString() : null;
      db.query(
        `
          INSERT INTO _chimpbase_kv (key, value_json, updated_at, expires_at)
          VALUES (?1, ?2, CURRENT_TIMESTAMP, ?3)
          ON CONFLICT(key) DO UPDATE SET
            value_json = excluded.value_json,
            updated_at = CURRENT_TIMESTAMP,
            expires_at = excluded.expires_at
        `,
      ).run(key, JSON.stringify(value ?? null), expiresAt);
    },
    async listCronSchedules(): Promise<PersistedCronScheduleRow[]> {
      return parseRows(db.query(
        `
          SELECT
            schedule_name,
            cron_expression,
            next_fire_at_ms
          FROM _chimpbase_cron_schedules
          ORDER BY schedule_name ASC
        `,
      ).all(), cronScheduleRowValidator, "cron schedule rows");
    },
    async markQueueJobFailure(
      jobId: number,
      status: "dlq" | "failed" | "pending",
      nextAvailableAtMs: number,
      errorMessage: string,
    ) {
      db.query(
        `
          UPDATE _chimpbase_queue_jobs
          SET
            status = ?1,
            available_at_ms = ?2,
            lease_expires_at_ms = NULL,
            last_error = ?3,
            updated_at = CURRENT_TIMESTAMP
          WHERE id = ?4
        `,
      ).run(status, nextAvailableAtMs, errorMessage, jobId);
    },
    createKysely<TDatabase = Record<string, never>>(): Kysely<TDatabase> {
      return createSqliteKysely<TDatabase>({
        executeQuery<R>(compiledQuery: CompiledQuery): Promise<QueryResult<R>> {
          const statement = db.query(compiledQuery.sql);

          if (statement.columnNames.length > 0) {
            return Promise.resolve({
              rows: parseKyselyRows<R>(statement.all(...toSqlBindings(compiledQuery.parameters))),
            });
          }

          const result = statement.run(...toSqlBindings(compiledQuery.parameters));

          return Promise.resolve({
            insertId: result.lastInsertRowid === undefined ? undefined : BigInt(result.lastInsertRowid),
            numAffectedRows: BigInt(result.changes),
            rows: [],
          });
        },
      });
    },
    async query<T>(
      sql: string,
      params: readonly unknown[],
      validator: ChimpbaseValidator<T>,
    ): Promise<T[]> {
      return runQuery(db, sql, toSqlBindings(params), validator);
    },
    async queueEnqueue<TPayload = unknown>(name: string, payload: TPayload, options?: ChimpbaseQueueEnqueueOptions) {
      const availableAtMs = platform.now() + Math.max(0, options?.delayMs ?? 0);
      db.query(
        `
          INSERT INTO _chimpbase_queue_jobs (
            queue_name,
            payload_json,
            status,
            available_at_ms,
            attempt_count
          ) VALUES (?1, ?2, 'pending', ?3, 0)
        `,
      ).run(name, JSON.stringify(payload ?? null), availableAtMs);
    },
    async releaseCronScheduleLease(scheduleName: string, leaseToken: string): Promise<void> {
      db.query(
        `
          UPDATE _chimpbase_cron_schedules
          SET
            lease_token = NULL,
            lease_expires_at_ms = NULL,
            updated_at = CURRENT_TIMESTAMP
          WHERE schedule_name = ?1 AND lease_token = ?2
        `,
      ).run(scheduleName, leaseToken);
    },
    async rollbackTransaction() {
      try {
        db.exec("ROLLBACK");
      } catch {
      }
    },
    async streamAppend<TPayload = unknown>(stream: string, event: string, payload: TPayload): Promise<number> {
      db.query(
        `
          INSERT INTO _chimpbase_stream_events (
            stream_name,
            event_name,
            payload_json
          ) VALUES (?1, ?2, ?3)
        `,
      ).run(stream, event, JSON.stringify(payload ?? null));

      const [row] = parseRows(db.query(
        "SELECT last_insert_rowid() AS id",
      ).all(), idRowValidator, "insert id rows");
      return row?.id ?? 0;
    },
    async streamRead<TPayload>(
      stream: string,
      options: ChimpbaseStreamReadOptions | undefined,
      validator: ChimpbaseValidator<TPayload>,
    ): Promise<ChimpbaseStreamEvent<TPayload>[]> {
      const sinceId = options?.sinceId ?? 0;
      const limit = options?.limit ?? 100;
      const rows = parseRows(db.query(
        `
          SELECT
            id,
            stream_name,
            event_name,
            payload_json,
            created_at
          FROM _chimpbase_stream_events
          WHERE stream_name = ?1 AND id > ?2
          ORDER BY id ASC
          LIMIT ?3
        `,
      ).all(stream, sinceId, limit), streamEventRowValidator, "stream event rows");

      return rows.map((row) => ({
        createdAt: row.created_at,
        event: row.event_name,
        id: row.id,
        payload: validator.parse(parseJson(row.payload_json, `stream ${stream} event ${row.id}`), `stream ${stream} event ${row.id}`),
        stream: row.stream_name,
      }));
    },
    async upsertCronSchedule(scheduleName: string, cronExpression: string, nextFireAtMs: number): Promise<void> {
      db.query(
        `
          INSERT INTO _chimpbase_cron_schedules (
            schedule_name,
            cron_expression,
            next_fire_at_ms,
            lease_token,
            lease_expires_at_ms
          ) VALUES (?1, ?2, ?3, NULL, NULL)
          ON CONFLICT(schedule_name) DO UPDATE SET
            cron_expression = excluded.cron_expression,
            next_fire_at_ms = excluded.next_fire_at_ms,
            lease_token = NULL,
            lease_expires_at_ms = NULL,
            updated_at = CURRENT_TIMESTAMP
        `,
      ).run(scheduleName, cronExpression, nextFireAtMs);
    },
    async blobPutMetadata(row: ChimpbaseBlobMetaRow) {
      db.query(
        `
          INSERT INTO _chimpbase_blobs (
            bucket, key, size, etag, content_type, metadata_json, driver_ref, created_at, updated_at
          ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
          ON CONFLICT(bucket, key) DO UPDATE SET
            size = excluded.size,
            etag = excluded.etag,
            content_type = excluded.content_type,
            metadata_json = excluded.metadata_json,
            driver_ref = excluded.driver_ref,
            updated_at = excluded.updated_at
        `,
      ).run(
        row.bucket,
        row.key,
        row.size,
        row.etag,
        row.contentType,
        JSON.stringify(row.metadata),
        row.driverRef,
        row.createdAt,
        row.updatedAt,
      );
    },
    async blobGetMetadata(bucket: string, key: string): Promise<ChimpbaseBlobMetaRow | null> {
      const [row] = parseRows(db.query(
        `
          SELECT bucket, key, size, etag, content_type, metadata_json, driver_ref, created_at, updated_at
          FROM _chimpbase_blobs
          WHERE bucket = ?1 AND key = ?2
          LIMIT 1
        `,
      ).all(bucket, key), blobMetadataRowValidator, "blob metadata rows");
      if (!(row !== null && row !== undefined)) return null;
      return {
        bucket: row.bucket,
        key: row.key,
        size: Number(row.size),
        etag: row.etag,
        contentType: row.content_type,
        metadata: parseStringRecord(row.metadata_json, "blob metadata"),
        driverRef: row.driver_ref,
        createdAt: row.created_at,
        updatedAt: row.updated_at,
      };
    },
    async blobDeleteMetadata(bucket: string, key: string): Promise<boolean> {
      const result = db.query(
        "DELETE FROM _chimpbase_blobs WHERE bucket = ?1 AND key = ?2",
      ).run(bucket, key);
      return result.changes > 0;
    },
    async blobListMetadata(
      bucket: string,
      options: ChimpbaseBlobListOptions,
    ): Promise<ChimpbaseBlobListMetaResult> {
      const prefix = options.prefix ?? "";
      const delimiter = options.delimiter ?? null;
      const cursor = options.cursor ?? "";
      const limit = Math.min(Math.max(options.limit ?? 1000, 1), 1000);
      const rows = parseRows(db.query(
        `
          SELECT bucket, key, size, etag, content_type, metadata_json, driver_ref, created_at, updated_at
          FROM _chimpbase_blobs
          WHERE bucket = ?1 AND key LIKE ?2 AND key > ?3
          ORDER BY key ASC
          LIMIT ?4
        `,
      ).all(bucket, `${prefix}%`, cursor, limit + 1), blobMetadataRowValidator, "blob metadata rows");
      const mapped: ChimpbaseBlobMetaRow[] = rows.map((row) => ({
        bucket: row.bucket,
        key: row.key,
        size: Number(row.size),
        etag: row.etag,
        contentType: row.content_type,
        metadata: parseStringRecord(row.metadata_json, "blob metadata"),
        driverRef: row.driver_ref,
        createdAt: row.created_at,
        updatedAt: row.updated_at,
      }));
      return sliceBlobList(mapped, prefix, delimiter, limit);
    },
    async blobInitUpload(row: ChimpbaseBlobUploadRow): Promise<void> {
      db.query(
        `
          INSERT INTO _chimpbase_blob_uploads (
            upload_id, bucket, key, content_type, metadata_json, driver_ref, created_at_ms, expires_at_ms
          ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
        `,
      ).run(
        row.uploadId,
        row.bucket,
        row.key,
        row.contentType,
        JSON.stringify(row.metadata),
        row.driverRef,
        row.createdAtMs,
        row.expiresAtMs,
      );
    },
    async blobGetUpload(uploadId: string): Promise<ChimpbaseBlobUploadRow | null> {
      const [row] = parseRows(db.query(
        `
          SELECT upload_id, bucket, key, content_type, metadata_json, driver_ref, created_at_ms, expires_at_ms
          FROM _chimpbase_blob_uploads
          WHERE upload_id = ?1
          LIMIT 1
        `,
      ).all(uploadId), blobUploadRowValidator, "blob upload rows");
      if (row === undefined) return null;
      return {
        uploadId: row.upload_id,
        bucket: row.bucket,
        key: row.key,
        contentType: row.content_type,
        metadata: parseStringRecord(row.metadata_json, "blob metadata"),
        driverRef: row.driver_ref,
        createdAtMs: Number(row.created_at_ms),
        expiresAtMs: Number(row.expires_at_ms),
      };
    },
    async blobRecordPart(row: ChimpbaseBlobPartRow): Promise<void> {
      db.query(
        `
          INSERT INTO _chimpbase_blob_upload_parts (
            upload_id, part_number, size, etag, driver_ref, created_at
          ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
          ON CONFLICT(upload_id, part_number) DO UPDATE SET
            size = excluded.size,
            etag = excluded.etag,
            driver_ref = excluded.driver_ref,
            created_at = excluded.created_at
        `,
      ).run(row.uploadId, row.partNumber, row.size, row.etag, row.driverRef, row.createdAt);
    },
    async blobListParts(uploadId: string): Promise<ChimpbaseBlobPartRow[]> {
      const rows = parseRows(db.query(
        `
          SELECT upload_id, part_number, size, etag, driver_ref, created_at
          FROM _chimpbase_blob_upload_parts
          WHERE upload_id = ?1
          ORDER BY part_number ASC
        `,
      ).all(uploadId), blobPartRowValidator, "blob part rows");
      return rows.map((row) => ({
        uploadId: row.upload_id,
        partNumber: row.part_number,
        size: Number(row.size),
        etag: row.etag,
        driverRef: row.driver_ref,
        createdAt: row.created_at,
      }));
    },
    async blobFinalizeUpload(uploadId: string, finalMeta: ChimpbaseBlobMetaRow): Promise<void> {
      db.query(
        `
          INSERT INTO _chimpbase_blobs (
            bucket, key, size, etag, content_type, metadata_json, driver_ref, created_at, updated_at
          ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
          ON CONFLICT(bucket, key) DO UPDATE SET
            size = excluded.size,
            etag = excluded.etag,
            content_type = excluded.content_type,
            metadata_json = excluded.metadata_json,
            driver_ref = excluded.driver_ref,
            updated_at = excluded.updated_at
        `,
      ).run(
        finalMeta.bucket,
        finalMeta.key,
        finalMeta.size,
        finalMeta.etag,
        finalMeta.contentType,
        JSON.stringify(finalMeta.metadata),
        finalMeta.driverRef,
        finalMeta.createdAt,
        finalMeta.updatedAt,
      );
      db.query("DELETE FROM _chimpbase_blob_uploads WHERE upload_id = ?1").run(uploadId);
    },
    async blobAbortUpload(uploadId: string): Promise<void> {
      db.query("DELETE FROM _chimpbase_blob_uploads WHERE upload_id = ?1").run(uploadId);
    },
    async blobListUploads(
      bucket: string,
      options: ChimpbaseBlobUploadListOptions,
    ): Promise<ChimpbaseBlobUploadListMetaResult> {
      const prefix = options.prefix ?? "";
      const cursor = options.cursor ?? "";
      const limit = Math.min(Math.max(options.limit ?? 100, 1), 1000);
      const rows = parseRows(db.query(
        `
          SELECT upload_id, bucket, key, content_type, metadata_json, driver_ref, created_at_ms, expires_at_ms
          FROM _chimpbase_blob_uploads
          WHERE bucket = ?1 AND key LIKE ?2 AND upload_id > ?3
          ORDER BY upload_id ASC
          LIMIT ?4
        `,
      ).all(bucket, `${prefix}%`, cursor, limit + 1), blobUploadRowValidator, "blob upload rows");
      const mapped: ChimpbaseBlobUploadRow[] = rows.map((row) => ({
        uploadId: row.upload_id,
        bucket: row.bucket,
        key: row.key,
        contentType: row.content_type,
        metadata: parseStringRecord(row.metadata_json, "blob metadata"),
        driverRef: row.driver_ref,
        createdAtMs: Number(row.created_at_ms),
        expiresAtMs: Number(row.expires_at_ms),
      }));
      const hasMore = mapped.length > limit;
      const page = hasMore ? mapped.slice(0, limit) : mapped;
      return {
        uploads: page,
        nextCursor: hasMore ? page[page.length - 1].uploadId : null,
      };
    },
    async blobGcExpiredUploads(nowMs: number): Promise<string[]> {
      const rows = parseRows(db.query(
        "SELECT upload_id FROM _chimpbase_blob_uploads WHERE expires_at_ms <= ?1",
      ).all(nowMs), uploadIdRowValidator, "expired upload rows");
      db.query("DELETE FROM _chimpbase_blob_uploads WHERE expires_at_ms <= ?1").run(nowMs);
      return rows.map((row) => row.upload_id);
    },
  };
}

function sliceBlobList(
  rows: ChimpbaseBlobMetaRow[],
  prefix: string,
  delimiter: string | null,
  limit: number,
): ChimpbaseBlobListMetaResult {
  const entries: ChimpbaseBlobMetaRow[] = [];
  const commonPrefixes = new Set<string>();
  let nextCursor: string | null = null;
  for (const row of rows) {
    if (entries.length + commonPrefixes.size >= limit) {
      nextCursor = entries.length > 0 ? entries[entries.length - 1].key : row.key;
      break;
    }
    if ((delimiter !== null && delimiter.length > 0)) {
      const after = row.key.slice(prefix.length);
      const idx = after.indexOf(delimiter);
      if (idx >= 0) {
        commonPrefixes.add(prefix + after.slice(0, idx + delimiter.length));
        continue;
      }
    }
    entries.push(row);
  }
  if (!(nextCursor !== null && nextCursor.length > 0) && rows.length > limit) {
    nextCursor = rows[limit - 1]?.key ?? null;
  }
  return {
    entries,
    commonPrefixes: [...commonPrefixes].sort(),
    nextCursor,
  };
}

function createSqliteDatabase(db: DatabaseSync): SqliteDatabase {
  return {
    close() {
      db.close();
    },
    exec(sql: string) {
      return db.exec(sql);
    },
    query(sql: string): SqliteStatement {
      const statement = db.prepare(sql);
      return {
        columnNames: statement.columns().map((column) => column.name),
        all(...params: SqliteBinding[]) {
          return statement.all(...toNodeSqlBindings(params));
        },
        run(...params: SqliteBinding[]) {
          const result = statement.run(...toNodeSqlBindings(params));
          return {
            changes: typeof result.changes === "bigint" ? Number(result.changes) : result.changes,
            lastInsertRowid: result.lastInsertRowid,
          };
        },
      };
    },
  };
}

function runQuery<T>(
  db: SqliteDatabase,
  sql: string,
  params: SqliteBinding[],
  validator: ChimpbaseValidator<T>,
): T[] {
  const statement = db.query(sql);
  if (statement.columnNames.length === 0) {
    statement.run(...params);
    return [];
  }

  return parseRows(statement.all(...params), validator, "database query rows");
}

function toSqlBindings(params: readonly unknown[]): SqliteBinding[] {
  return [...params];
}
function isNodeSqlInputValue(value: unknown): value is SQLInputValue {
  return value === null
    || typeof value === "string"
    || typeof value === "number"
    || typeof value === "bigint"
    || ArrayBuffer.isView(value);
}

function toNodeSqlBindings(params: readonly unknown[]): SQLInputValue[] {
  const bindings: SQLInputValue[] = [];
  for (const param of params) {
    if (typeof param === "boolean") {
      bindings.push(param ? 1 : 0);
    } else if (isNodeSqlInputValue(param)) {
      bindings.push(param);
    } else {
      throw new TypeError("SQLite query parameters must be supported binding values");
    }
  }
  return bindings;
}


function persistEvents(db: SqliteDatabase, events: ChimpbaseEventRecord[]): void {
  if (events.length === 0) {
    return;
  }

  const statement = db.query(
    "INSERT INTO _chimpbase_events (event_name, payload_json) VALUES (?1, ?2)",
  );
  for (const event of events) {
    const result = statement.run(event.name, event.payloadJson);
    event.id = result.lastInsertRowid === undefined ? undefined : Number(result.lastInsertRowid);
  }
}

function findCollectionDocuments(
  db: SqliteDatabase,
  name: string,
  filter: ChimpbaseCollectionFilter = {},
  options?: ChimpbaseCollectionFindOptions,
): PersistedCollectionDocument[] {
  const rows = parseRows(db.query(
    `
      SELECT
        document_id,
        document_json
      FROM _chimpbase_collections
      WHERE collection_name = ?1
      ORDER BY document_id ASC
    `,
  ).all(name), collectionDocumentRowValidator, "collection document rows");

  const matched = rows.filter((row) => {
    const document = parseJsonObject(row.document_json, "collection document");
    return Object.entries(filter).every(([key, value]) => document[key] === value);
  });

  const limit = options?.limit;
  return typeof limit === "number" ? matched.slice(0, limit) : matched;
}
