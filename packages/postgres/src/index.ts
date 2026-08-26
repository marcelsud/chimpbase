import type { CompiledQuery, Kysely, QueryResult } from "kysely";
import { Pool, type PoolClient } from "pg";

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
import {
  assertChimpbaseModuleCompiledSql,
  createChimpbaseEventDeliveryPayloads,
} from "@chimpbase/core";
import type {
  ChimpbaseBlobListOptions,
  ChimpbaseBlobUploadListOptions,
  ChimpbaseCollectionFilter,
  ChimpbaseCollectionFindOptions,
  ChimpbaseValidator,
  ChimpbaseCollectionPatch,
  ChimpbaseKvListOptions,
  ChimpbaseQueueEnqueueOptions,
  ChimpbaseStreamEvent,
  ChimpbaseStreamReadOptions,
} from "@chimpbase/runtime";
import { isArrayValue, isJsonObject, parseJson, parseJsonObject, parseStringRecord, v } from "@chimpbase/runtime";

import { createPostgresKysely } from "./kysely.ts";

export { PostgresPollingEventBus, type PostgresPollingEventBusOptions } from "./event-bus.ts";
export {
  PayloadTooLargeError,
  PostgresListenEventBus,
  type PostgresListenEventBusOptions,
} from "./listen-event-bus.ts";

interface PersistedCollectionDocument {
  document_id: string;
  document_json: string;
}

interface PersistedCronScheduleRow {
  cron_expression: string;
  next_fire_at_ms: number;
  schedule_name: string;
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


type Queryable = Pool | PoolClient;

export function openPostgresPool(config: ChimpbaseProjectConfig): Pool {
  if (!(config.storage.url !== null && config.storage.url.length > 0)) {
    throw new Error("postgres storage requires storage.url");
  }

  return new Pool({
    connectionString: config.storage.url,
  });
}

export async function applyPostgresSqlMigrations(
  pool: Pool,
  migrations: readonly string[],
): Promise<void> {
  for (const migration of migrations) {
    await pool.query(migration);
  }
}

export async function applyInlinePostgresMigrations(pool: Pool, migrations: string[]): Promise<void> {
  for (const migration of migrations) {
    await pool.query(migration);
  }
}

export async function ensurePostgresInternalTables(pool: Pool): Promise<void> {
  await pool.query(
    `
      CREATE TABLE IF NOT EXISTS _chimpbase_events (
        id BIGSERIAL PRIMARY KEY,
        event_name TEXT NOT NULL,
        payload_json JSONB NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `,
  );

  await pool.query(
    `
      CREATE TABLE IF NOT EXISTS _chimpbase_kv (
        key TEXT PRIMARY KEY,
        value_json JSONB NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        expires_at TIMESTAMPTZ DEFAULT NULL
      )
    `,
  );

  await pool.query(
    `
      CREATE TABLE IF NOT EXISTS _chimpbase_collections (
        collection_name TEXT NOT NULL,
        document_id TEXT NOT NULL,
        document_json JSONB NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (collection_name, document_id)
      )
    `,
  );

  await pool.query(
    `
      CREATE TABLE IF NOT EXISTS _chimpbase_stream_events (
        id BIGSERIAL PRIMARY KEY,
        stream_name TEXT NOT NULL,
        event_name TEXT NOT NULL,
        payload_json JSONB NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `,
  );

  await pool.query(
    `
      CREATE TABLE IF NOT EXISTS _chimpbase_queue_jobs (
        id BIGSERIAL PRIMARY KEY,
        queue_name TEXT NOT NULL,
        payload_json JSONB NOT NULL,
        status TEXT NOT NULL,
        available_at_ms BIGINT NOT NULL,
        attempt_count INTEGER NOT NULL DEFAULT 0,
        lease_expires_at_ms BIGINT,
        last_error TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        completed_at TIMESTAMPTZ
      )
    `,
  );

  await pool.query(
    `
      CREATE INDEX IF NOT EXISTS idx_chimpbase_queue_jobs_pending_due
      ON _chimpbase_queue_jobs(available_at_ms, id)
      WHERE status = 'pending'
    `,
  );

  await pool.query(
    `
      CREATE INDEX IF NOT EXISTS idx_chimpbase_queue_jobs_processing_due
      ON _chimpbase_queue_jobs(lease_expires_at_ms, id)
      WHERE status = 'processing' AND lease_expires_at_ms IS NOT NULL
    `,
  );

  await pool.query(
    `
      CREATE TABLE IF NOT EXISTS _chimpbase_cron_schedules (
        schedule_name TEXT PRIMARY KEY,
        cron_expression TEXT NOT NULL,
        next_fire_at_ms BIGINT NOT NULL,
        lease_token TEXT,
        lease_expires_at_ms BIGINT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `,
  );

  await pool.query(
    `
      CREATE INDEX IF NOT EXISTS idx_chimpbase_cron_schedules_due
      ON _chimpbase_cron_schedules(next_fire_at_ms, lease_expires_at_ms)
    `,
  );

  await pool.query(
    `
      CREATE TABLE IF NOT EXISTS _chimpbase_cron_runs (
        schedule_name TEXT NOT NULL,
        fire_at_ms BIGINT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (schedule_name, fire_at_ms)
      )
    `,
  );

  await pool.query(
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
        wake_at_ms BIGINT,
        last_error TEXT,
        lease_token TEXT,
        lease_expires_at_ms BIGINT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        completed_at TIMESTAMPTZ
      )
    `,
  );

  await pool.query(
    `
      ALTER TABLE _chimpbase_workflow_instances
      ADD COLUMN IF NOT EXISTS current_step_id TEXT
    `,
  );

  await pool.query(
    `
      CREATE INDEX IF NOT EXISTS idx_chimpbase_workflow_instances_status
      ON _chimpbase_workflow_instances(status, wake_at_ms)
    `,
  );

  await pool.query(
    `
      ALTER TABLE _chimpbase_kv
      ADD COLUMN IF NOT EXISTS expires_at TIMESTAMPTZ DEFAULT NULL
    `,
  );

  await pool.query(
    `
      CREATE TABLE IF NOT EXISTS _chimpbase_workflow_signals (
        id BIGSERIAL PRIMARY KEY,
        workflow_id TEXT NOT NULL,
        signal_name TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        consumed_at TIMESTAMPTZ,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `,
  );

  await pool.query(
    `
      CREATE INDEX IF NOT EXISTS idx_chimpbase_workflow_signals_pending
      ON _chimpbase_workflow_signals(workflow_id, signal_name, consumed_at, id)
    `,
  );

  await pool.query(
    `
      CREATE TABLE IF NOT EXISTS _chimpbase_blobs (
        bucket TEXT NOT NULL,
        key TEXT NOT NULL,
        size BIGINT NOT NULL,
        etag TEXT NOT NULL,
        content_type TEXT NOT NULL DEFAULT 'application/octet-stream',
        metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
        driver_ref TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (bucket, key)
      )
    `,
  );

  await pool.query(
    `
      CREATE INDEX IF NOT EXISTS idx_chimpbase_blobs_bucket_prefix
      ON _chimpbase_blobs (bucket, key text_pattern_ops)
    `,
  );

  await pool.query(
    `
      CREATE TABLE IF NOT EXISTS _chimpbase_blob_uploads (
        upload_id TEXT PRIMARY KEY,
        bucket TEXT NOT NULL,
        key TEXT NOT NULL,
        content_type TEXT,
        metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
        driver_ref TEXT NOT NULL,
        created_at_ms BIGINT NOT NULL,
        expires_at_ms BIGINT NOT NULL
      )
    `,
  );

  await pool.query(
    `
      CREATE INDEX IF NOT EXISTS idx_chimpbase_blob_uploads_expires
      ON _chimpbase_blob_uploads (expires_at_ms)
    `,
  );

  await pool.query(
    `
      CREATE INDEX IF NOT EXISTS idx_chimpbase_blob_uploads_bucket_key
      ON _chimpbase_blob_uploads (bucket, key)
    `,
  );

  await pool.query(
    `
      CREATE TABLE IF NOT EXISTS _chimpbase_blob_upload_parts (
        upload_id TEXT NOT NULL REFERENCES _chimpbase_blob_uploads(upload_id) ON DELETE CASCADE,
        part_number INTEGER NOT NULL,
        size BIGINT NOT NULL,
        etag TEXT NOT NULL,
        driver_ref TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (upload_id, part_number)
      )
    `,
  );
}

export function createPostgresEngineAdapter(
  pool: Pool,
  platform: ChimpbasePlatformShim,
): ChimpbaseEngineAdapter {
  let transactionClient: PoolClient | null = null;

  const queryable = (): Queryable => transactionClient ?? pool;

  return {
    async advanceCronSchedule(
      scheduleName: string,
      fireAtMs: number,
      nextFireAtMs: number,
      leaseToken: string,
    ) {
      const result = await queryable().query(
        `
          UPDATE _chimpbase_cron_schedules
          SET
            next_fire_at_ms = $1,
            lease_token = NULL,
            lease_expires_at_ms = NULL,
            updated_at = NOW()
          WHERE schedule_name = $2
            AND next_fire_at_ms = $3
            AND lease_token = $4
        `,
        [nextFireAtMs, scheduleName, fireAtMs, leaseToken],
      );

      if ((result.rowCount ?? 0) === 0) {
        throw new Error(`cron schedule advance failed: ${scheduleName}`);
      }
    },
    async beginTransaction() {
      if ((transactionClient !== null)) {
        return;
      }

      transactionClient = await pool.connect();
      await transactionClient.query("BEGIN");
    },
    async claimNextCronSchedule(leaseMs: number): Promise<(PersistedCronScheduleRow & { lease_token: string }) | null> {
      const now = platform.now();
      const leaseToken = platform.randomUUID();
      const leaseExpiresAtMs = now + leaseMs;
      const result = await queryable().query<PersistedCronScheduleRow & { lease_token: string }>(
        `
          WITH candidate AS (
            SELECT
              schedule_name,
              cron_expression,
              next_fire_at_ms::double precision AS next_fire_at_ms
            FROM _chimpbase_cron_schedules
            WHERE next_fire_at_ms <= $1
              AND (
                lease_token IS NULL
                OR lease_expires_at_ms IS NULL
                OR lease_expires_at_ms <= $1
              )
            ORDER BY next_fire_at_ms ASC, schedule_name ASC
            FOR UPDATE SKIP LOCKED
            LIMIT 1
          )
          UPDATE _chimpbase_cron_schedules s
          SET
            lease_token = $2,
            lease_expires_at_ms = $3,
            updated_at = NOW()
          FROM candidate
          WHERE s.schedule_name = candidate.schedule_name
          RETURNING candidate.schedule_name, candidate.cron_expression, candidate.next_fire_at_ms, s.lease_token
        `,
        [now, leaseToken, leaseExpiresAtMs],
      );

      return result.rows[0] ?? null;
    },
    async claimNextQueueJob(leaseMs: number, queueNames: readonly string[]): Promise<ChimpbaseQueueJobRecord | null> {
      if (queueNames.length === 0) {
        return null;
      }

      const now = platform.now();
      const leaseExpiresAtMs = now + leaseMs;
      const result = await queryable().query<ChimpbaseQueueJobRecord>(
        `
          WITH candidate AS (
            SELECT id
            FROM _chimpbase_queue_jobs
            WHERE queue_name = ANY($3::text[])
              AND (
                (status = 'pending' AND available_at_ms <= $1)
                OR
                (status = 'processing' AND lease_expires_at_ms IS NOT NULL AND lease_expires_at_ms <= $1)
              )
            ORDER BY id ASC
            FOR UPDATE SKIP LOCKED
            LIMIT 1
          )
          UPDATE _chimpbase_queue_jobs q
          SET
            status = 'processing',
            attempt_count = q.attempt_count + 1,
            lease_expires_at_ms = $2,
            updated_at = NOW()
          FROM candidate
          WHERE q.id = candidate.id
          RETURNING q.id, q.queue_name, q.payload_json::text, q.attempt_count
        `,
        [now, leaseExpiresAtMs, queueNames],
      );
      return result.rows[0] ?? null;
    },
    async claimNextQueueJobs(
      leaseMs: number,
      limit: number,
      queueNames: readonly string[],
    ): Promise<ChimpbaseQueueJobRecord[]> {
      if (queueNames.length === 0) {
        return [];
      }

      const now = platform.now();
      const leaseExpiresAtMs = now + leaseMs;
      const batchSize = Math.max(1, Math.floor(limit));
      const result = await queryable().query<ChimpbaseQueueJobRecord>(
        `
          WITH candidate AS (
            SELECT id
            FROM _chimpbase_queue_jobs
            WHERE queue_name = ANY($3::text[])
              AND (
                (status = 'pending' AND available_at_ms <= $1)
                OR
                (status = 'processing' AND lease_expires_at_ms IS NOT NULL AND lease_expires_at_ms <= $1)
              )
            ORDER BY id ASC
            FOR UPDATE SKIP LOCKED
            LIMIT $4
          ),
          updated AS (
            UPDATE _chimpbase_queue_jobs q
            SET
              status = 'processing',
              attempt_count = q.attempt_count + 1,
              lease_expires_at_ms = $2,
              updated_at = NOW()
            FROM candidate
            WHERE q.id = candidate.id
            RETURNING q.id, q.queue_name, q.payload_json::text, q.attempt_count
          )
          SELECT id, queue_name, payload_json, attempt_count
          FROM updated
          ORDER BY id ASC
        `,
        [now, leaseExpiresAtMs, queueNames, batchSize],
      );

      return result.rows;
    },
    async collectionDelete(name: string, filter: ChimpbaseCollectionFilter = {}): Promise<number> {
      const matched = await findCollectionDocuments(queryable(), name, filter);
      for (const row of matched) {
        await queryable().query(
          "DELETE FROM _chimpbase_collections WHERE collection_name = $1 AND document_id = $2",
          [name, row.document_id],
        );
      }
      return matched.length;
    },
    async collectionFind<TDocument>(
      name: string,
      filter: ChimpbaseCollectionFilter,
      options: ChimpbaseCollectionFindOptions | undefined,
      validator: ChimpbaseValidator<TDocument>,
    ): Promise<TDocument[]> {
      return (await findCollectionDocuments(queryable(), name, filter, options)).map((row) =>
        validator.parse(parseJson(row.document_json, `collection ${name} document ${row.document_id}`), `collection ${name} document ${row.document_id}`)
      );
    },
    async collectionFindOne<TDocument>(
      name: string,
      filter: ChimpbaseCollectionFilter,
      validator: ChimpbaseValidator<TDocument>,
    ): Promise<TDocument | null> {
      const [row] = await findCollectionDocuments(queryable(), name, filter, { limit: 1 });
      return row === undefined
        ? null
        : validator.parse(parseJson(row.document_json, `collection ${name} document ${row.document_id}`), `collection ${name} document ${row.document_id}`);
    },
    async collectionInsert<TDocument extends Record<string, unknown>>(name: string, document: TDocument): Promise<string> {
      const documentId = platform.randomUUID();
      await queryable().query(
        `
          INSERT INTO _chimpbase_collections (
            collection_name,
            document_id,
            document_json,
            created_at,
            updated_at
          ) VALUES ($1, $2, $3::jsonb, NOW(), NOW())
        `,
        [name, documentId, JSON.stringify({ ...document, id: documentId })],
      );
      return documentId;
    },
    async collectionList(): Promise<string[]> {
      const result = await queryable().query<{ collection_name: string }>(
        `
          SELECT DISTINCT collection_name
          FROM _chimpbase_collections
          ORDER BY collection_name ASC
        `,
      );
      return result.rows.map((row) => row.collection_name);
    },
    async collectionUpdate(name: string, filter: ChimpbaseCollectionFilter, patch: ChimpbaseCollectionPatch): Promise<number> {
      const matched = await findCollectionDocuments(queryable(), name, filter);
      for (const row of matched) {
        const current = parseJsonObject(row.document_json, "collection document");
        await queryable().query(
          `
            UPDATE _chimpbase_collections
            SET document_json = $1::jsonb, updated_at = NOW()
            WHERE collection_name = $2 AND document_id = $3
          `,
          [JSON.stringify({ ...current, ...patch }), name, row.document_id],
        );
      }
      return matched.length;
    },
    async commitTransaction(events: ChimpbaseEventRecord[]) {
      const connection = queryable();
      await persistEvents(connection, events);
      const availableAtMs = platform.now();
      for (const event of events) {
        for (const payload of createChimpbaseEventDeliveryPayloads(event)) {
          await connection.query(
            `INSERT INTO _chimpbase_queue_jobs (
              queue_name, payload_json, status, available_at_ms, attempt_count
            ) VALUES ($1, $2::jsonb, 'pending', $3, 0)`,
            [
              "__chimpbase.subscription.run",
              JSON.stringify(payload),
              availableAtMs,
            ],
          );
        }
      }
      if ((transactionClient !== null)) {
        await transactionClient.query("COMMIT");
        transactionClient.release();
        transactionClient = null;
      }
    },
    async completeQueueJob(jobId: number) {
      await queryable().query(
        `
          UPDATE _chimpbase_queue_jobs
          SET
            status = 'completed',
            completed_at = NOW(),
            lease_expires_at_ms = NULL,
            updated_at = NOW()
          WHERE id = $1
        `,
        [jobId],
      );
    },
    async deleteCronSchedule(scheduleName: string): Promise<void> {
      await queryable().query(
        "DELETE FROM _chimpbase_cron_schedules WHERE schedule_name = $1",
        [scheduleName],
      );
    },
    async getQueueJobPayload(jobId: number): Promise<string | null> {
      const result = await queryable().query<{ payload_json: string }>(
        "SELECT payload_json::text AS payload_json FROM _chimpbase_queue_jobs WHERE id = $1 LIMIT 1",
        [jobId],
      );
      return result.rows[0]?.payload_json ?? null;
    },
    async insertCronRun(scheduleName: string, fireAtMs: number): Promise<boolean> {
      const result = await queryable().query(
        `
          INSERT INTO _chimpbase_cron_runs (
            schedule_name,
            fire_at_ms
          ) VALUES ($1, $2)
          ON CONFLICT(schedule_name, fire_at_ms) DO NOTHING
        `,
        [scheduleName, fireAtMs],
      );

      return (result.rowCount ?? 0) > 0;
    },
    async kvDelete(key: string) {
      await queryable().query("DELETE FROM _chimpbase_kv WHERE key = $1", [key]);
    },
    async kvGet<TValue>(
      key: string,
      validator: ChimpbaseValidator<TValue>,
    ): Promise<TValue | null> {
      const result = await queryable().query<{ value_json: string }>(
        "SELECT value_json::text AS value_json FROM _chimpbase_kv WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW()) LIMIT 1",
        [key],
      );
      const row = result.rows[0];
      return row === undefined
        ? null
        : validator.parse(parseJson(row.value_json, `key-value entry ${key}`), `key-value entry ${key}`);
    },
    async kvList(options?: ChimpbaseKvListOptions): Promise<string[]> {
      const prefix = options?.prefix ?? "";
      const result = await queryable().query<{ key: string }>(
        `
          SELECT key
          FROM _chimpbase_kv
          WHERE key LIKE $1 AND (expires_at IS NULL OR expires_at > NOW())
          ORDER BY key ASC
        `,
        [`${prefix}%`],
      );
      return result.rows.map((row) => row.key);
    },
    async kvSet<TValue = unknown>(key: string, value: TValue, ttlMs?: number) {
      const expiresAt = ttlMs !== undefined ? new Date(Date.now() + ttlMs).toISOString() : null;
      await queryable().query(
        `
          INSERT INTO _chimpbase_kv (key, value_json, updated_at, expires_at)
          VALUES ($1, $2::jsonb, NOW(), $3::timestamptz)
          ON CONFLICT(key) DO UPDATE SET
            value_json = excluded.value_json,
            updated_at = NOW(),
            expires_at = excluded.expires_at
        `,
        [key, JSON.stringify(value ?? null), expiresAt],
      );
    },
    async listCronSchedules(): Promise<PersistedCronScheduleRow[]> {
      const result = await queryable().query<PersistedCronScheduleRow>(
        `
          SELECT
            schedule_name,
            cron_expression,
            next_fire_at_ms::double precision AS next_fire_at_ms
          FROM _chimpbase_cron_schedules
          ORDER BY schedule_name ASC
        `,
      );

      return result.rows;
    },
    async markQueueJobFailure(
      jobId: number,
      status: "dlq" | "failed" | "pending",
      nextAvailableAtMs: number,
      errorMessage: string,
    ) {
      await queryable().query(
        `
          UPDATE _chimpbase_queue_jobs
          SET
            status = $1,
            available_at_ms = $2,
            lease_expires_at_ms = NULL,
            last_error = $3,
            updated_at = NOW()
          WHERE id = $4
        `,
        [status, nextAvailableAtMs, errorMessage, jobId],
      );
    },
    createKysely<TDatabase = Record<string, never>>(schema?: string): Kysely<TDatabase> {
      const database = createPostgresKysely<TDatabase>({
        async executeQuery<R>(compiledQuery: CompiledQuery): Promise<QueryResult<R>> {
          if (schema !== undefined) assertChimpbaseModuleCompiledSql(schema, compiledQuery.sql);
          const result = await queryable().query<Record<string, unknown>>(
            compiledQuery.sql,
            [...compiledQuery.parameters],
          );

          return {
            numAffectedRows: result.rowCount == null ? undefined : BigInt(result.rowCount),
            rows: parseKyselyRows<R>(result.rows),
          };
        },
      });
      return schema === undefined ? database : database.withSchema(schema);
    },
    async query<T>(
      sql: string,
      params: readonly unknown[],
      validator: ChimpbaseValidator<T>,
    ): Promise<T[]> {
      const result = await queryable().query<Record<string, unknown>>(normalizePostgresSql(sql), [...params]);
      return validator.array().parse(result.rows, "database query rows");
    },
    async queueEnqueue<TPayload = unknown>(name: string, payload: TPayload, options?: ChimpbaseQueueEnqueueOptions) {
      const availableAtMs = platform.now() + Math.max(0, options?.delayMs ?? 0);
      await queryable().query(
        `
          INSERT INTO _chimpbase_queue_jobs (
            queue_name,
            payload_json,
            status,
            available_at_ms,
            attempt_count
          ) VALUES ($1, $2::jsonb, 'pending', $3, 0)
        `,
        [name, JSON.stringify(payload ?? null), availableAtMs],
      );
    },
    async releaseCronScheduleLease(scheduleName: string, leaseToken: string): Promise<void> {
      await queryable().query(
        `
          UPDATE _chimpbase_cron_schedules
          SET
            lease_token = NULL,
            lease_expires_at_ms = NULL,
            updated_at = NOW()
          WHERE schedule_name = $1 AND lease_token = $2
        `,
        [scheduleName, leaseToken],
      );
    },
    async rollbackTransaction() {
      if (!(transactionClient !== null)) {
        return;
      }

      try {
        await transactionClient.query("ROLLBACK");
      } finally {
        transactionClient.release();
        transactionClient = null;
      }
    },
    async streamAppend<TPayload = unknown>(stream: string, event: string, payload: TPayload): Promise<number> {
      const result = await queryable().query<{ id: number }>(
        `
          INSERT INTO _chimpbase_stream_events (
            stream_name,
            event_name,
            payload_json
          ) VALUES ($1, $2, $3::jsonb)
          RETURNING id
        `,
        [stream, event, JSON.stringify(payload ?? null)],
      );
      return result.rows[0]?.id ?? 0;
    },
    async streamRead<TPayload>(
      stream: string,
      options: ChimpbaseStreamReadOptions | undefined,
      validator: ChimpbaseValidator<TPayload>,
    ): Promise<ChimpbaseStreamEvent<TPayload>[]> {
      const sinceId = options?.sinceId ?? 0;
      const limit = options?.limit ?? 100;
      const result = await queryable().query<{
        created_at: string;
        event_name: string;
        id: number;
        payload_json: string;
        stream_name: string;
      }>(
        `
          SELECT
            id,
            stream_name,
            event_name,
            payload_json::text AS payload_json,
            created_at::text AS created_at
          FROM _chimpbase_stream_events
          WHERE stream_name = $1 AND id > $2
          ORDER BY id ASC
          LIMIT $3
        `,
        [stream, sinceId, limit],
      );

      return result.rows.map((row) => ({
        createdAt: row.created_at,
        event: row.event_name,
        id: row.id,
        payload: validator.parse(parseJson(row.payload_json, `stream ${stream} event ${row.id}`), `stream ${stream} event ${row.id}`),
        stream: row.stream_name,
      }));
    },
    async upsertCronSchedule(scheduleName: string, cronExpression: string, nextFireAtMs: number): Promise<void> {
      await queryable().query(
        `
          INSERT INTO _chimpbase_cron_schedules (
            schedule_name,
            cron_expression,
            next_fire_at_ms,
            lease_token,
            lease_expires_at_ms
          ) VALUES ($1, $2, $3, NULL, NULL)
          ON CONFLICT(schedule_name) DO UPDATE SET
            cron_expression = excluded.cron_expression,
            next_fire_at_ms = excluded.next_fire_at_ms,
            lease_token = NULL,
            lease_expires_at_ms = NULL,
            updated_at = NOW()
        `,
        [scheduleName, cronExpression, nextFireAtMs],
      );
    },
    async blobPutMetadata(row: ChimpbaseBlobMetaRow): Promise<void> {
      await queryable().query(
        `
          INSERT INTO _chimpbase_blobs (
            bucket, key, size, etag, content_type, metadata, driver_ref, created_at, updated_at
          ) VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8::timestamptz, $9::timestamptz)
          ON CONFLICT(bucket, key) DO UPDATE SET
            size = excluded.size,
            etag = excluded.etag,
            content_type = excluded.content_type,
            metadata = excluded.metadata,
            driver_ref = excluded.driver_ref,
            updated_at = excluded.updated_at
        `,
        [
          row.bucket,
          row.key,
          row.size,
          row.etag,
          row.contentType,
          JSON.stringify(row.metadata),
          row.driverRef,
          row.createdAt,
          row.updatedAt,
        ],
      );
    },
    async blobGetMetadata(bucket: string, key: string): Promise<ChimpbaseBlobMetaRow | null> {
      const result = await queryable().query<{
        bucket: string;
        key: string;
        size: string;
        etag: string;
        content_type: string;
        metadata: string;
        driver_ref: string;
        created_at: string;
        updated_at: string;
      }>(
        `
          SELECT bucket, key, size::text AS size, etag, content_type,
                 metadata::text AS metadata, driver_ref,
                 created_at::text AS created_at, updated_at::text AS updated_at
          FROM _chimpbase_blobs
          WHERE bucket = $1 AND key = $2
          LIMIT 1
        `,
        [bucket, key],
      );
      const row = result.rows[0];
      if (!(row !== null && row !== undefined)) return null;
      return {
        bucket: row.bucket,
        key: row.key,
        size: Number(row.size),
        etag: row.etag,
        contentType: row.content_type,
        metadata: parseStringRecord(row.metadata, "blob metadata"),
        driverRef: row.driver_ref,
        createdAt: row.created_at,
        updatedAt: row.updated_at,
      };
    },
    async blobDeleteMetadata(bucket: string, key: string): Promise<boolean> {
      const result = await queryable().query(
        "DELETE FROM _chimpbase_blobs WHERE bucket = $1 AND key = $2",
        [bucket, key],
      );
      return (result.rowCount ?? 0) > 0;
    },
    async blobListMetadata(
      bucket: string,
      options: ChimpbaseBlobListOptions,
    ): Promise<ChimpbaseBlobListMetaResult> {
      const prefix = options.prefix ?? "";
      const delimiter = options.delimiter ?? null;
      const limit = Math.min(Math.max(options.limit ?? 1000, 1), 1000);
      const cursor = options.cursor ?? "";
      const result = await queryable().query<{
        bucket: string;
        key: string;
        size: string;
        etag: string;
        content_type: string;
        metadata: string;
        driver_ref: string;
        created_at: string;
        updated_at: string;
      }>(
        `
          SELECT bucket, key, size::text AS size, etag, content_type,
                 metadata::text AS metadata, driver_ref,
                 created_at::text AS created_at, updated_at::text AS updated_at
          FROM _chimpbase_blobs
          WHERE bucket = $1
            AND key LIKE $2
            AND key > $3
          ORDER BY key ASC
          LIMIT $4
        `,
        [bucket, `${prefix}%`, cursor, limit + 1],
      );
      return sliceBlobList(result.rows.map((row) => ({
        bucket: row.bucket,
        key: row.key,
        size: Number(row.size),
        etag: row.etag,
        contentType: row.content_type,
        metadata: parseStringRecord(row.metadata, "blob metadata"),
        driverRef: row.driver_ref,
        createdAt: row.created_at,
        updatedAt: row.updated_at,
      })), prefix, delimiter, limit);
    },
    async blobInitUpload(row: ChimpbaseBlobUploadRow): Promise<void> {
      await queryable().query(
        `
          INSERT INTO _chimpbase_blob_uploads (
            upload_id, bucket, key, content_type, metadata, driver_ref, created_at_ms, expires_at_ms
          ) VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7, $8)
        `,
        [
          row.uploadId,
          row.bucket,
          row.key,
          row.contentType,
          JSON.stringify(row.metadata),
          row.driverRef,
          row.createdAtMs,
          row.expiresAtMs,
        ],
      );
    },
    async blobGetUpload(uploadId: string): Promise<ChimpbaseBlobUploadRow | null> {
      const result = await queryable().query<{
        upload_id: string;
        bucket: string;
        key: string;
        content_type: string | null;
        metadata: string;
        driver_ref: string;
        created_at_ms: string;
        expires_at_ms: string;
      }>(
        `
          SELECT upload_id, bucket, key, content_type,
                 metadata::text AS metadata, driver_ref,
                 created_at_ms::text AS created_at_ms,
                 expires_at_ms::text AS expires_at_ms
          FROM _chimpbase_blob_uploads
          WHERE upload_id = $1
          LIMIT 1
        `,
        [uploadId],
      );
      const row = result.rows[0];
      if (row === undefined) return null;
      return {
        uploadId: row.upload_id,
        bucket: row.bucket,
        key: row.key,
        contentType: row.content_type,
        metadata: parseStringRecord(row.metadata, "blob metadata"),
        driverRef: row.driver_ref,
        createdAtMs: Number(row.created_at_ms),
        expiresAtMs: Number(row.expires_at_ms),
      };
    },
    async blobRecordPart(row: ChimpbaseBlobPartRow): Promise<void> {
      await queryable().query(
        `
          INSERT INTO _chimpbase_blob_upload_parts (
            upload_id, part_number, size, etag, driver_ref, created_at
          ) VALUES ($1, $2, $3, $4, $5, $6::timestamptz)
          ON CONFLICT(upload_id, part_number) DO UPDATE SET
            size = excluded.size,
            etag = excluded.etag,
            driver_ref = excluded.driver_ref,
            created_at = excluded.created_at
        `,
        [row.uploadId, row.partNumber, row.size, row.etag, row.driverRef, row.createdAt],
      );
    },
    async blobListParts(uploadId: string): Promise<ChimpbaseBlobPartRow[]> {
      const result = await queryable().query<{
        upload_id: string;
        part_number: number;
        size: string;
        etag: string;
        driver_ref: string;
        created_at: string;
      }>(
        `
          SELECT upload_id, part_number, size::text AS size, etag, driver_ref, created_at::text AS created_at
          FROM _chimpbase_blob_upload_parts
          WHERE upload_id = $1
          ORDER BY part_number ASC
        `,
        [uploadId],
      );
      return result.rows.map((row) => ({
        uploadId: row.upload_id,
        partNumber: row.part_number,
        size: Number(row.size),
        etag: row.etag,
        driverRef: row.driver_ref,
        createdAt: row.created_at,
      }));
    },
    async blobFinalizeUpload(
      uploadId: string,
      finalMeta: ChimpbaseBlobMetaRow,
    ): Promise<void> {
      const client = transactionClient ?? await pool.connect();
      const ownsClient = !(transactionClient !== null);
      try {
        if (ownsClient) await client.query("BEGIN");
        await client.query(
          `
            INSERT INTO _chimpbase_blobs (
              bucket, key, size, etag, content_type, metadata, driver_ref, created_at, updated_at
            ) VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8::timestamptz, $9::timestamptz)
            ON CONFLICT(bucket, key) DO UPDATE SET
              size = excluded.size,
              etag = excluded.etag,
              content_type = excluded.content_type,
              metadata = excluded.metadata,
              driver_ref = excluded.driver_ref,
              updated_at = excluded.updated_at
          `,
          [
            finalMeta.bucket,
            finalMeta.key,
            finalMeta.size,
            finalMeta.etag,
            finalMeta.contentType,
            JSON.stringify(finalMeta.metadata),
            finalMeta.driverRef,
            finalMeta.createdAt,
            finalMeta.updatedAt,
          ],
        );
        await client.query(
          "DELETE FROM _chimpbase_blob_uploads WHERE upload_id = $1",
          [uploadId],
        );
        if (ownsClient) await client.query("COMMIT");
      } catch (error) {
        if (ownsClient) {
          try { await client.query("ROLLBACK"); } catch {}
        }
        throw error;
      } finally {
        if (ownsClient) client.release();
      }
    },
    async blobAbortUpload(uploadId: string): Promise<void> {
      await queryable().query(
        "DELETE FROM _chimpbase_blob_uploads WHERE upload_id = $1",
        [uploadId],
      );
    },
    async blobListUploads(
      bucket: string,
      options: ChimpbaseBlobUploadListOptions,
    ): Promise<ChimpbaseBlobUploadListMetaResult> {
      const prefix = options.prefix ?? "";
      const cursor = options.cursor ?? "";
      const limit = Math.min(Math.max(options.limit ?? 100, 1), 1000);
      const result = await queryable().query<{
        upload_id: string;
        bucket: string;
        key: string;
        content_type: string | null;
        metadata: string;
        driver_ref: string;
        created_at_ms: string;
        expires_at_ms: string;
      }>(
        `
          SELECT upload_id, bucket, key, content_type,
                 metadata::text AS metadata, driver_ref,
                 created_at_ms::text AS created_at_ms,
                 expires_at_ms::text AS expires_at_ms
          FROM _chimpbase_blob_uploads
          WHERE bucket = $1 AND key LIKE $2 AND upload_id > $3
          ORDER BY upload_id ASC
          LIMIT $4
        `,
        [bucket, `${prefix}%`, cursor, limit + 1],
      );
      const rows = result.rows.map((row) => ({
        uploadId: row.upload_id,
        bucket: row.bucket,
        key: row.key,
        contentType: row.content_type,
        metadata: parseStringRecord(row.metadata, "blob metadata"),
        driverRef: row.driver_ref,
        createdAtMs: Number(row.created_at_ms),
        expiresAtMs: Number(row.expires_at_ms),
      }));
      const hasMore = rows.length > limit;
      const page = hasMore ? rows.slice(0, limit) : rows;
      return {
        uploads: page,
        nextCursor: hasMore ? page[page.length - 1].uploadId : null,
      };
    },
    async blobGcExpiredUploads(nowMs: number): Promise<string[]> {
      const result = await queryable().query<{ upload_id: string }>(
        `
          DELETE FROM _chimpbase_blob_uploads
          WHERE expires_at_ms <= $1
          RETURNING upload_id
        `,
        [nowMs],
      );
      return result.rows.map((row) => row.upload_id);
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

function normalizePostgresSql(sql: string): string {
  return sql.replace(/\?(\d+)/g, (_match: string, index: string) => `$${index}`);
}

async function persistEvents(queryable: Queryable, events: ChimpbaseEventRecord[]): Promise<void> {
  if (events.length === 0) {
    return;
  }

  for (const event of events) {
    const result = await queryable.query<{ id: number }>(
      "INSERT INTO _chimpbase_events (event_name, payload_json) VALUES ($1, $2::jsonb) RETURNING CAST(id AS DOUBLE PRECISION) AS id",
      [event.name, event.payloadJson],
    );
    const row = v.object({ id: v.integer() }).parse(result.rows[0], "persisted event row");
    event.id = row.id;
  }
}

async function findCollectionDocuments(
  queryable: Queryable,
  name: string,
  filter: ChimpbaseCollectionFilter = {},
  options?: ChimpbaseCollectionFindOptions,
): Promise<PersistedCollectionDocument[]> {
  const result = await queryable.query<PersistedCollectionDocument>(
    `
      SELECT
        document_id,
        document_json::text AS document_json
      FROM _chimpbase_collections
      WHERE collection_name = $1
      ORDER BY document_id ASC
    `,
    [name],
  );

  const matched = result.rows.filter((row) => {
    const document = parseJsonObject(row.document_json, "collection document");
    return Object.entries(filter).every(([key, value]) => document[key] === value);
  });

  const limit = options?.limit;
  return typeof limit === "number" ? matched.slice(0, limit) : matched;
}
