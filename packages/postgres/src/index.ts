import type { CompiledQuery, Kysely, QueryResult } from "kysely";
import { Pool, type PoolClient, type QueryResultRow } from "pg";

import type {
  ChimpbaseBlobListMetaResult,
  ChimpbaseBlobMetaRow,
  ChimpbaseBlobPartRow,
  ChimpbaseBlobUploadListMetaResult,
  ChimpbaseBlobUploadRow,
  ChimpbaseEngineAdapter,
  ChimpbaseEventRecord,
  ChimpbaseMigration,
  ChimpbasePlatformShim,
  ChimpbaseProjectConfig,
  ChimpbaseQueueClaimTargets,
  ChimpbaseQueueJobRecord,
} from "@chimpbase/core";
import {
  assertWorkerInteger,
  assertChimpbaseModuleCompiledSql,
  createChimpbaseEventDeliveryPayloads,
  escapeSqlLikePrefix,
  paginateChimpbaseBlobMetadata,
  ChimpbasePreconditionFailedError,
} from "@chimpbase/core";
import type {
  ChimpbaseBlobListOptions,
  ChimpbaseBlobPutOptions,
  ChimpbaseBlobUploadListOptions,
  ChimpbaseCollectionFilter,
  ChimpbaseCollectionFindOptions,
  ChimpbaseValidator,
  ChimpbaseCollectionPatch,
  ChimpbaseKvListOptions,
  ChimpbaseQueueEnqueueOptions,
  ChimpbaseJob,
  ChimpbaseJobListOptions,
  ChimpbaseStreamEvent,
  ChimpbaseStreamReadOptions,
} from "@chimpbase/runtime";
import { isArrayValue, isJsonObject, parseJson, parseJsonObject, parseStringRecord, v } from "@chimpbase/runtime";

import { createPostgresKysely } from "./kysely.ts";
import { PostgresPollingEventBus } from "./event-bus.ts";

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

function safeRecordId(value: number | string, kind = "queue job"): number {
  const id = Number(value);
  if (!Number.isSafeInteger(id) || id <= 0) throw new Error(`${kind} id exceeds the supported safe integer range`);
  return id;
}

interface InspectedJobRow extends QueryResultRow {
  id: number | string;
  queue_name: string;
  payload_json: string;
  status: ChimpbaseJob["status"];
  attempt_count: number;
  available_at_ms: number | string;
  lease_expires_at_ms: number | string | null;
  last_error: string | null;
}

function inspectedJob(row: InspectedJobRow): Omit<ChimpbaseJob, "registered"> {
  return { id: safeRecordId(row.id), queue: row.queue_name, payload: parseJson(row.payload_json), status: row.status,
    attempt: row.attempt_count, availableAtMs: Number(row.available_at_ms),
    leaseExpiresAtMs: row.lease_expires_at_ms === null ? null : Number(row.lease_expires_at_ms), lastError: row.last_error };
}

function queueTargetFilter(paramIndex: number): string {
  const targets = `$${paramIndex}::jsonb`;
  return `CASE queue_name
    WHEN '__chimpbase.cron.run' THEN EXISTS (
      SELECT 1 FROM jsonb_array_elements_text(${targets}->'cronScheduleNames') target
      WHERE target = payload_json->>'scheduleName'
    )
    WHEN '__chimpbase.subscription.run' THEN EXISTS (
      SELECT 1 FROM jsonb_array_elements(${targets}->'subscriptions') target
      WHERE target->>'eventName' = payload_json->>'eventName'
        AND (payload_json->>'subscriptionName' IS NULL
          OR target->>'subscriptionName' = payload_json->>'subscriptionName')
    )
    WHEN '__chimpbase.workflow.run' THEN EXISTS (
      SELECT 1 FROM _chimpbase_workflow_instances instance,
        jsonb_array_elements(${targets}->'workflows') target
      WHERE instance.workflow_id = payload_json->>'workflowId'
        AND instance.workflow_name = target->>'workflowName'
        AND instance.workflow_version::text = target->>'workflowVersion'
    )
    ELSE TRUE END`;
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


interface Queryable {
  query<T extends QueryResultRow = QueryResultRow>(sql: string, params?: unknown[]): Promise<{ rowCount: number | null; rows: T[] }>;
}

export function openPostgresPool(config: ChimpbaseProjectConfig, max = 10): Pool {
  if (!(config.storage.url !== null && config.storage.url.length > 0)) {
    throw new Error("postgres storage requires storage.url");
  }

  const connectionTimeoutMillis = config.storage.connectionTimeoutMs ?? 5_000;
  const queryTimeoutMs = config.storage.queryTimeoutMs ?? 30_000;
  for (const [name, value] of [["connectionTimeoutMs", connectionTimeoutMillis], ["queryTimeoutMs", queryTimeoutMs]] as const) {
    if (!Number.isSafeInteger(value) || value <= 0 || value > 2_147_483_647) {
      throw new Error(`storage.${name} must be a positive integer no greater than 2147483647`);
    }
  }
  const pool = new Pool({
    connectionString: config.storage.url,
    max,
    connectionTimeoutMillis,
    query_timeout: queryTimeoutMs,
    statement_timeout: queryTimeoutMs,
    idle_in_transaction_session_timeout: queryTimeoutMs,
  });
  // pg removes failed idle clients before emitting; the next checkout reconnects.
  pool.on("error", () => {});
  return pool;
}

export async function openPostgresStorage(
  config: ChimpbaseProjectConfig,
  platform: ChimpbasePlatformShim,
  migrations: readonly ChimpbaseMigration[],
  migrationsSql: string[],
) {
  const pool = openPostgresPool(config);
  const isolatedPools = new Map<"rpc" | "detached", Pool>();
  try {
    await applyPostgresSqlMigrations(pool, migrations);
    await applyInlinePostgresMigrations(pool, migrationsSql);
    await ensurePostgresInternalTables(pool);
    return {
      createAdapter(group?: "rpc" | "detached") {
        if (group === undefined) return createPostgresEngineAdapter(pool, platform);
        let isolated = isolatedPools.get(group);
        if (isolated === undefined) {
          isolated = openPostgresPool(config, group === "detached" ? 2 : 10);
          isolatedPools.set(group, isolated);
        }
        return createPostgresEngineAdapter(isolated, platform);
      },
      eventBus: new PostgresPollingEventBus({ pool }),
      storage: {
        close() {
          return Promise.all([pool, ...isolatedPools.values()].map((resource) => resource.end())).then(() => undefined);
        },
      },
      supportsConcurrentWorkers: true,
      requestConcurrency: 10,
    };
  } catch (error) {
    try {
      await pool.end();
    } catch {
      // Preserve the startup error if closing the failed pool also fails.
    }
    throw error;
  }
}

export async function applyPostgresSqlMigrations(
  pool: Pool,
  migrations: readonly ChimpbaseMigration[],
): Promise<void> {
  if (migrations.length === 0) return;
  const client = await pool.connect();
  let failed = false;
  try {
    await client.query("BEGIN");
    await client.query("SELECT pg_advisory_xact_lock(hashtext('_chimpbase_migrations'))");
    await client.query(`CREATE TABLE IF NOT EXISTS _chimpbase_migrations (
      name TEXT PRIMARY KEY,
      applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
    )`);
    for (const migration of migrations) {
      const applied = await client.query<{ name: string }>("SELECT name FROM _chimpbase_migrations WHERE name = $1", [migration.name]);
      if (applied.rows.length > 0) continue;
      await client.query(migration.sql);
      await client.query("INSERT INTO _chimpbase_migrations (name) VALUES ($1)", [migration.name]);
    }
    await client.query("COMMIT");
  } catch (error) {
    failed = true;
    throw error;
  } finally {
    client.release(failed);
  }
}

export async function applyInlinePostgresMigrations(pool: Pool, migrations: string[]): Promise<void> {
  for (const migration of migrations) {
    await pool.query(migration);
  }
}

export async function ensurePostgresInternalTables(pool: Pool): Promise<void> {
  const client = await pool.connect();
  let failed = false;
  try {
    await client.query("BEGIN");
    await client.query("SELECT pg_advisory_xact_lock(hashtext('_chimpbase_internal_tables'))");
    await createPostgresInternalTables(client);
    await client.query("COMMIT");
  } catch (error) {
    failed = true;
    throw error;
  } finally {
    client.release(failed);
  }
}

async function createPostgresInternalTables(pool: Queryable): Promise<void> {
  await pool.query(
    `
      CREATE TABLE IF NOT EXISTS _chimpbase_events (
        id BIGSERIAL PRIMARY KEY,
        event_name TEXT NOT NULL,
        payload_json JSONB NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        transaction_id BIGINT NOT NULL DEFAULT txid_current()
      )
    `,
  );

  await pool.query(`
    ALTER TABLE _chimpbase_events ADD COLUMN IF NOT EXISTS transaction_id BIGINT;
    ALTER TABLE _chimpbase_events ALTER COLUMN transaction_id SET DEFAULT txid_current();
    CREATE INDEX IF NOT EXISTS idx_chimpbase_events_transaction_id ON _chimpbase_events(transaction_id)
  `);

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
  let failedTransaction: { error: unknown } | null = null;

  const failTransaction = (error: unknown) => {
    if (transactionClient === null || failedTransaction !== null) return;
    failedTransaction = { error };
    transactionClient.removeListener("error", failTransaction);
    transactionClient.release(true);
  };
  const transactionQueryable: Queryable = {
    async query<T extends QueryResultRow>(sql: string, params?: unknown[]) {
      if (failedTransaction !== null) throw failedTransaction.error;
      if (transactionClient === null) throw new Error("PostgreSQL transaction is not active");
      try {
        return await transactionClient.query<T>(sql, params);
      } catch (error) {
        // Discard the socket: a timed-out SQL statement may still be executing.
        failTransaction(error);
        throw error;
      }
    },
  };

  const queryable = (): Queryable => transactionClient === null ? pool : transactionQueryable;

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
      transactionClient.on("error", failTransaction);
      try {
        await transactionQueryable.query("BEGIN");
      } catch (error) {
        transactionClient = null;
        failedTransaction = null;
        throw error;
      }
    },
    async claimNextCronSchedule(
      leaseMs: number,
      scheduleNames: readonly string[],
    ): Promise<(PersistedCronScheduleRow & { lease_token: string }) | null> {
      if (scheduleNames.length === 0) return null;
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
              AND schedule_name = ANY($4::text[])
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
        [now, leaseToken, leaseExpiresAtMs, scheduleNames],
      );

      return result.rows[0] ?? null;
    },
    async claimNextQueueJob(
      leaseMs: number,
      queueNames: readonly string[],
      targets: ChimpbaseQueueClaimTargets,
    ): Promise<ChimpbaseQueueJobRecord | null> {
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
              AND ${queueTargetFilter(4)}
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
        [now, leaseExpiresAtMs, queueNames, JSON.stringify(targets)],
      );
      const row = result.rows[0];
      return row === undefined ? null : { ...row, id: safeRecordId(row.id) };
    },
    async claimNextQueueJobs(
      leaseMs: number,
      limit: number,
      queueNames: readonly string[],
      targets: ChimpbaseQueueClaimTargets,
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
              AND ${queueTargetFilter(5)}
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
        [now, leaseExpiresAtMs, queueNames, batchSize, JSON.stringify(targets)],
      );

      return result.rows.map((row) => ({ ...row, id: safeRecordId(row.id) }));
    },
    async lockQueueJob(jobId: number, attempt: number): Promise<boolean> {
      const result = await queryable().query<{ id: number }>(
        "SELECT id FROM _chimpbase_queue_jobs WHERE id = $1 AND status = 'processing' AND attempt_count = $2 FOR UPDATE",
        [jobId, attempt],
      );
      return result.rows.length > 0;
    },
    async releaseQueueJob(jobId: number, attempt: number): Promise<void> {
      await queryable().query(`UPDATE _chimpbase_queue_jobs
        SET status = 'pending', attempt_count = attempt_count - 1, lease_expires_at_ms = NULL, updated_at = NOW()
        WHERE id = $1 AND status = 'processing' AND attempt_count = $2`, [jobId, attempt]);
    },
    async collectionDelete(name: string, filter: ChimpbaseCollectionFilter = {}): Promise<number> {
      const { where, params } = collectionWhere(name, filter);
      const result = await queryable().query(`DELETE FROM _chimpbase_collections WHERE ${where}`, params);
      return result.rowCount ?? 0;
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
      const { where, params } = collectionWhere(name, filter);
      const patchJson = JSON.stringify({ ...patch });
      const storedPatch = parseJsonObject(patchJson, "collection patch");
      const removedKeys = Object.keys(patch).filter((key) => !Object.hasOwn(storedPatch, key));
      params.push(removedKeys, patchJson);
      const result = await queryable().query(
        `
          UPDATE _chimpbase_collections
          SET document_json = (document_json - $${params.length - 1}::text[]) || $${params.length}::jsonb,
              updated_at = NOW()
          WHERE ${where}
        `,
        params,
      );
      return result.rowCount ?? 0;
    },
    async persistEvents(events: ChimpbaseEventRecord[]) {
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
    },
    async commitTransaction() {
      if ((transactionClient !== null)) {
        await transactionQueryable.query("COMMIT");
        transactionClient.removeListener("error", failTransaction);
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
    async listQueueJobs(options: ChimpbaseJobListOptions) {
      const result = await queryable().query<InspectedJobRow>(`SELECT id, queue_name, payload_json::text,
        status, attempt_count, available_at_ms, lease_expires_at_ms, last_error FROM _chimpbase_queue_jobs
        WHERE ($1::text IS NULL OR queue_name = $1) AND ($2::text IS NULL OR status = $2)
        ORDER BY id DESC LIMIT $3`, [options.queue ?? null, options.status ?? null, options.limit ?? 100]);
      return result.rows.map(inspectedJob);
    },
    async getQueueJob(jobId: number) {
      const result = await queryable().query<InspectedJobRow>(`SELECT id, queue_name, payload_json::text,
        status, attempt_count, available_at_ms, lease_expires_at_ms, last_error
        FROM _chimpbase_queue_jobs WHERE id = $1`, [jobId]);
      return result.rows[0] === undefined ? null : inspectedJob(result.rows[0]);
    },
    async retryQueueJob(jobId: number, dlqName?: string) {
      const source = (await queryable().query<InspectedJobRow>(`SELECT id, queue_name, status
        FROM _chimpbase_queue_jobs WHERE id = $1 FOR UPDATE`, [jobId])).rows[0];
      if (source === undefined || source.queue_name.startsWith("__chimpbase.")
        || (source.status !== "failed" && source.status !== "dlq")) return false;
      if (source.status === "dlq") {
        if (dlqName === undefined) return false;
        const related = `id != $1 AND queue_name = $3 AND payload_json->>'queue' = $2
          AND (payload_json->>'jobId' = $1::text OR payload_json->>'jobId' IS NULL)`;
        const envelopes = await queryable().query<{ status: string; job_id: string | null }>(`SELECT status, payload_json->>'jobId' AS job_id
          FROM _chimpbase_queue_jobs WHERE ${related} FOR UPDATE`, [jobId, source.queue_name, dlqName]);
        if (envelopes.rows.some((row) => row.status !== "completed" && (row.job_id === null || row.status !== "pending"))) return false;
        await queryable().query(`UPDATE _chimpbase_queue_jobs SET status = 'completed', completed_at = NOW(),
          updated_at = NOW(), last_error = 'superseded by retry of job ' || $1::text
          WHERE ${related} AND payload_json->>'jobId' = $1::text AND status = 'pending'`, [jobId, source.queue_name, dlqName]);
      }
      const result = await queryable().query(`UPDATE _chimpbase_queue_jobs SET status = 'pending', attempt_count = 0,
        available_at_ms = $2, lease_expires_at_ms = NULL, last_error = NULL, completed_at = NULL,
        updated_at = NOW() WHERE id = $1 AND status IN ('failed', 'dlq')`, [jobId, platform.now()]);
      return result.rowCount === 1;
    },
    async retireWorkflowJobs(workflowId: string): Promise<void> {
      await queryable().query(`UPDATE _chimpbase_queue_jobs SET status = 'completed', completed_at = NOW(),
        lease_expires_at_ms = NULL, updated_at = NOW(), last_error = 'superseded by workflow retry'
        WHERE queue_name = '__chimpbase.workflow.run' AND payload_json->>'workflowId' = $1
          AND status IN ('pending', 'processing')`, [workflowId]);
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
        "SELECT value_json::text AS value_json FROM _chimpbase_kv WHERE key = $1 AND (expires_at IS NULL OR expires_at > clock_timestamp()) LIMIT 1",
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
          WHERE key LIKE $1 ESCAPE '!' AND (expires_at IS NULL OR expires_at > clock_timestamp())
          ORDER BY key ASC
        `,
        [escapeSqlLikePrefix(prefix)],
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
    async kvSetIfAbsent<TValue = unknown>(key: string, value: TValue, ttlMs?: number) {
      const expiresAt = ttlMs !== undefined ? new Date(Date.now() + ttlMs).toISOString() : null;
      const result = await queryable().query(
        `
          INSERT INTO _chimpbase_kv (key, value_json, updated_at, expires_at)
          VALUES ($1, $2::jsonb, NOW(), $3::timestamptz)
          ON CONFLICT(key) DO UPDATE SET
            value_json = excluded.value_json,
            updated_at = NOW(),
            expires_at = excluded.expires_at
          WHERE _chimpbase_kv.expires_at <= clock_timestamp()
        `,
        [key, JSON.stringify(value ?? null), expiresAt],
      );
      return (result.rowCount ?? 0) > 0;
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
      const now = platform.now();
      const availableAtMs = now + assertWorkerInteger("delayMs", options?.delayMs ?? 0, 0, Number.MAX_SAFE_INTEGER - now);
      const result = await queryable().query<{ id: number | string }>(
        `
          INSERT INTO _chimpbase_queue_jobs (
            queue_name,
            payload_json,
            status,
            available_at_ms,
            attempt_count
          ) VALUES ($1, $2::jsonb, 'pending', $3, 0)
          RETURNING id
        `,
        [name, JSON.stringify(payload ?? null), availableAtMs],
      );
      return safeRecordId(result.rows[0]!.id);
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
        if (failedTransaction === null) await transactionQueryable.query("ROLLBACK");
      } finally {
        transactionClient.removeListener("error", failTransaction);
        if (failedTransaction === null) transactionClient.release();
        transactionClient = null;
        failedTransaction = null;
      }
    },
    async streamAppend<TPayload = unknown>(stream: string, event: string, payload: TPayload): Promise<number> {
      const result = await queryable().query<{ id: number | string }>(
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
      const row = result.rows[0];
      return row === undefined ? 0 : safeRecordId(row.id, "stream event");
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
        id: number | string;
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
        id: safeRecordId(row.id, "stream event"),
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
    async blobLockMetadata(bucket: string, key: string): Promise<void> {
      await queryable().query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", [JSON.stringify([bucket, key])]);
    },
    async blobPutMetadata(row: ChimpbaseBlobMetaRow, conditions?: Pick<ChimpbaseBlobPutOptions, "ifMatch" | "ifNoneMatch">): Promise<void> {
      const ifMatch = conditions?.ifMatch;
      const params: unknown[] = [row.bucket, row.key, row.size, row.etag, row.contentType,
        JSON.stringify(row.metadata), row.driverRef, row.createdAt, row.updatedAt];
      const sql = ifMatch !== undefined && ifMatch.length > 0
        ? `UPDATE _chimpbase_blobs SET size = $3, etag = $4, content_type = $5,
             metadata = $6::jsonb, driver_ref = $7, updated_at = $8::timestamptz
           WHERE bucket = $1 AND key = $2 AND etag = $9`
        : `
          INSERT INTO _chimpbase_blobs (
            bucket, key, size, etag, content_type, metadata, driver_ref, created_at, updated_at
          ) VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8::timestamptz, $9::timestamptz)
          ${conditions?.ifNoneMatch === "*" ? "ON CONFLICT(bucket, key) DO NOTHING" : `ON CONFLICT(bucket, key) DO UPDATE SET
            size = excluded.size,
            etag = excluded.etag,
            content_type = excluded.content_type,
            metadata = excluded.metadata,
            driver_ref = excluded.driver_ref,
            updated_at = excluded.updated_at`}
        `;
      if (ifMatch !== undefined && ifMatch.length > 0) {
        params.splice(7, 1);
        params.push(ifMatch);
      }
      const result = await queryable().query(sql, params);
      if (result.rowCount === 0) throw new ChimpbasePreconditionFailedError(`blob ${row.bucket}/${row.key} precondition failed`);
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
      let firstPage = options.cursor === undefined;
      return await paginateChimpbaseBlobMetadata(options, async (cursor, limit) => {
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
              AND key LIKE $2 ESCAPE '!'
              AND ($5::boolean OR key > $3)
            ORDER BY key ASC
            LIMIT $4
          `,
          [bucket, escapeSqlLikePrefix(prefix), cursor, limit, firstPage],
        );
        firstPage = false;
        return result.rows.map((row) => ({
          bucket: row.bucket,
          key: row.key,
          size: Number(row.size),
          etag: row.etag,
          contentType: row.content_type,
          metadata: parseStringRecord(row.metadata, "blob metadata"),
          driverRef: row.driver_ref,
          createdAt: row.created_at,
          updatedAt: row.updated_at,
        }));
      });
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
      const ownedClient = transactionClient === null ? await pool.connect() : null;
      const client = ownedClient ?? transactionQueryable;
      let failed = false;
      try {
        if (ownedClient !== null) await client.query("BEGIN");
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
        if (ownedClient !== null) await client.query("COMMIT");
      } catch (error) {
        failed = true;
        throw error;
      } finally {
        ownedClient?.release(failed);
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
          WHERE bucket = $1 AND key LIKE $2 ESCAPE '!' AND upload_id > $3
          ORDER BY upload_id ASC
          LIMIT $4
        `,
        [bucket, escapeSqlLikePrefix(prefix), cursor, limit + 1],
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
    async blobListExpiredUploads(nowMs: number): Promise<string[]> {
      const result = await queryable().query<{ upload_id: string }>(
        `
          SELECT upload_id FROM _chimpbase_blob_uploads
          WHERE expires_at_ms <= $1
        `,
        [nowMs],
      );
      return result.rows.map((row) => row.upload_id);
    },
  };
}


function normalizePostgresSql(sql: string): string {
  let result = "";
  for (let index = 0; index < sql.length;) {
    let end = index + 1;
    const current = sql[index];
    const next = sql[index + 1];
    if (current === "-" && next === "-") {
      const newline = sql.slice(index).search(/[\r\n]/);
      end = newline < 0 ? sql.length : index + newline;
    } else if (current === "/" && next === "*") {
      end = index + 2;
      let depth = 1;
      while (end < sql.length && depth > 0) {
        if (sql.slice(end, end + 2) === "/*") { depth++; end += 2; }
        else if (sql.slice(end, end + 2) === "*/") { depth--; end += 2; }
        else end++;
      }
    } else if (current === "'" || current === '"') {
      const escaped = current === "'" && /[eE]/.test(sql[index - 1] ?? "")
        && !/[a-z0-9_$\u0080-\uffff]/i.test(sql[index - 2] ?? "");
      while (end < sql.length) {
        if (escaped && sql[end] === "\\") { end += 2; continue; }
        if (sql[end] === current && sql[end + 1] === current) { end += 2; continue; }
        if (sql[end++] === current) break;
      }
    } else if (current === "$" && !/[a-z0-9_$\u0080-\uffff]/i.test(sql[index - 1] ?? "")) {
      const dollar = sql.slice(index).match(/^\$(?:[a-z_\u0080-\uffff][a-z0-9_\u0080-\uffff]*)?\$/i)?.[0];
      if (dollar !== undefined) {
        const closing = sql.indexOf(dollar, index + dollar.length);
        end = closing < 0 ? sql.length : closing + dollar.length;
      }
    } else if (current === "?") {
      const placeholder = sql.slice(index).match(/^\?(\d+)/);
      if (placeholder !== null) {
        result += `$${placeholder[1]}`;
        index += placeholder[0].length;
        continue;
      }
    }
    result += sql.slice(index, end);
    index = end;
  }
  return result;
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
  const { where, params } = collectionWhere(name, filter);
  let sql = `SELECT document_id, document_json::text AS document_json
    FROM _chimpbase_collections WHERE ${where}`;
  const limit = options?.limit;
  if (typeof limit === "number" && limit < 0 && Number.isFinite(limit) && Math.trunc(limit) < 0) {
    params.push(Math.max(-Number.MAX_SAFE_INTEGER, Math.trunc(limit)));
    sql = `WITH matched AS (${sql})
      SELECT document_id, document_json FROM matched ORDER BY document_id ASC
      LIMIT GREATEST((SELECT COUNT(*) FROM matched) + $${params.length}::bigint, 0)`;
  } else {
    sql += " ORDER BY document_id ASC";
    if (typeof limit === "number" && limit !== Infinity && (Number.isNaN(limit) || limit < Number.MAX_SAFE_INTEGER)) {
      params.push(Number.isFinite(limit) ? Math.max(0, Math.trunc(limit)) : 0);
      sql += ` LIMIT $${params.length}`;
    }
  }
  return (await queryable.query<PersistedCollectionDocument>(sql, params)).rows;
}

function collectionWhere(name: string, filter: ChimpbaseCollectionFilter): { where: string; params: unknown[] } {
  const clauses = ["collection_name = $1"];
  const params: unknown[] = [name];
  for (const [key, value] of Object.entries(filter)) {
    const primitive = value === null || typeof value === "string" || typeof value === "boolean"
      || (typeof value === "number" && Number.isFinite(value));
    const inherited: unknown = key === "__proto__" ? Object.prototype : Object.getOwnPropertyDescriptor(Object.prototype, key)?.value;
    if (!primitive && value !== inherited) {
      clauses.push("FALSE");
      continue;
    }
    params.push(key);
    const keyParam = `$${params.length}::text`;
    if (primitive) {
      params.push(JSON.stringify(value));
      clauses.push(`document_json -> ${keyParam} = $${params.length}::jsonb`);
    } else {
      clauses.push(`NOT (document_json ? ${keyParam})`);
    }
  }
  return { where: clauses.join(" AND "), params };
}
