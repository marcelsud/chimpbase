import { applySqliteMigrations } from "@chimpbase/core";
import { mkdir } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { DatabaseSync, type SQLInputValue } from "node:sqlite";

import type {
  ChimpbaseMigration,
  ChimpbaseProjectConfig,
  ChimpbaseSqliteDatabase,
  ChimpbaseSqliteStatement,
} from "@chimpbase/core";
export { createSqliteEngineAdapter } from "@chimpbase/core";

interface SqliteDatabase extends ChimpbaseSqliteDatabase {
  close(): void;
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

export async function applySqlMigrations(db: SqliteDatabase, migrations: readonly ChimpbaseMigration[]): Promise<void> {
  applySqliteMigrations(db, migrations);
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

function createSqliteDatabase(db: DatabaseSync): SqliteDatabase {
  return {
    close() {
      db.close();
    },
    exec(sql: string) {
      return db.exec(sql);
    },
    query(sql: string): ChimpbaseSqliteStatement {
      const statement = db.prepare(sql);
      return {
        reader: statement.columns().length > 0,
        all(...params: unknown[]) {
          return statement.all(...toNodeSqlBindings(params));
        },
        run(...params: unknown[]) {
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
