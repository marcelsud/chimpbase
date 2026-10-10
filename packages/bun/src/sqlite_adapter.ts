import { applySqliteMigrations } from "@chimpbase/core";
import { mkdir } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { Database, type SQLQueryBindings } from "bun:sqlite";

import {
  createSqliteEngineAdapter as createSharedSqliteEngineAdapter,
  type ChimpbaseEngineAdapter,
  type ChimpbasePlatformShim,
  type ChimpbaseMigration,
  type ChimpbaseProjectConfig,
} from "@chimpbase/core";
import { isJsonObject } from "@chimpbase/runtime";

export async function openSqliteDatabase(
  projectDir: string,
  config: ChimpbaseProjectConfig,
): Promise<Database> {
  if (config.storage.engine === "memory" || !(config.storage.path !== null && config.storage.path.length > 0) || config.storage.path === ":memory:") {
    return new Database(":memory:");
  }

  const databasePath = resolve(projectDir, config.storage.path);
  await mkdir(dirname(databasePath), { recursive: true });
  return new Database(databasePath);
}

export async function applySqlMigrations(db: Database, migrations: readonly ChimpbaseMigration[]): Promise<void> {
  applySqliteMigrations(db, migrations);
}

export async function applyInlineSqlMigrations(db: Database, migrations: string[]): Promise<void> {
  for (const migration of migrations) {
    db.exec(migration);
  }
}

export async function ensureSqliteInternalTables(db: Database): Promise<void> {
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
      CREATE INDEX IF NOT EXISTS idx_chimpbase_queue_jobs_pending_due
      ON _chimpbase_queue_jobs(status, available_at_ms, id);
    `,
  );

  db.exec(
    `
      CREATE INDEX IF NOT EXISTS idx_chimpbase_queue_jobs_processing_due
      ON _chimpbase_queue_jobs(status, lease_expires_at_ms, id);
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
      CREATE INDEX IF NOT EXISTS idx_chimpbase_cron_schedules_due
      ON _chimpbase_cron_schedules(next_fire_at_ms, lease_expires_at_ms);
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
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        completed_at TEXT
      );
    `,
  );

  try {
    db.exec("ALTER TABLE _chimpbase_workflow_instances ADD COLUMN current_step_id TEXT");
  } catch {
    // Column already exists on upgraded databases.
  }

  try {
    db.exec("ALTER TABLE _chimpbase_kv ADD COLUMN expires_at TEXT DEFAULT NULL");
  } catch {
    // Column already exists on upgraded databases.
  }

  db.exec(
    `
      CREATE INDEX IF NOT EXISTS idx_chimpbase_workflow_instances_status
      ON _chimpbase_workflow_instances(status, wake_at_ms);
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
      CREATE INDEX IF NOT EXISTS idx_chimpbase_workflow_signals_pending
      ON _chimpbase_workflow_signals(workflow_id, signal_name, consumed_at, id);
    `,
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
    `
      CREATE INDEX IF NOT EXISTS idx_chimpbase_blobs_bucket_prefix
      ON _chimpbase_blobs (bucket, key);
    `,
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
    `
      CREATE INDEX IF NOT EXISTS idx_chimpbase_blob_uploads_expires
      ON _chimpbase_blob_uploads (expires_at_ms);
    `,
  );

  db.exec(
    `
      CREATE INDEX IF NOT EXISTS idx_chimpbase_blob_uploads_bucket_key
      ON _chimpbase_blob_uploads (bucket, key);
    `,
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
  db: Database,
  platform: ChimpbasePlatformShim,
): ChimpbaseEngineAdapter {
  return createSharedSqliteEngineAdapter({
    exec(sql) {
      return db.exec(sql);
    },
    query(sql) {
      const statement = db.query(sql);
      return {
        reader: statement.columnNames.length > 0,
        all(...params) {
          return statement.all(...toSqlBindings(params));
        },
        run(...params) {
          return statement.run(...toSqlBindings(params));
        },
      };
    },
  }, platform);
}

function isSqlBinding(value: unknown): value is SQLQueryBindings {
  if (
    value === null
    || typeof value === "string"
    || typeof value === "number"
    || typeof value === "bigint"
    || typeof value === "boolean"
    || ArrayBuffer.isView(value)
  ) {
    return true;
  }
  return isJsonObject(value) && Object.values(value).every((entry) =>
    entry === null
    || typeof entry === "string"
    || typeof entry === "number"
    || typeof entry === "bigint"
    || typeof entry === "boolean"
    || ArrayBuffer.isView(entry)
  );
}

function toSqlBindings(params: readonly unknown[]): SQLQueryBindings[] {
  const bindings: SQLQueryBindings[] = [];
  for (const param of params) {
    if (!isSqlBinding(param)) {
      throw new TypeError("SQLite query parameters must be supported binding values");
    }
    bindings.push(param);
  }
  return bindings;
}
