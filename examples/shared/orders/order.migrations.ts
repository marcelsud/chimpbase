import { defineChimpbaseMigrations } from "@chimpbase/core";

const postgresInit = {
  name: "001_init",
  sql: `
    CREATE TABLE IF NOT EXISTS orders (
      id BIGSERIAL PRIMARY KEY,
      customer TEXT NOT NULL,
      amount BIGINT NOT NULL,
      status TEXT NOT NULL,
      assignee TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS order_audit_log (
      id BIGSERIAL PRIMARY KEY,
      order_id BIGINT NOT NULL,
      event TEXT NOT NULL,
      payload JSONB NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS order_notifications (
      id BIGSERIAL PRIMARY KEY,
      order_id BIGINT NOT NULL,
      channel TEXT NOT NULL,
      status TEXT NOT NULL,
      detail TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS order_backlog_snapshots (
      id BIGSERIAL PRIMARY KEY,
      pending_count INTEGER NOT NULL,
      in_progress_count INTEGER NOT NULL,
      total_count INTEGER NOT NULL,
      snapshot_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `,
};

const sqliteInit = {
  name: "001_init",
  sql: `
    CREATE TABLE IF NOT EXISTS orders (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      customer TEXT NOT NULL,
      amount INTEGER NOT NULL,
      status TEXT NOT NULL,
      assignee TEXT,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS order_audit_log (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      order_id INTEGER NOT NULL,
      event TEXT NOT NULL,
      payload TEXT NOT NULL,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS order_notifications (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      order_id INTEGER NOT NULL,
      channel TEXT NOT NULL,
      status TEXT NOT NULL,
      detail TEXT,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS order_backlog_snapshots (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      pending_count INTEGER NOT NULL,
      in_progress_count INTEGER NOT NULL,
      total_count INTEGER NOT NULL,
      snapshot_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
  `,
};

export default defineChimpbaseMigrations({
  postgres: [postgresInit],
  sqlite: [sqliteInit],
});
