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

export { ensureSqliteInternalTables } from "@chimpbase/core";

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
