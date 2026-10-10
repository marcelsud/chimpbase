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

export { ensureSqliteInternalTables } from "@chimpbase/core";

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
