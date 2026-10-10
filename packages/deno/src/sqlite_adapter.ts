import { applySqliteMigrations } from "@chimpbase/core";
import { mkdir } from "node:fs/promises";
import { dirname, resolve } from "node:path";

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

interface RawSqliteStatement {
  columns(): Array<{ name: string }>;
  all(...params: unknown[]): unknown[];
  run(...params: unknown[]): {
    changes: number | bigint;
    lastInsertRowid?: bigint | number;
  };
}

interface RawSqliteDatabase {
  close(): void;
  exec(sql: string): unknown;
  prepare(sql: string): RawSqliteStatement;
}

export async function openSqliteDatabase(
  projectDir: string,
  config: ChimpbaseProjectConfig,
): Promise<SqliteDatabase> {
  const DatabaseConstructor = await loadSqliteDatabaseConstructor();

  if (config.storage.engine === "memory" || !(config.storage.path !== null && config.storage.path.length > 0) || config.storage.path === ":memory:") {
    return createSqliteDatabase(new DatabaseConstructor(":memory:"));
  }

  const databasePath = resolve(projectDir, config.storage.path);
  await mkdir(dirname(databasePath), { recursive: true });
  return createSqliteDatabase(new DatabaseConstructor(databasePath));
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

function createSqliteDatabase(db: RawSqliteDatabase): SqliteDatabase {
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
        reader: statement.columns().length > 0 || statementProducesRows(sql),
        all(...params: unknown[]) {
          return statement.all(...params);
        },
        run(...params: unknown[]) {
          const result = statement.run(...params);
          return {
            changes: typeof result.changes === "bigint" ? Number(result.changes) : result.changes,
            lastInsertRowid: result.lastInsertRowid,
          };
        },
      };
    },
  };
}

async function loadSqliteDatabaseConstructor(): Promise<new (path: string) => RawSqliteDatabase> {
  const module = await import("node:sqlite") as {
    DatabaseSync: new (path: string) => RawSqliteDatabase;
  };
  return module.DatabaseSync;
}

function statementProducesRows(sql: string): boolean {
  const normalized = sql.trimStart().toLowerCase();
  return normalized.startsWith("select")
    || normalized.startsWith("pragma")
    || normalized.startsWith("values")
    || normalized.startsWith("explain")
    || normalized.includes(" returning ");
}
