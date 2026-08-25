import { access, readdir, readFile } from "node:fs/promises";
import { extname, join, resolve } from "node:path";
import { pathToFileURL } from "node:url";

import {
  defineChimpbaseMigrations,
  listChimpbaseMigrationsForEngine,
  type ChimpbaseMigration,
  type ChimpbaseMigrationsDefinition,
  type ChimpbaseProjectConfig,
} from "@chimpbase/core";
import { isArrayValue, isJsonObject } from "@chimpbase/runtime";
import { loadProjectAppDefinition } from "./app.ts";

export interface SqlMigration extends ChimpbaseMigration {}

const PROJECT_MIGRATIONS_MODULE_CANDIDATES = [
  "chimpbase.migrations.ts",
  "chimpbase.migrations.js",
  "chimpbase.migrations.mts",
  "chimpbase.migrations.mjs",
] as const;

export async function resolveLocalMigrationsDir(
  migrationsDir: string | null,
  engine: ChimpbaseProjectConfig["storage"]["engine"],
): Promise<string | null> {
  if (!(migrationsDir !== null && migrationsDir.length > 0)) {
    return null;
  }

  const engineDir = resolve(migrationsDir, resolveMigrationDialect(engine));
  return await directoryHasSqlFiles(engineDir) ? engineDir : migrationsDir;
}

export async function resolvePostgresMigrationsDir(projectDir: string): Promise<string | null> {
  const baseDir = resolve(projectDir, "migrations");
  const postgresDir = join(baseDir, "postgres");

  if (await directoryHasSqlFiles(postgresDir)) {
    return postgresDir;
  }

  return await directoryHasSqlFiles(baseDir) ? baseDir : null;
}

export async function readSqlMigrations(migrationsDir: string | null): Promise<SqlMigration[]> {
  if (!(migrationsDir !== null && migrationsDir.length > 0)) {
    return [];
  }

  let entries: string[];
  try {
    entries = await readdir(migrationsDir);
  } catch {
    return [];
  }

  const migrations = entries
    .filter((entry) => extname(entry) === ".sql")
    .sort();

  return await Promise.all(
    migrations.map(async (name) => ({
      name,
      sql: await readFile(resolve(migrationsDir, name), "utf8"),
    })),
  );
}

export async function loadProjectMigrations(
  projectDir: string,
  engine: ChimpbaseProjectConfig["storage"]["engine"],
  options: { migrationsDir?: string | null } = {},
): Promise<SqlMigration[]> {
  const app = await loadProjectAppDefinition(projectDir);
  if ((app !== null)) {
    const appMigrations = [...listChimpbaseMigrationsForEngine(app.migrations, engine)];
    if (appMigrations.length > 0) {
      return appMigrations;
    }
  }

  const definedMigrations = await loadProjectMigrationsDefinition(projectDir);
  if ((definedMigrations !== null)) {
    return [...listChimpbaseMigrationsForEngine(definedMigrations, engine)];
  }

  return await readSqlMigrations(await resolveLocalMigrationsDir(
    options.migrationsDir ?? resolve(projectDir, "migrations"),
    engine,
  ));
}

export async function loadProjectPostgresMigrations(
  projectDir: string,
  options: { migrationsDir?: string | null } = {},
): Promise<SqlMigration[]> {
  const app = await loadProjectAppDefinition(projectDir);
  if ((app !== null)) {
    const appMigrations = [...app.migrations.postgres];
    if (appMigrations.length > 0) {
      return appMigrations;
    }
  }

  const definedMigrations = await loadProjectMigrationsDefinition(projectDir);
  if ((definedMigrations !== null)) {
    return [...definedMigrations.postgres];
  }

  if ((options.migrationsDir !== null && options.migrationsDir !== undefined && options.migrationsDir.length > 0)) {
    return await readSqlMigrations(await resolveLocalMigrationsDir(options.migrationsDir, "postgres"));
  }

  return await readSqlMigrations(await resolvePostgresMigrationsDir(projectDir));
}

export async function loadProjectMigrationsDefinition(
  projectDir: string,
): Promise<ChimpbaseMigrationsDefinition | null> {
  const modulePath = await resolveProjectMigrationsModulePath(projectDir);
  if (!(modulePath !== null && modulePath.length > 0)) {
    return null;
  }

  const moduleUrl = pathToFileURL(modulePath);
  moduleUrl.searchParams.set("chimpbase_migrations", globalThis.crypto.randomUUID());
  const moduleExports: unknown = await import(moduleUrl.href);
  if (!isJsonObject(moduleExports)) {
    throw new Error(`project migrations module did not resolve to a module namespace: ${modulePath}`);
  }

  return coerceProjectMigrationsDefinition(moduleExports, modulePath);
}

export async function directoryHasSqlFiles(path: string): Promise<boolean> {
  try {
    const entries = await readdir(path);
    return entries.some((entry) => entry.endsWith(".sql"));
  } catch {
    return false;
  }
}

async function resolveProjectMigrationsModulePath(projectDir: string): Promise<string | null> {
  for (const candidate of PROJECT_MIGRATIONS_MODULE_CANDIDATES) {
    const path = resolve(projectDir, candidate);
    if (await fileExists(path)) {
      return path;
    }
  }

  return null;
}

function coerceProjectMigrationsDefinition(
  moduleExports: Record<string, unknown>,
  modulePath: string,
): ChimpbaseMigrationsDefinition {
  const candidate = moduleExports.default ?? moduleExports.migrations;
  if (!isJsonObject(candidate)) {
    throw new Error(`project migrations module must export a default object or named "migrations": ${modulePath}`);
  }

  return defineChimpbaseMigrations({
    postgres: coerceMigrationList(candidate.postgres, modulePath, "postgres"),
    sqlite: coerceMigrationList(candidate.sqlite, modulePath, "sqlite"),
  });
}

function coerceMigrationList(
  value: unknown,
  modulePath: string,
  engine: "postgres" | "sqlite",
): readonly SqlMigration[] | undefined {
  if (value === undefined) {
    return undefined;
  }

  if (!isArrayValue(value)) {
    throw new Error(`project migrations for ${engine} must be an array: ${modulePath}`);
  }

  return value.map((entry, index) => coerceMigration(entry, modulePath, engine, index));
}

function coerceMigration(
  value: unknown,
  modulePath: string,
  engine: "postgres" | "sqlite",
  index: number,
): SqlMigration {
  if (!isJsonObject(value)) {
    throw new Error(`project migration ${engine}[${index}] must be an object: ${modulePath}`);
  }

  if (typeof value.name !== "string" || value.name.length === 0) {
    throw new Error(`project migration ${engine}[${index}] must define a non-empty name: ${modulePath}`);
  }

  if (typeof value.sql !== "string") {
    throw new Error(`project migration ${engine}[${index}] must define sql as a string: ${modulePath}`);
  }

  return {
    name: value.name,
    sql: value.sql,
  };
}

async function fileExists(path: string): Promise<boolean> {
  try {
    await access(path);
    return true;
  } catch {
    return false;
  }
}

function resolveMigrationDialect(
  engine: ChimpbaseProjectConfig["storage"]["engine"],
): "postgres" | "sqlite" {
  return engine === "postgres" ? "postgres" : "sqlite";
}
