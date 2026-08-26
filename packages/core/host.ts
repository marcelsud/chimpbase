export interface ChimpbasePlatformShim {
  hashString(input: string): string;
  now(): number;
  randomUUID(): string;
}

export interface ChimpbaseSecretsSource {
  get(name: string): string | null;
}

export type ChimpbaseStorageEngine = "memory" | "postgres" | "sqlite";
export type ChimpbaseMigrationEngine = Exclude<ChimpbaseStorageEngine, "memory">;

export interface ChimpbaseMigration {
  name: string;
  owner?: string;
  sql: string;
}

export interface ChimpbaseMigrationsDefinition {
  postgres: readonly ChimpbaseMigration[];
  sqlite: readonly ChimpbaseMigration[];
}

export interface ChimpbaseMigrationsDefinitionInput {
  postgres?: readonly ChimpbaseMigration[];
  sqlite?: readonly ChimpbaseMigration[];
}

export interface ChimpbaseMigrationSource {
  list(): Promise<ChimpbaseMigration[]>;
}

export interface ChimpbaseDrainOptions {
  maxDurationMs?: number;
  maxRuns?: number;
}

export interface ChimpbaseDrainResult {
  cronSchedules: number;
  idle: boolean;
  queueJobs: number;
  runs: number;
  stopReason: "idle" | "max_duration" | "max_runs";
}

const DETERMINISTIC_HASH_OFFSET_BASIS = 0xcbf29ce484222325n;
const DETERMINISTIC_HASH_PRIME = 0x100000001b3n;
const textEncoder = new TextEncoder();

export function createDefaultChimpbasePlatformShim(): ChimpbasePlatformShim {
  return {
    hashString(input: string): string {
      return hashDeterministicString(input);
    },
    now(): number {
      return Date.now();
    },
    randomUUID(): string {
      if (typeof globalThis.crypto?.randomUUID !== "function") {
        throw new Error("global crypto.randomUUID is unavailable");
      }

      return globalThis.crypto.randomUUID();
    },
  };
}

export function defineChimpbaseMigration(migration: ChimpbaseMigration): ChimpbaseMigration {
  return {
    name: migration.name,
    owner: migration.owner ?? "framework",
    sql: migration.sql,
  };
}

export function defineChimpbaseMigrations(
  input: ChimpbaseMigrationsDefinitionInput = {},
): ChimpbaseMigrationsDefinition {
  return {
    postgres: normalizeMigrations(input.postgres),
    sqlite: normalizeMigrations(input.sqlite),
  };
}

export function listChimpbaseMigrationsForEngine(
  definition: ChimpbaseMigrationsDefinitionInput | null | undefined,
  engine: ChimpbaseStorageEngine,
): readonly ChimpbaseMigration[] {
  const normalized = defineChimpbaseMigrations(definition ?? {});
  return engine === "postgres" ? normalized.postgres : normalized.sqlite;
}

function hashDeterministicString(input: string): string {
  let hash = DETERMINISTIC_HASH_OFFSET_BASIS;

  for (const byte of textEncoder.encode(input)) {
    hash ^= BigInt(byte);
    hash = BigInt.asUintN(64, hash * DETERMINISTIC_HASH_PRIME);
  }

  return hash.toString(16).padStart(16, "0");
}

function normalizeMigrations(
  migrations: readonly ChimpbaseMigration[] | undefined,
): readonly ChimpbaseMigration[] {
  return (migrations ?? []).map((migration) => defineChimpbaseMigration(migration));
}
