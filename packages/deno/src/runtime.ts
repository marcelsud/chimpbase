import { join, resolve } from "node:path";

import {
  normalizeProjectConfig,
  type ChimpbaseAppDefinition,
  type ChimpbaseMigration,
  type ChimpbaseMigrationSource,
  type ChimpbasePlatformShim,
  type ChimpbaseProjectConfig,
} from "@chimpbase/core";
import {
  ChimpbaseHost,
  createRuntimeHost,
  inferNumberEnv,
  inferServerPort,
  inferStorageEngine,
  inferSubscriptionDispatchMode,
  type ActionExecutionResult,
  type ChimpbaseRuntimeEnvironment,
  type ChimpbaseRuntimeShim,
  type CreateHostOptions,
  type DrainOptions,
  type DrainResult,
  type RouteExecutionResult,
  type RuntimeHostInstanceOptions,
  type StartedHost,
  type TelemetryRecord,
} from "@chimpbase/host";
import { loadProjectAppDefinition } from "@chimpbase/tooling/app";
import {
  applyInlinePostgresMigrations,
  applyPostgresSqlMigrations,
  createPostgresEngineAdapter,
  ensurePostgresInternalTables,
  openPostgresPool,
  PostgresPollingEventBus,
} from "@chimpbase/postgres";

import { getDenoEnv, getDenoEnvObject, requireDenoServe, type DenoServeHandle } from "./deno_runtime.ts";
import {
  applyInlineSqlMigrations,
  applySqlMigrations,
  createSqliteEngineAdapter,
  ensureSqliteInternalTables,
  openSqliteDatabase,
} from "./sqlite_deno_adapter.ts";

export interface StartedDenoHost extends StartedHost<ChimpbaseDenoHost, DenoServeHandle> {}
export type { ActionExecutionResult, CreateHostOptions, DenoServeHandle, DrainOptions, DrainResult, RouteExecutionResult, TelemetryRecord };

const denoEnvironment: ChimpbaseRuntimeEnvironment = {
  get(name: string): string | undefined {
    return getDenoEnv(name);
  },
  toObject(): Record<string, string> {
    return getDenoEnvObject();
  },
};

export const denoRuntimeShim: ChimpbaseRuntimeShim<DenoServeHandle> = {
  debugNamespace: "@chimpbase/deno",
  env: denoEnvironment,
  server: {
    create(options, handler) {
      const serve = requireDenoServe();
      const server = serve({ port: options.port }, handler);
      return {
        ...server,
        port: options.port,
      };
    },
    async stop(server) {
      server.shutdown?.();
      await server.finished;
    },
  },
  storage: {
    async open(
      projectDir: string,
      config: ChimpbaseProjectConfig,
      platform: ChimpbasePlatformShim,
      inlineMigrations: readonly ChimpbaseMigration[],
      migrationSource: ChimpbaseMigrationSource,
      migrationsSql: string[],
    ) {
      const resolvedMigrations = [
        ...await migrationSource.list(),
        ...inlineMigrations,
      ];

      if (config.storage.engine === "postgres") {
        if (!(config.storage.url !== null && config.storage.url.length > 0)) {
          throw new Error("@chimpbase/deno requires storage.url for postgres storage");
        }
        const pool = openPostgresPool(config);
        await applyPostgresSqlMigrations(pool, resolvedMigrations.map((migration) => migration.sql));
        await applyInlinePostgresMigrations(pool, migrationsSql);
        await ensurePostgresInternalTables(pool);
        const eventBus = new PostgresPollingEventBus({ pool });
        return {
          createAdapter() {
            return createPostgresEngineAdapter(pool, platform);
          },
          eventBus,
          storage: {
            close() {
              return pool.end();
            },
          },
          supportsConcurrentWorkers: true,
        };
      }

      const db = await openSqliteDatabase(projectDir, config);
      await applySqlMigrations(db, resolvedMigrations.map((migration) => migration.sql));
      await applyInlineSqlMigrations(db, migrationsSql);
      await ensureSqliteInternalTables(db);
      return {
        createAdapter() {
          return createSqliteEngineAdapter(db, platform);
        },
        storage: {
          close() {
            db.close();
          },
        },
        supportsConcurrentWorkers: false,
      };
    },
  },
};

export class ChimpbaseDenoHost extends ChimpbaseHost<DenoServeHandle> {
  constructor(options: RuntimeHostInstanceOptions<DenoServeHandle>) {
    super(options);
  }

  static async load(projectDirInput: string): Promise<ChimpbaseDenoHost> {
    const projectDir = resolve(projectDirInput);
    const app = await loadProjectAppDefinitionOrThrow(projectDir);
    const config = buildConfigFromApp(app);
    return await ChimpbaseDenoHost.create({
      app,
      config,
      projectDir,
    });
  }

  static async create(options: CreateHostOptions): Promise<ChimpbaseDenoHost> {
    return await createRuntimeHost(ChimpbaseDenoHost, denoRuntimeShim, options);
  }
}

function buildConfigFromApp(app: ChimpbaseAppDefinition): ChimpbaseProjectConfig {
  const storageEngine = inferStorageEngine(denoEnvironment, {});
  return normalizeProjectConfig({
    project: {
      name: app.project.name,
    },
    server: {
      port: inferServerPort(denoEnvironment),
    },
    storage: {
      engine: storageEngine,
      path: storageEngine === "memory" || storageEngine === "postgres"
        ? null
        : denoEnvironment.get("CHIMPBASE_STORAGE_PATH") ?? join("data", `${app.project.name}.db`),
      url: denoEnvironment.get("CHIMPBASE_DATABASE_URL") ?? denoEnvironment.get("DATABASE_URL") ?? null,
    },
    subscriptions: {
      dispatch: inferSubscriptionDispatchMode(denoEnvironment),
    },
    telemetry: {
      minLevel: app.telemetry.minLevel,
      persist: app.telemetry.persist,
    },
    worker: {
      concurrency: inferNumberEnv(denoEnvironment, "CHIMPBASE_WORKER_CONCURRENCY"),
      leaseMs: inferNumberEnv(denoEnvironment, "CHIMPBASE_WORKER_LEASE_MS"),
      maxAttempts: app.worker.maxAttempts,
      pollIntervalMs: inferNumberEnv(denoEnvironment, "CHIMPBASE_WORKER_POLL_INTERVAL_MS"),
      retryDelayMs: app.worker.retryDelayMs,
    },
    workflows: {
      contractsDir: app.workflows.contractsDir ?? undefined,
    },
  });
}

async function loadProjectAppDefinitionOrThrow(projectDir: string): Promise<ChimpbaseAppDefinition> {
  const app = await loadProjectAppDefinition(projectDir);
  if (!(app !== null)) {
    throw new Error(`missing chimpbase.app.ts in ${projectDir}`);
  }

  return app;
}
