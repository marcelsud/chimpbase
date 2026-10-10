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

import {
  applyInlineSqlMigrations,
  applySqlMigrations,
  createSqliteEngineAdapter,
  ensureSqliteInternalTables,
  openSqliteDatabase,
} from "./sqlite_bun_adapter.ts";

export interface StartedBunHost extends StartedHost<ChimpbaseBunHost, Bun.Server<unknown>> {}
export type { ActionExecutionResult, CreateHostOptions, DrainOptions, DrainResult, RouteExecutionResult, TelemetryRecord };

const bunEnvironment: ChimpbaseRuntimeEnvironment = {
  get(name: string): string | undefined {
    return Bun.env[name];
  },
  toObject(): Record<string, string> {
    return Object.fromEntries(
      Object.entries(process.env).flatMap(([key, value]) => typeof value === "string" ? [[key, value]] : []),
    );
  },
};

export const bunRuntimeShim: ChimpbaseRuntimeShim<Bun.Server<unknown>> = {
  debugNamespace: "@chimpbase/bun",
  env: bunEnvironment,
  server: {
    create(options, handler) {
      return Bun.serve({
        fetch: handler,
        port: options.port,
      });
    },
    async stop(server) {
      await server.stop(true);
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
          throw new Error("@chimpbase/bun requires storage.url for postgres storage");
        }
        const pool = openPostgresPool(config);
        await applyPostgresSqlMigrations(pool, resolvedMigrations);
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
      await applySqlMigrations(db, resolvedMigrations);
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

export class ChimpbaseBunHost extends ChimpbaseHost<Bun.Server<unknown>> {
  constructor(options: RuntimeHostInstanceOptions<Bun.Server<unknown>>) {
    super(options);
  }

  static async load(projectDirInput: string): Promise<ChimpbaseBunHost> {
    const projectDir = resolve(projectDirInput);
    const app = await loadProjectAppDefinitionOrThrow(projectDir);
    const config = buildConfigFromApp(app);
    return await ChimpbaseBunHost.create({
      app,
      config,
      projectDir,
    });
  }

  static async create(options: CreateHostOptions): Promise<ChimpbaseBunHost> {
    return await createRuntimeHost(ChimpbaseBunHost, bunRuntimeShim, options);
  }
}

function buildConfigFromApp(app: ChimpbaseAppDefinition): ChimpbaseProjectConfig {
  const storageEngine = inferStorageEngine(bunEnvironment, {});
  return normalizeProjectConfig({
    project: {
      name: app.project.name,
    },
    server: {
      port: inferServerPort(bunEnvironment),
    },
    storage: {
      engine: storageEngine,
      path: storageEngine === "memory" || storageEngine === "postgres"
        ? null
        : bunEnvironment.get("CHIMPBASE_STORAGE_PATH") ?? join("data", `${app.project.name}.db`),
      url: bunEnvironment.get("CHIMPBASE_DATABASE_URL") ?? bunEnvironment.get("DATABASE_URL") ?? null,
    },
    subscriptions: {
      dispatch: inferSubscriptionDispatchMode(bunEnvironment),
    },
    telemetry: {
      minLevel: app.telemetry.minLevel,
      persist: app.telemetry.persist,
    },
    worker: {
      concurrency: inferNumberEnv(bunEnvironment, "CHIMPBASE_WORKER_CONCURRENCY"),
      leaseMs: inferNumberEnv(bunEnvironment, "CHIMPBASE_WORKER_LEASE_MS"),
      maxAttempts: app.worker.maxAttempts,
      pollIntervalMs: inferNumberEnv(bunEnvironment, "CHIMPBASE_WORKER_POLL_INTERVAL_MS"),
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
