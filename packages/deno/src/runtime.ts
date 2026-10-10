import {
  type ChimpbaseMigration,
  type ChimpbaseMigrationSource,
  type ChimpbasePlatformShim,
  type ChimpbaseProjectConfig,
} from "@chimpbase/core";
import {
  ChimpbaseHost,
  createRuntimeHost,
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
import { openPostgresStorage } from "@chimpbase/postgres";

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
        return await openPostgresStorage(config, platform, resolvedMigrations, migrationsSql);
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

export class ChimpbaseDenoHost extends ChimpbaseHost<DenoServeHandle> {
  constructor(options: RuntimeHostInstanceOptions<DenoServeHandle>) {
    super(options);
  }

  static async create(options: CreateHostOptions): Promise<ChimpbaseDenoHost> {
    return await createRuntimeHost(ChimpbaseDenoHost, denoRuntimeShim, options);
  }
}
