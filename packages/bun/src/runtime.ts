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
        return await openPostgresStorage(config, platform, resolvedMigrations, migrationsSql);
      }

      const db = await openSqliteDatabase(projectDir, config);
      try {
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
      } catch (error) {
        try {
          db.close();
        } catch {
          // Preserve the initialization error after attempting database cleanup.
        }
        throw error;
      }
    },
  },
};

export class ChimpbaseBunHost extends ChimpbaseHost<Bun.Server<unknown>> {
  constructor(options: RuntimeHostInstanceOptions<Bun.Server<unknown>>) {
    super(options);
  }

  static async create(options: CreateHostOptions): Promise<ChimpbaseBunHost> {
    return await createRuntimeHost(ChimpbaseBunHost, bunRuntimeShim, options);
  }
}
