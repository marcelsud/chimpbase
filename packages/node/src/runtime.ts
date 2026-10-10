import { join, resolve } from "node:path";
import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import type { ReadableStream as NodeReadableStream } from "node:stream/web";

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
} from "./sqlite_node_adapter.ts";

export interface NodeServeHandle {
  port: number;
  server: Server;
}

export interface StartedNodeHost extends StartedHost<ChimpbaseNodeHost, NodeServeHandle> {}
export type { ActionExecutionResult, CreateHostOptions, DrainOptions, DrainResult, RouteExecutionResult, TelemetryRecord };

const nodeEnvironment: ChimpbaseRuntimeEnvironment = {
  get(name: string): string | undefined {
    return process.env[name];
  },
  toObject(): Record<string, string> {
    return Object.fromEntries(
      Object.entries(process.env).flatMap(([key, value]) => typeof value === "string" ? [[key, value]] : []),
    );
  },
};

export const nodeRuntimeShim: ChimpbaseRuntimeShim<NodeServeHandle> = {
  debugNamespace: "@chimpbase/node",
  env: nodeEnvironment,
  server: {
    create(
      options: { port: number },
      handler: (request: Request) => Response | Promise<Response>,
    ): NodeServeHandle {
      const server = createServer(async (request, response) => {
        try {
          const webRequest = createWebRequest(request, options.port);
          const webResponse = await handler(webRequest);
          await writeNodeResponse(response, webResponse);
        } catch (error) {
          if (response.destroyed) return;
          if (response.headersSent) {
            response.destroy(error instanceof Error ? error : new Error(String(error)));
            return;
          }
          response.statusCode = 500;
          response.end(error instanceof Error ? error.message : String(error));
        }
      });
      server.listen(options.port);
      return {
        port: options.port,
        server,
      };
    },
    async stop(handle: NodeServeHandle): Promise<void> {
      await new Promise<void>((resolveStop, rejectStop) => {
        handle.server.close((error?: Error | null) => {
          if ((error !== null && error !== undefined)) {
            rejectStop(error);
            return;
          }

          resolveStop();
        });
      });
    },
  },
  storage: {
    async open(
      _projectDir: string,
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
          throw new Error("@chimpbase/node requires storage.url for postgres storage");
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

      const db = await openSqliteDatabase(_projectDir, config);
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

export class ChimpbaseNodeHost extends ChimpbaseHost<NodeServeHandle> {
  constructor(options: RuntimeHostInstanceOptions<NodeServeHandle>) {
    super(options);
  }

  static async load(projectDirInput: string): Promise<ChimpbaseNodeHost> {
    const projectDir = resolve(projectDirInput);
    const app = await loadProjectAppDefinitionOrThrow(projectDir);
    const config = buildConfigFromApp(app);
    return await ChimpbaseNodeHost.create({
      app,
      config,
      projectDir,
    });
  }

  static async create(options: CreateHostOptions): Promise<ChimpbaseNodeHost> {
    return await createRuntimeHost(ChimpbaseNodeHost, nodeRuntimeShim, options);
  }
}

function buildConfigFromApp(app: ChimpbaseAppDefinition): ChimpbaseProjectConfig {
  const storageEngine = inferStorageEngine(nodeEnvironment, {});
  return normalizeProjectConfig({
    project: {
      name: app.project.name,
    },
    server: {
      port: inferServerPort(nodeEnvironment),
    },
    storage: {
      engine: storageEngine,
      path: storageEngine === "memory" || storageEngine === "postgres"
        ? null
        : nodeEnvironment.get("CHIMPBASE_STORAGE_PATH") ?? join("data", `${app.project.name}.db`),
      url: nodeEnvironment.get("CHIMPBASE_DATABASE_URL") ?? nodeEnvironment.get("DATABASE_URL") ?? null,
    },
    subscriptions: {
      dispatch: inferSubscriptionDispatchMode(nodeEnvironment),
    },
    telemetry: {
      minLevel: app.telemetry.minLevel,
      persist: app.telemetry.persist,
    },
    worker: {
      concurrency: inferNumberEnv(nodeEnvironment, "CHIMPBASE_WORKER_CONCURRENCY"),
      leaseMs: inferNumberEnv(nodeEnvironment, "CHIMPBASE_WORKER_LEASE_MS"),
      maxAttempts: app.worker.maxAttempts,
      pollIntervalMs: inferNumberEnv(nodeEnvironment, "CHIMPBASE_WORKER_POLL_INTERVAL_MS"),
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

function isByteReadableStream(value: unknown): value is ReadableStream<Uint8Array> {
  return value instanceof ReadableStream;
}

function createWebRequest(request: IncomingMessage, port: number): Request {
  const headers = new Headers();
  for (const [name, value] of Object.entries(request.headers)) {
    if (value === undefined) {
      continue;
    }

    if (Array.isArray(value)) {
      for (const item of value) {
        headers.append(name, item);
      }
      continue;
    }

    headers.set(name, value);
  }

  const method = request.method ?? "GET";
  const url = new URL(request.url ?? "/", `http://${request.headers.host ?? `127.0.0.1:${port}`}`);
  let body: ReadableStream<Uint8Array> | undefined;
  if (method !== "GET" && method !== "HEAD") {
    const stream: unknown = Readable.toWeb(request);
    if (!isByteReadableStream(stream)) {
      throw new TypeError("Node request did not produce a web byte stream");
    }
    body = stream;
  }

  return new Request(url, {
    body,
    headers,
    method,
    ...((body !== undefined) ? { duplex: "half" } : {}),
  } as RequestInit);
}

async function writeNodeResponse(response: ServerResponse, webResponse: Response): Promise<void> {
  response.statusCode = webResponse.status;
  response.statusMessage = webResponse.statusText;
  webResponse.headers.forEach((value, name) => {
    response.setHeader(name, value);
  });

  if (!(webResponse.body !== null)) {
    response.end();
    return;
  }

  await pipeline(Readable.fromWeb(webResponse.body as unknown as NodeReadableStream<Uint8Array>), response);
}
