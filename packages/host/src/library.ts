import { join, resolve } from "node:path";

import {
  defineChimpbaseApp,
  normalizeProjectConfig,
  type ChimpbaseAppDefinition,
  type ChimpbaseAppDefinitionInput,
  type ChimpbaseBlobDriver,
  type ChimpbaseBlobSigner,
  type ChimpbaseSecretsSource,
} from "@chimpbase/core";
import type { ChimpbaseTelemetrySink } from "@chimpbase/runtime";
import { loadProjectAppDefinition } from "@chimpbase/tooling/app";
import type { ChimpbaseSchemaSyncOptions, ChimpbaseSchemaSyncResult } from "@chimpbase/tooling/schema";
import { syncChimpbaseSchemaArtifacts } from "@chimpbase/tooling/schema";
import {
  syncChimpbaseModuleArtifacts,
  type SyncChimpbaseModuleArtifactsOptions,
  type SyncChimpbaseModuleArtifactsResult,
} from "@chimpbase/tooling/modules";
import type { WorkflowContractSyncOptions, WorkflowContractSyncResult } from "@chimpbase/tooling/workflow_contracts";

import {
  ChimpbaseHost,
  createRuntimeHost,
  inferDebugEnabled,
  inferMigrationsDir,
  inferNumberEnv,
  inferServerPort,
  inferStorageEngine,
  inferSubscriptionDispatchMode,
  type ActionExecutionResult,
  type ChimpbaseRuntimeShim,
  type CreateHostOptions,
  type RuntimeHostInstanceOptions,
  type StartedHost,
} from "./runtime.ts";

export interface StartChimpbaseProjectOptions {
  projectDir?: string;
  runWorker?: boolean;
  serve?: boolean;
}

export interface StartedChimpbaseProject<THost, TServer> {
  host: THost;
  server: TServer | null;
  stop(): Promise<void>;
}

export interface CreateChimpbaseBlobsOptions {
  driver: ChimpbaseBlobDriver;
  buckets: readonly string[];
  signer?: ChimpbaseBlobSigner;
}

export interface CreateChimpbaseRuntimeOptions {
  blobs?: CreateChimpbaseBlobsOptions;
  debug?: boolean;
  migrationsDir?: string;
  migrationsSql?: string[];
  projectDir?: string;
  secrets?: ChimpbaseSecretsSource;
  server?: {
    port?: number;
  };
  sinks?: ChimpbaseTelemetrySink[];
  storage?: {
    engine?: "memory" | "postgres" | "sqlite";
    path?: string | null;
    url?: string | null;
  };
  subscriptions?: {
    dispatch?: "async" | "sync";
    idempotency?: {
      retention?: {
        enabled?: boolean;
        maxAgeDays?: number;
        schedule?: string;
      };
    };
  };
  telemetryRetention?: {
    enabled?: boolean;
    maxAgeDays?: number;
    schedule?: string;
  };
  workerRuntime?: {
    concurrency?: number;
    leaseMs?: number;
    pollIntervalMs?: number;
  };
}

export interface CreateChimpbaseFromAppOptions extends CreateChimpbaseRuntimeOptions {
  app: ChimpbaseAppDefinition | ChimpbaseAppDefinitionInput;
}

export interface CreateChimpbaseAppFieldsOptions extends CreateChimpbaseRuntimeOptions, ChimpbaseAppDefinitionInput {}

export type CreateChimpbaseOptions = CreateChimpbaseAppFieldsOptions | CreateChimpbaseFromAppOptions;
export type CreateChimpbaseWithDefaultsOptions = Omit<CreateChimpbaseRuntimeOptions, "projectDir">;
export type LoadChimpbaseAppOptions = CreateChimpbaseRuntimeOptions;

export interface SyncChimpbaseWorkflowContractsOptions extends WorkflowContractSyncOptions {
  projectDir?: string;
}

export interface SyncChimpbaseModulesOptions extends SyncChimpbaseModuleArtifactsOptions {
  projectDir?: string;
}

export interface SyncChimpbaseSchemaOptions extends ChimpbaseSchemaSyncOptions {
  projectDir?: string;
}

export interface ChimpbaseRuntimeLibrary<THost, TServer> {
  createChimpbase: ((
    options: CreateChimpbaseOptions,
  ) => Promise<THost>) & {
    from(projectDir: string, options?: CreateChimpbaseWithDefaultsOptions): Promise<THost>;
  };
  loadChimpbaseApp(app: ChimpbaseAppDefinition | ChimpbaseAppDefinitionInput, options?: LoadChimpbaseAppOptions): Promise<THost>;
  loadChimpbaseProject(projectDir?: string): Promise<THost>;
  runChimpbaseAction(
    actionName: string,
    args?: unknown[] | unknown,
    options?: { projectDir?: string },
  ): Promise<{ host: THost; outcome: ActionExecutionResult }>;
  runChimpbaseAppAction(
    app: ChimpbaseAppDefinition | ChimpbaseAppDefinitionInput,
    actionName: string,
    args?: unknown[] | unknown,
    options?: LoadChimpbaseAppOptions,
  ): Promise<{ host: THost; outcome: ActionExecutionResult }>;
  startChimpbaseApp(
    options: CreateChimpbaseFromAppOptions & Pick<StartChimpbaseProjectOptions, "serve" | "runWorker">,
  ): Promise<StartedChimpbaseProject<THost, TServer>>;
  startChimpbaseProject(options?: StartChimpbaseProjectOptions): Promise<StartedChimpbaseProject<THost, TServer>>;
  syncChimpbaseModules(options?: SyncChimpbaseModulesOptions): Promise<SyncChimpbaseModuleArtifactsResult>;
  syncChimpbaseSchema(options?: SyncChimpbaseSchemaOptions): Promise<ChimpbaseSchemaSyncResult>;
  syncChimpbaseWorkflowContracts(
    options?: SyncChimpbaseWorkflowContractsOptions,
  ): Promise<{ host: THost; result: WorkflowContractSyncResult }>;
}

export function createChimpbaseRuntimeLibrary<
  TServer,
  THost extends ChimpbaseHost<TServer>,
>(
  HostClass: new (options: RuntimeHostInstanceOptions<TServer>) => THost,
  runtime: ChimpbaseRuntimeShim<TServer>,
): ChimpbaseRuntimeLibrary<THost, TServer> {
  async function loadChimpbaseProject(projectDir = "."): Promise<THost> {
    const resolvedProjectDir = resolve(projectDir);
    const app = await loadProjectAppDefinitionOrThrow(resolvedProjectDir);
    return await loadChimpbaseApp(app, { projectDir: resolvedProjectDir });
  }

  async function loadChimpbaseApp(
    app: ChimpbaseAppDefinition | ChimpbaseAppDefinitionInput,
    options: LoadChimpbaseAppOptions = {},
  ): Promise<THost> {
    return await createChimpbaseImpl({
      ...options,
      app,
    });
  }

  async function createChimpbaseImpl(
    options: CreateChimpbaseOptions,
  ): Promise<THost> {
    return await createChimpbaseFromApp(normalizeCreateChimpbaseOptions(options));
  }

  async function createChimpbaseFrom(
    projectDir: string,
    options: CreateChimpbaseWithDefaultsOptions = {},
  ): Promise<THost> {
    const resolvedProjectDir = resolve(projectDir);
    const app = await loadProjectAppDefinitionOrThrow(resolvedProjectDir);
    return await createChimpbaseImpl({
      app,
      ...options,
      projectDir: resolvedProjectDir,
    });
  }

  const createChimpbase = Object.assign(createChimpbaseImpl, {
    from: createChimpbaseFrom,
  });

  async function runChimpbaseAction(
    actionName: string,
    args: unknown[] | unknown = [],
    options: { projectDir?: string } = {},
  ): Promise<{ host: THost; outcome: ActionExecutionResult }> {
    const host = await loadChimpbaseProject(options.projectDir);

    try {
      const outcome = await host.executeAction(actionName, args);
      return { host, outcome };
    } catch (error) {
      await host.close();
      throw error;
    }
  }

  async function startChimpbaseProject(
    options: StartChimpbaseProjectOptions = {},
  ): Promise<StartedChimpbaseProject<THost, TServer>> {
    const host = await loadChimpbaseProject(options.projectDir);
    return startLoadedHost(host, options.serve, options.runWorker);
  }

  async function syncChimpbaseWorkflowContracts(
    options: SyncChimpbaseWorkflowContractsOptions = {},
  ): Promise<{ host: THost; result: WorkflowContractSyncResult }> {
    const host = await loadChimpbaseProject(options.projectDir);

    try {
      const result = await host.syncWorkflowContracts(options);
      return { host, result };
    } catch (error) {
      await host.close();
      throw error;
    }
  }

  async function syncChimpbaseSchema(
    options: SyncChimpbaseSchemaOptions = {},
  ): Promise<ChimpbaseSchemaSyncResult> {
    return await syncChimpbaseSchemaArtifacts(options.projectDir ?? ".", options);
  }

  async function syncChimpbaseModules(
    options: SyncChimpbaseModulesOptions = {},
  ): Promise<SyncChimpbaseModuleArtifactsResult> {
    const projectDir = resolve(options.projectDir ?? ".");
    const app = await loadProjectAppDefinitionOrThrow(projectDir);
    return await syncChimpbaseModuleArtifacts(app, projectDir, options);
  }

  async function startChimpbaseApp(
    options: CreateChimpbaseFromAppOptions & Pick<StartChimpbaseProjectOptions, "serve" | "runWorker">,
  ): Promise<StartedChimpbaseProject<THost, TServer>> {
    const host = await loadChimpbaseApp(options.app, options);
    return startLoadedHost(host, options.serve, options.runWorker);
  }

  async function runChimpbaseAppAction(
    app: ChimpbaseAppDefinition | ChimpbaseAppDefinitionInput,
    actionName: string,
    args: unknown[] | unknown = [],
    options: LoadChimpbaseAppOptions = {},
  ): Promise<{ host: THost; outcome: ActionExecutionResult }> {
    const host = await loadChimpbaseApp(app, options);

    try {
      const outcome = await host.executeAction(actionName, args);
      return { host, outcome };
    } catch (error) {
      await host.close();
      throw error;
    }
  }

  async function createChimpbaseFromApp(
    options: CreateChimpbaseFromAppOptions,
  ): Promise<THost> {
    const app = defineChimpbaseApp(options.app);
    const projectDir = resolve(options.projectDir ?? ".");
    const storageEngine = inferStorageEngine(runtime.env, options);
    const config = normalizeProjectConfig({
      project: {
        name: app.project.name,
      },
      server: {
        port: options.server?.port ?? inferServerPort(runtime.env),
      },
      storage: {
        engine: storageEngine,
        path: storageEngine === "memory" || storageEngine === "postgres"
          ? null
          : options.storage?.path
            ?? runtime.env.get("CHIMPBASE_STORAGE_PATH")
            ?? join("data", `${app.project.name}.db`),
        url: options.storage?.url
          ?? runtime.env.get("CHIMPBASE_DATABASE_URL")
          ?? runtime.env.get("DATABASE_URL")
          ?? null,
      },
      subscriptions: {
        dispatch: options.subscriptions?.dispatch ?? inferSubscriptionDispatchMode(runtime.env),
        idempotency: {
          retention: options.subscriptions?.idempotency?.retention,
        },
      },
      telemetry: {
        minLevel: app.telemetry.minLevel,
        persist: app.telemetry.persist,
        retention: options.telemetryRetention,
      },
      worker: {
        concurrency: options.workerRuntime?.concurrency ?? inferNumberEnv(runtime.env, "CHIMPBASE_WORKER_CONCURRENCY"),
        leaseMs: options.workerRuntime?.leaseMs ?? inferNumberEnv(runtime.env, "CHIMPBASE_WORKER_LEASE_MS"),
        maxAttempts: app.worker.maxAttempts,
        pollIntervalMs: options.workerRuntime?.pollIntervalMs ?? inferNumberEnv(runtime.env, "CHIMPBASE_WORKER_POLL_INTERVAL_MS"),
        retryDelayMs: app.worker.retryDelayMs,
      },
      workflows: {
        contractsDir: app.workflows.contractsDir ?? undefined,
      },
    });

    return await createRuntimeHost(HostClass, runtime, {
      app,
      blobs: options.blobs,
      config,
      debug: inferDebugEnabled(runtime.env, options.debug),
      migrationsDir: inferMigrationsDir(projectDir, options.migrationsDir ?? runtime.env.get("CHIMPBASE_MIGRATIONS_DIR"), options),
      migrationsSql: options.migrationsSql,
      projectDir,
      secrets: options.secrets,
      sinks: options.sinks,
    } satisfies CreateHostOptions);
  }

  return {
    createChimpbase,
    loadChimpbaseApp,
    loadChimpbaseProject,
    runChimpbaseAction,
    runChimpbaseAppAction,
    startChimpbaseApp,
    startChimpbaseProject,
    syncChimpbaseSchema,
    syncChimpbaseModules,
    syncChimpbaseWorkflowContracts,
  };
}

function normalizeCreateChimpbaseOptions(
  options: CreateChimpbaseOptions,
): CreateChimpbaseFromAppOptions {
  if ("app" in options && options.app !== undefined) {
    return options as CreateChimpbaseFromAppOptions;
  }

  const {
    httpHandler,
    migrations,
    project,
    modules,
    registrations,
    telemetry,
    worker,
    workflows,
    ...runtimeOptions
  } = options as CreateChimpbaseAppFieldsOptions;

  return {
    ...runtimeOptions,
    app: defineChimpbaseApp({
      httpHandler,
      migrations,
      project,
      modules,
      registrations,
      telemetry,
      worker,
      workflows,
    }),
  };
}

async function loadProjectAppDefinitionOrThrow(projectDir: string): Promise<ChimpbaseAppDefinition> {
  const app = await loadProjectAppDefinition(projectDir);
  if (!(app !== null)) {
    throw new Error(`missing chimpbase.app.ts in ${projectDir}`);
  }

  return app;
}

async function startLoadedHost<TServer, THost extends ChimpbaseHost<TServer>>(
  host: THost,
  serveOption?: boolean,
  runWorkerOption?: boolean,
): Promise<StartedChimpbaseProject<THost, TServer>> {
  const started = await host.start({
    runWorker: runWorkerOption,
    serve: serveOption,
  }) as StartedHost<THost, TServer>;

  return {
    host,
    server: started.server,
    async stop() {
      await started.stop();
      await host.close();
    },
  };
}
