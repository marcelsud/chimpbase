import type {
  ChimpbaseActionHandler,
  ChimpbaseObjectActionHandler,
  ChimpbaseActionRegistration,
  ChimpbaseContext,
  ChimpbaseActionRegistrationLike,
  ChimpbaseContextExtensionRegistration,
  ChimpbaseTupleActionHandler,
  ChimpbaseValidator,
  ChimpbaseCronHandler,
  ChimpbaseRegistration,
  ChimpbaseRouteHandler,
  ChimpbaseRouteRegistration,
  ChimpbaseSubscriptionHandler,
  ChimpbaseSubscriptionOptions,
  ChimpbaseWorkerDefinition,
  ChimpbaseWorkerHandler,
  ChimpbaseWorkflowDefinition,
} from "@chimpbase/runtime";
import {
  defineChimpbaseMigrations,
  type ChimpbaseMigrationsDefinition,
  type ChimpbaseMigrationsDefinitionInput,
} from "./host.ts";

export type ChimpbaseTelemetryPersistOverride =
  | boolean
  | { log?: boolean; metric?: boolean; trace?: boolean };

export interface ChimpbaseProjectConfig {
  project: {
    name: string;
  };
  server: {
    port: number;
  };
  storage: {
    engine: "memory" | "postgres" | "sqlite";
    path: string | null;
    url: string | null;
  };
  subscriptions: {
    dispatch: "async" | "sync";
    idempotency: {
      retention: { enabled: boolean; maxAgeDays: number; schedule: string };
    };
  };
  telemetry: {
    minLevel: "debug" | "info" | "warn" | "error";
    persist: { log: boolean; metric: boolean; trace: boolean };
    retention: { enabled: boolean; maxAgeDays: number; schedule: string };
  };
  worker: {
    concurrency: number;
    leaseMs: number;
    maxAttempts: number;
    pollIntervalMs: number;
    retryDelayMs: number;
  };
  secrets: {
    dir: string | null;
    envFile: string | null;
  };
  kv: {
    retention: { enabled: boolean; schedule: string };
  };
  workflows: {
    contractsDir: string | null;
  };
}

export interface ChimpbaseProjectConfigInput {
  project?: {
    name?: string;
  };
  server?: {
    port?: number;
  };
  storage?: {
    engine?: "memory" | "postgres" | "sqlite";
    path?: string | null;
    url?: string | null;
  };
  subscriptions?: {
    dispatch?: "async" | "sync";
    idempotency?: {
      retention?: { enabled?: boolean; maxAgeDays?: number; schedule?: string };
    };
  };
  worker?: {
    concurrency?: number;
    leaseMs?: number;
    maxAttempts?: number;
    pollIntervalMs?: number;
    retryDelayMs?: number;
  };
  secrets?: {
    dir?: string | null;
    envFile?: string | null;
  };
  telemetry?: {
    minLevel?: "debug" | "info" | "warn" | "error";
    persist?: { log?: boolean; metric?: boolean; trace?: boolean };
    retention?: { enabled?: boolean; maxAgeDays?: number; schedule?: string };
  };
  kv?: {
    retention?: { enabled?: boolean; schedule?: string };
  };
  workflows?: {
    contractsDir?: string | null;
  };
}

export interface ChimpbaseAppWorkerConfig {
  maxAttempts: number;
  retryDelayMs: number;
}

export interface ChimpbaseAppWorkerConfigInput {
  maxAttempts?: number;
  retryDelayMs?: number;
}

export interface ChimpbaseAppTelemetryConfig {
  minLevel: "debug" | "info" | "warn" | "error";
  persist: { log: boolean; metric: boolean; trace: boolean };
}

export interface ChimpbaseAppTelemetryConfigInput {
  minLevel?: "debug" | "info" | "warn" | "error";
  persist?: { log?: boolean; metric?: boolean; trace?: boolean };
}

export interface ChimpbaseAppWorkflowConfig {
  contractsDir: string | null;
}

export interface ChimpbaseAppWorkflowConfigInput {
  contractsDir?: string | null;
}

export interface ChimpbaseAppDefinition {
  httpHandler: ChimpbaseRouteHandler | null;
  migrations: ChimpbaseMigrationsDefinition;
  project: {
    name: string;
  };
  registrations: readonly ChimpbaseRegistration[];
  telemetry: ChimpbaseAppTelemetryConfig;
  worker: ChimpbaseAppWorkerConfig;
  workflows: ChimpbaseAppWorkflowConfig;
}

export type ChimpbaseAppModule = ChimpbaseAppDefinition;

export interface ChimpbaseAppDefinitionInput {
  httpHandler?: ChimpbaseRouteHandler | { fetch: ChimpbaseRouteHandler } | null;
  migrations?: ChimpbaseMigrationsDefinitionInput;
  project?: {
    name?: string;
  };
  registrations?: ReadonlyArray<ChimpbaseRegistration | readonly ChimpbaseRegistration[]>;
  telemetry?: ChimpbaseAppTelemetryConfigInput;
  worker?: ChimpbaseAppWorkerConfigInput;
  workflows?: ChimpbaseAppWorkflowConfigInput;
}

export type ChimpbaseAppModuleInput = ChimpbaseAppDefinitionInput;

export interface ChimpbaseWorkerRegistration {
  definition: Required<ChimpbaseWorkerDefinition>;
  handler: ChimpbaseWorkerHandler;
  name: string;
}

export interface ChimpbaseCronRegistration {
  handler: ChimpbaseCronHandler;
  name: string;
  schedule: string;
}

export interface ChimpbaseSubscriptionEntry {
  handler: ChimpbaseSubscriptionHandler;
  idempotent: boolean;
  name: string;
}

export interface ChimpbaseRegistry {
  actions: Map<string, ChimpbaseActionRegistrationLike>;
  contextExtensions: ChimpbaseContextExtensionRegistration[];
  crons: Map<string, ChimpbaseCronRegistration>;
  httpHandler: ChimpbaseRouteHandler | null;
  onStartHooks: Array<{ handler: (ctx: ChimpbaseContext) => Promise<void> | void; name: string }>;
  onStopHooks: Array<{ handler: () => Promise<void> | void; name: string }>;
  routes: ChimpbaseRouteRegistration[];
  subscriptions: Map<string, ChimpbaseSubscriptionEntry[]>;
  telemetryOverrides: Map<string, ChimpbaseTelemetryPersistOverride>;
  workers: Map<string, ChimpbaseWorkerRegistration>;
  workflows: Map<string, Map<number, ChimpbaseWorkflowDefinition>>;
}

export interface ChimpbaseEntrypointTarget {
  registerAction<TArgs extends unknown[] = unknown[], TResult = unknown>(
    name: string,
    handler: ChimpbaseTupleActionHandler<TArgs, TResult>,
    definition?: { args?: undefined },
  ): ChimpbaseTupleActionHandler<TArgs, TResult>;
  registerAction<TArgs, TResult = unknown>(
    name: string,
    handler: ChimpbaseObjectActionHandler<TArgs, TResult>,
    definition: { args: ChimpbaseValidator<TArgs> },
  ): ChimpbaseObjectActionHandler<TArgs, TResult>;
  registerSubscription<TPayload = unknown, TResult = unknown>(
    eventName: string,
    handler: ChimpbaseSubscriptionHandler<TPayload, TResult>,
    options?: ChimpbaseSubscriptionOptions,
  ): ChimpbaseSubscriptionHandler<TPayload, TResult>;
  registerWorker<TPayload = unknown, TResult = unknown>(
    name: string,
    handler: ChimpbaseWorkerHandler<TPayload, TResult>,
    definition?: ChimpbaseWorkerDefinition,
  ): ChimpbaseWorkerHandler<TPayload, TResult>;
  registerCron<TResult = unknown>(
    name: string,
    schedule: string,
    handler: ChimpbaseCronHandler<TResult>,
  ): ChimpbaseCronHandler<TResult>;
  registerRoute(name: string, handler: ChimpbaseRouteHandler): ChimpbaseRouteHandler;
  registerWorkflow<TInput = unknown, TState = unknown>(
    definition: ChimpbaseWorkflowDefinition<TInput, TState>,
  ): ChimpbaseWorkflowDefinition<TInput, TState>;
  setHttpHandler(handler: ChimpbaseRouteHandler | null): void;
}

export function normalizeProjectConfig(
  input: ChimpbaseProjectConfigInput = {},
): ChimpbaseProjectConfig {
  return {
    project: {
      name: input.project?.name ?? "chimpbase-app",
    },
    server: {
      port: input.server?.port ?? 3000,
    },
    storage: {
      engine: input.storage?.engine ?? "sqlite",
      path: input.storage?.path ?? null,
      url: input.storage?.url ?? null,
    },
    subscriptions: {
      dispatch: input.subscriptions?.dispatch ?? "sync",
      idempotency: {
        retention: {
          enabled: input.subscriptions?.idempotency?.retention?.enabled ?? false,
          maxAgeDays: input.subscriptions?.idempotency?.retention?.maxAgeDays ?? 30,
          schedule: input.subscriptions?.idempotency?.retention?.schedule ?? "0 2 * * *",
        },
      },
    },
    worker: {
      concurrency: input.worker?.concurrency ?? 1,
      leaseMs: input.worker?.leaseMs ?? 30_000,
      maxAttempts: input.worker?.maxAttempts ?? 5,
      pollIntervalMs: input.worker?.pollIntervalMs ?? 250,
      retryDelayMs: input.worker?.retryDelayMs ?? 1_000,
    },
    secrets: {
      dir: input.secrets?.dir ?? null,
      envFile: input.secrets?.envFile ?? null,
    },
    telemetry: {
      minLevel: input.telemetry?.minLevel ?? "debug",
      persist: {
        log: input.telemetry?.persist?.log ?? false,
        metric: input.telemetry?.persist?.metric ?? false,
        trace: input.telemetry?.persist?.trace ?? false,
      },
      retention: {
        enabled: input.telemetry?.retention?.enabled ?? false,
        maxAgeDays: input.telemetry?.retention?.maxAgeDays ?? 30,
        schedule: input.telemetry?.retention?.schedule ?? "0 2 * * *",
      },
    },
    kv: {
      retention: {
        enabled: input.kv?.retention?.enabled ?? false,
        schedule: input.kv?.retention?.schedule ?? "0 3 * * *",
      },
    },
    workflows: {
      contractsDir: input.workflows?.contractsDir ?? "workflow-contracts",
    },
  };
}

export function defineChimpbaseApp(
  input: ChimpbaseAppDefinitionInput,
): ChimpbaseAppDefinition {
  return {
    httpHandler: normalizeHttpHandler(input.httpHandler),
    migrations: defineChimpbaseMigrations(input.migrations),
    project: {
      name: input.project?.name ?? "chimpbase-app",
    },
    registrations: normalizeRegistrations(input.registrations),
    telemetry: {
      minLevel: input.telemetry?.minLevel ?? "debug",
      persist: {
        log: input.telemetry?.persist?.log ?? false,
        metric: input.telemetry?.persist?.metric ?? false,
        trace: input.telemetry?.persist?.trace ?? false,
      },
    },
    worker: {
      maxAttempts: input.worker?.maxAttempts ?? 5,
      retryDelayMs: input.worker?.retryDelayMs ?? 1_000,
    },
    workflows: {
      contractsDir: input.workflows?.contractsDir ?? "workflow-contracts",
    },
  };
}

export function createChimpbaseRegistry(): ChimpbaseRegistry {
  return {
    actions: new Map(),
    contextExtensions: [],
    crons: new Map(),
    httpHandler: null,
    onStartHooks: [],
    onStopHooks: [],
    routes: [],
    subscriptions: new Map(),
    telemetryOverrides: new Map(),
    workers: new Map(),
    workflows: new Map(),
  };
}

export {
  ChimpbaseEngine,
  ChimpbaseNotModifiedError,
  ChimpbasePreconditionFailedError,
  type ChimpbaseActionExecutionResult,
  type ChimpbaseBlobDriver,
  type ChimpbaseBlobDriverGetResult,
  type ChimpbaseBlobDriverPutResult,
  type ChimpbaseBlobDriverRange,
  type ChimpbaseBlobListMetaResult,
  type ChimpbaseBlobMetaRow,
  type ChimpbaseBlobPartRow,
  type ChimpbaseBlobSigner,
  type ChimpbaseBlobUploadListMetaResult,
  type ChimpbaseBlobUploadRow,
  type ChimpbaseBlobsEngineConfig,
  type ChimpbaseCronScheduleExecutionResult,
  type ChimpbaseEngineAdapter,
  type ChimpbaseEngineOptions,
  type ChimpbaseEventRecord,
  type ChimpbaseExecutionScope,
  type ChimpbaseQueueExecutionResult,
  type ChimpbaseQueueJobRecord,
  type ChimpbaseRouteExecutionResult,
  type ChimpbaseTelemetryRecord,
} from "./engine.ts";

export {
  NoopEventBus,
  type ChimpbaseEventBus,
  type ChimpbaseEventBusCallback,
} from "./event-bus.ts";

export {
  createDefaultChimpbasePlatformShim,
  type ChimpbaseDrainOptions,
  type ChimpbaseDrainResult,
  type ChimpbaseMigration,
  defineChimpbaseMigration,
  type ChimpbaseMigrationSource,
  defineChimpbaseMigrations,
  listChimpbaseMigrationsForEngine,
  type ChimpbaseMigrationsDefinition,
  type ChimpbaseMigrationsDefinitionInput,
  type ChimpbaseMigrationEngine,
  type ChimpbasePlatformShim,
  type ChimpbaseSecretsSource,
  type ChimpbaseStorageEngine,
} from "./host.ts";

function normalizeRegistrations(
  registrations: ReadonlyArray<ChimpbaseRegistration | readonly ChimpbaseRegistration[]> | undefined,
): readonly ChimpbaseRegistration[] {
  return (registrations ?? []).flatMap((entryOrGroup) => Array.isArray(entryOrGroup) ? entryOrGroup : [entryOrGroup]);
}

function normalizeHttpHandler(
  input: ChimpbaseRouteHandler | { fetch: ChimpbaseRouteHandler } | null | undefined,
): ChimpbaseRouteHandler | null {
  if (!(input !== null && input !== undefined)) {
    return null;
  }

  if (typeof input === "function") {
    return input;
  }

  return input.fetch.bind(input);
}
