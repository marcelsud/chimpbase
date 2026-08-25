import { resolve } from "node:path";

import {
  ChimpbaseEngine,
  createDefaultChimpbasePlatformShim,
  listChimpbaseMigrationsForEngine,
  type ChimpbaseBlobDriver,
  type ChimpbaseBlobSigner,
  type ChimpbaseBlobsEngineConfig,
  type ChimpbaseDrainOptions,
  type ChimpbaseDrainResult,
  type ChimpbaseEventBus,
  type ChimpbaseActionExecutionResult,
  type ChimpbaseCronScheduleExecutionResult,
  type ChimpbaseProjectConfig,
  type ChimpbaseRegistry,
  type ChimpbaseRouteExecutionResult,
  type ChimpbaseSecretsSource,
  type ChimpbaseTelemetryPersistOverride,
  type ChimpbaseTelemetryRecord,
  type ChimpbaseWorkerRegistration,
  type ChimpbasePlatformShim,
  type ChimpbaseAppDefinition,
  type ChimpbaseMigration,
  type ChimpbaseMigrationSource,
  type ChimpbaseMigrationsDefinition,
  type ChimpbaseEngineAdapter,
  createChimpbaseRegistry,
} from "@chimpbase/core";
import {
  action as createActionEntry,
  bindActionInvoker as bindActionReferenceInvoker,
  cron as createCronEntry,
  describeWorkflow,
  register as registerEntries,
  registerFrom as registerEntriesFrom,
  route as createRouteEntry,
  resolveChimpbaseActionRegistrationName,
  subscription as createSubscriptionEntry,
  workflow as createWorkflowEntry,
  worker as createWorkerEntry,
  type ChimpbaseActionHandler,
  type ChimpbaseActionRegistrationLike,
  type ChimpbaseContext,
  type ChimpbaseContextExtensionRegistration,
  type ChimpbaseObjectActionHandler,
  type ChimpbaseCronHandler,
  type ChimpbaseInferActionArgs,
  type ChimpbaseInferActionResult,
  type ChimpbaseRegistrationSource,
  type ChimpbaseRouteEnv,
  type ChimpbaseRouteHandler,
  type ChimpbaseSubscriptionHandler,
  type ChimpbaseSubscriptionOptions,
  type ChimpbaseTelemetryPersistOption,
  type ChimpbaseTupleActionHandler,
  type ChimpbaseValidator,
  type ChimpbaseWorkerDefinition,
  type ChimpbaseWorkerHandler,
  type ChimpbaseWorkflowContract,
  type ChimpbaseTelemetrySink,
  type ChimpbaseWorkflowDefinition,
} from "@chimpbase/runtime";
import { loadProjectMigrations } from "@chimpbase/tooling/migrations";
import { loadLocalSecretStore } from "@chimpbase/tooling/secrets";
import {
  syncRegisteredWorkflowContracts,
  type WorkflowContractSyncOptions,
  type WorkflowContractSyncResult,
} from "@chimpbase/tooling/workflow_contracts";

interface RouteRequestLike {
  headers: Headers;
  method: string;
  url: string;
}

interface WorkerHandle {
  stop(): void | Promise<void>;
}

interface StorageHandle {
  close(): void | Promise<void>;
}

export interface StorageResources {
  createAdapter(): ChimpbaseEngineAdapter;
  eventBus?: ChimpbaseEventBus;
  storage: StorageHandle;
  supportsConcurrentWorkers: boolean;
}

interface WorkerLane {
  engine: ChimpbaseEngine;
  id: number;
  running: boolean;
  serializedOperations: Promise<void>;
}

export interface StartedHost<THost, TServer> {
  host: THost;
  server: TServer | null;
  stop(): Promise<void>;
}

export type TelemetryRecord = ChimpbaseTelemetryRecord;
export type ActionExecutionResult = ChimpbaseActionExecutionResult;
export type CronScheduleExecutionResult = ChimpbaseCronScheduleExecutionResult;
export type RouteExecutionResult = ChimpbaseRouteExecutionResult;
export type DrainOptions = ChimpbaseDrainOptions;
export type DrainResult = ChimpbaseDrainResult;

export interface ChimpbaseRuntimeEnvironment {
  get(name: string): string | undefined;
  toObject(): Record<string, string>;
}

export interface ChimpbaseServerAdapter<TServer> {
  create(
    options: { port: number },
    handler: (request: Request) => Response | Promise<Response>,
  ): TServer;
  stop(server: TServer): Promise<void>;
}

export interface ChimpbaseStorageAdapter {
  open(
    projectDir: string,
    config: ChimpbaseProjectConfig,
    platform: ChimpbasePlatformShim,
    inlineMigrations: readonly ChimpbaseMigration[],
    migrationSource: ChimpbaseMigrationSource,
    migrationsSql: string[],
  ): Promise<StorageResources>;
}

export interface ChimpbaseRuntimeShim<TServer> {
  debugNamespace: string;
  env: ChimpbaseRuntimeEnvironment;
  server: ChimpbaseServerAdapter<TServer>;
  storage: ChimpbaseStorageAdapter;
}

export interface CreateHostOptions {
  app?: ChimpbaseAppDefinition;
  blobs?: {
    driver: ChimpbaseBlobDriver;
    buckets: readonly string[];
    signer?: ChimpbaseBlobSigner;
  };
  config: ChimpbaseProjectConfig;
  debug?: boolean;
  migrations?: ChimpbaseMigrationsDefinition;
  migrationSource?: ChimpbaseMigrationSource;
  migrationsDir?: string | null;
  migrationsSql?: string[];
  platform?: ChimpbasePlatformShim;
  projectDir?: string;
  secrets?: ChimpbaseSecretsSource;
  sinks?: ChimpbaseTelemetrySink[];
}

export interface RuntimeHostInstanceOptions<TServer> {
  config: ChimpbaseProjectConfig;
  createWorkerEngine: () => ChimpbaseEngine;
  debugEnabled: boolean;
  engine: ChimpbaseEngine;
  platform: ChimpbasePlatformShim;
  projectDir: string;
  registry: ChimpbaseRegistry;
  runtime: ChimpbaseRuntimeShim<TServer>;
  storage: StorageHandle;
  supportsConcurrentWorkers: boolean;
}

const IDEMPOTENT_SUBSCRIPTION_MARKER_PREFIX = "_chimpbase.sub.seen:";
const POSTGRES_WORKER_QUEUE_BATCH_SIZE = 8;
const RESERVED_ENGINE_QUEUE_NAMES = new Set([
  "__chimpbase.cron.run",
  "__chimpbase.subscription.run",
  "__chimpbase.workflow.run",
]);

export class ChimpbaseHost<TServer> {
  readonly config: ChimpbaseProjectConfig;
  readonly engine: ChimpbaseEngine;
  readonly platform: ChimpbasePlatformShim;
  readonly projectDir: string;
  readonly registry: ChimpbaseRegistry;
  private cronRegistryDirty = true;
  private cronSyncPromise: Promise<void> | null = null;
  private readonly createWorkerEngine: () => ChimpbaseEngine;
  private readonly debugEnabled: boolean;
  private readonly runtime: ChimpbaseRuntimeShim<TServer>;
  private serializedEngineOperations: Promise<void> = Promise.resolve();
  private readonly storage: StorageHandle;
  private readonly supportsConcurrentWorkers: boolean;

  constructor(options: RuntimeHostInstanceOptions<TServer>) {
    this.projectDir = options.projectDir;
    this.config = options.config;
    this.platform = options.platform;
    this.debugEnabled = options.debugEnabled;
    this.runtime = options.runtime;
    this.storage = options.storage;
    this.engine = options.engine;
    this.registry = options.registry;
    this.createWorkerEngine = options.createWorkerEngine;
    this.supportsConcurrentWorkers = options.supportsConcurrentWorkers;
  }

  async executeAction<TAction extends ChimpbaseActionRegistrationLike>(
    reference: TAction,
    ...args: ChimpbaseInferActionArgs<TAction> extends readonly unknown[]
      ? ChimpbaseInferActionArgs<TAction>
      : [ChimpbaseInferActionArgs<TAction>]
  ): Promise<{ emittedEvents: unknown[]; result: ChimpbaseInferActionResult<TAction> }>;
  async executeAction(name: string, args?: unknown[] | unknown): Promise<ActionExecutionResult>;
  /** Invoke a registration whose argument types are no longer known, such as one relayed by a bound invoker. */
  async executeAction(
    reference: ChimpbaseActionRegistrationLike,
    ...args: unknown[]
  ): Promise<ActionExecutionResult>;
  async executeAction(
    nameOrReference: string | ChimpbaseActionRegistrationLike,
    ...args: unknown[]
  ): Promise<ActionExecutionResult> {
    const actionName = typeof nameOrReference === "string"
      ? nameOrReference
      : resolveChimpbaseActionRegistrationName(nameOrReference);
    this.debug("action executing", { name: actionName });

    try {
      const outcome = typeof nameOrReference === "string"
        ? await this.runEngineOperation(async () => await this.engine.executeAction(
          nameOrReference,
          normalizeActionExecutionArgs(args[0]),
        ))
        : await this.runEngineOperation(async () => await this.engine.executeAction(
          actionName,
          normalizeReferenceInvocationArgs(nameOrReference, args),
        ));

      this.debug("action completed", { emittedEvents: outcome.emittedEvents.length, name: actionName });
      return outcome;
    } catch (error) {
      this.debug("action failed", { error: formatError(error), name: actionName });
      throw error;
    }
  }

  async executeRoute(request: Request): Promise<RouteExecutionResult> {
    const route = getRouteKey(request);
    this.debug("route executing", { route });

    try {
      const outcome = await this.runEngineOperation(async () => await this.engine.executeRoute(request));
      this.debug("route completed", {
        emittedEvents: outcome.emittedEvents.length,
        route,
        status: outcome.response?.status ?? 404,
      });
      return outcome;
    } catch (error) {
      this.debug("route failed", { error: formatError(error), route });
      throw error;
    }
  }

  async processNextCronSchedule(): Promise<CronScheduleExecutionResult | null> {
    const outcome = await this.runEngineOperation(async () => {
      await this.syncCronSchedulesIfNeeded();
      return await this.engine.processNextCronSchedule();
    });
    if (outcome) {
      this.debug("cron schedule processed", {
        fireAtMs: outcome.fireAtMs,
        nextFireAtMs: outcome.nextFireAtMs,
        scheduleName: outcome.scheduleName,
      });
    }

    return outcome;
  }

  async processNextQueueJob(): Promise<Awaited<ReturnType<ChimpbaseEngine["processNextQueueJob"]>>> {
    const outcome = await this.runEngineOperation(async () => await this.engine.processNextQueueJob());
    if (outcome) {
      this.debug("queue job processed", {
        emittedEvents: outcome.emittedEvents.length,
        jobId: outcome.jobId,
        queueName: outcome.queueName,
      });
    }

    return outcome;
  }

  async drain(options: DrainOptions = {}): Promise<DrainResult> {
    const outcome = await this.runEngineOperation(async () => {
      await this.syncCronSchedulesIfNeeded();
      return await this.engine.drain(options);
    });
    this.debug("worker drain completed", {
      cronSchedules: outcome.cronSchedules,
      queueJobs: outcome.queueJobs,
      runs: outcome.runs,
      stopReason: outcome.stopReason,
    });
    return outcome;
  }

  register(...entriesOrGroups: ChimpbaseRegistrationSource[]): this {
    registerEntries(this, ...entriesOrGroups);
    return this;
  }

  registerFrom(...sources: object[]): this {
    registerEntriesFrom(this, ...sources);
    return this;
  }

  action<TArgs extends unknown[] = unknown[], TResult = unknown>(
    name: string,
    handler: ChimpbaseTupleActionHandler<TArgs, TResult>,
    options?: { telemetry?: ChimpbaseTelemetryPersistOption },
  ): this;
  action<TAction extends ChimpbaseActionRegistrationLike>(entry: TAction): this;
  action(
    nameOrEntry: string | ChimpbaseActionRegistrationLike,
    handler?: ChimpbaseTupleActionHandler<unknown[], unknown>,
    options?: { telemetry?: ChimpbaseTelemetryPersistOption },
  ): this {
    return this.register(
      typeof nameOrEntry === "string"
        ? createActionEntry(nameOrEntry, handler as ChimpbaseTupleActionHandler<unknown[], unknown>, options)
        : nameOrEntry,
    );
  }

  subscription<TPayload = unknown, TResult = unknown>(
    eventName: string,
    handler: ChimpbaseSubscriptionHandler<TPayload, TResult>,
    options?: ChimpbaseSubscriptionOptions,
  ): this {
    return this.register(createSubscriptionEntry(eventName, handler, options));
  }

  worker<TPayload = unknown, TResult = unknown>(
    name: string,
    handler: ChimpbaseWorkerHandler<TPayload, TResult>,
    definition?: ChimpbaseWorkerDefinition,
    options?: { telemetry?: ChimpbaseTelemetryPersistOption },
  ): this {
    return this.register(createWorkerEntry(name, handler, definition, options));
  }

  cron<TResult = unknown>(
    name: string,
    schedule: string,
    handler: ChimpbaseCronHandler<TResult>,
    options?: { telemetry?: ChimpbaseTelemetryPersistOption },
  ): this {
    return this.register(createCronEntry(name, schedule, handler, options));
  }

  route(
    name: string,
    handler: ChimpbaseRouteHandler,
  ): this {
    return this.register(createRouteEntry(name, handler));
  }

  workflow<TInput = unknown, TState = unknown>(
    definition: ChimpbaseWorkflowDefinition<TInput, TState>,
  ): this {
    return this.register(createWorkflowEntry(definition));
  }

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
  registerAction(
    name: string,
    handler: ChimpbaseActionHandler<unknown, unknown>,
    definition?: { args?: ChimpbaseValidator<unknown> },
  ): ChimpbaseActionHandler<unknown, unknown> {
    const entry = definition?.args
      ? createActionEntry({
          args: definition.args,
          handler: handler as ChimpbaseObjectActionHandler<unknown, unknown>,
          name,
        })
      : createActionEntry(name, handler as ChimpbaseTupleActionHandler<unknown[], unknown>);

    this.registry.actions.set(name, entry);
    return handler;
  }

  bindActionInvoker(reference: ChimpbaseActionRegistrationLike): void {
    bindActionReferenceInvoker(reference, async <TResult = unknown>(
      nameOrReference: string | ChimpbaseActionRegistrationLike,
      args: unknown[],
    ): Promise<TResult> => {
      if (typeof nameOrReference === "string") {
        const outcome = await this.executeAction(nameOrReference, args);
        return outcome.result as TResult;
      }

      const outcome = await this.executeAction(nameOrReference, ...(args as unknown[]));
      return outcome.result as TResult;
    });
  }

  registerSubscription<TPayload = unknown, TResult = unknown>(
    eventName: string,
    handler: ChimpbaseSubscriptionHandler<TPayload, TResult>,
    options?: ChimpbaseSubscriptionOptions,
  ): ChimpbaseSubscriptionHandler<TPayload, TResult> {
    const subscriptions = this.registry.subscriptions.get(eventName) ?? [];
    subscriptions.push({
      handler: handler as ChimpbaseSubscriptionHandler,
      idempotent: options?.idempotent ?? false,
      name: options?.name ?? "",
    });
    this.registry.subscriptions.set(eventName, subscriptions);
    return handler;
  }

  registerWorker<TPayload = unknown, TResult = unknown>(
    name: string,
    handler: ChimpbaseWorkerHandler<TPayload, TResult>,
    definition?: ChimpbaseWorkerDefinition,
  ): ChimpbaseWorkerHandler<TPayload, TResult> {
    const registration: ChimpbaseWorkerRegistration = {
      definition: {
        dlq: definition?.dlq === undefined ? `${name}.dlq` : definition.dlq,
      },
      handler: handler as ChimpbaseWorkerHandler,
      name,
    };
    this.registry.workers.set(name, registration);
    return handler;
  }

  registerCron<TResult = unknown>(
    name: string,
    schedule: string,
    handler: ChimpbaseCronHandler<TResult>,
  ): ChimpbaseCronHandler<TResult> {
    this.registry.crons.set(name, {
      handler: handler as ChimpbaseCronHandler,
      name,
      schedule,
    });
    this.cronRegistryDirty = true;
    return handler;
  }

  registerRoute(
    name: string,
    handler: ChimpbaseRouteHandler,
  ): ChimpbaseRouteHandler {
    this.registry.routes.push({
      handler,
      kind: "route",
      name,
    });
    return handler;
  }

  registerOnStart(
    name: string,
    handler: (ctx: ChimpbaseContext) => Promise<void> | void,
  ): void {
    this.registry.onStartHooks.push({ handler, name });
  }

  registerOnStop(
    name: string,
    handler: () => Promise<void> | void,
  ): void {
    this.registry.onStopHooks.push({ handler, name });
  }

  registerWorkflow<TInput = unknown, TState = unknown>(
    definition: ChimpbaseWorkflowDefinition<TInput, TState>,
  ): ChimpbaseWorkflowDefinition<TInput, TState> {
    const versions = this.registry.workflows.get(definition.name) ?? new Map();
    versions.set(definition.version, definition as ChimpbaseWorkflowDefinition);
    this.registry.workflows.set(definition.name, versions);
    return definition;
  }

  registerContextExtension(registration: ChimpbaseContextExtensionRegistration): void {
    const existing = this.registry.contextExtensions.findIndex((entry) => entry.key === registration.key);
    if (existing >= 0) {
      this.registry.contextExtensions[existing] = registration;
    } else {
      this.registry.contextExtensions.push(registration);
    }
  }

  setHttpHandler(handler: ChimpbaseRouteHandler | null): void {
    this.registry.httpHandler = handler;
  }

  setTelemetryOverride(key: string, value: ChimpbaseTelemetryPersistOverride): void {
    this.registry.telemetryOverrides.set(key, value);
  }

  routeEnv(): ChimpbaseRouteEnv {
    return this.engine.createRouteEnv();
  }

  listWorkflowContracts(): ChimpbaseWorkflowContract[] {
    return [...this.registry.workflows.entries()]
      .flatMap(([, versions]) => [...versions.values()].map((definition) => describeWorkflow(definition)))
      .sort((left, right) => {
        const byName = left.name.localeCompare(right.name);
        if (byName !== 0) {
          return byName;
        }

        return left.version - right.version;
      });
  }

  async syncWorkflowContracts(
    options: WorkflowContractSyncOptions = {},
  ): Promise<WorkflowContractSyncResult> {
    return await syncRegisteredWorkflowContracts(this.registry, this.projectDir, {
      ...options,
      contractsDir: options.contractsDir ?? this.config.workflows.contractsDir,
    });
  }

  async syncCronSchedules(): Promise<void> {
    this.cronRegistryDirty = true;
    await this.syncCronSchedulesIfNeeded();
  }

  serve(): TServer {
    return this.runtime.server.create(
      { port: this.config.server.port },
      async (request) => {
        if (new URL(request.url).pathname === "/health") {
          return Response.json({ ok: true });
        }

        const outcome = await this.executeRoute(request);
        if (!outcome.response) {
          return new Response("route handler not found", { status: 404 });
        }

        if (outcome.emittedEvents.length > 0) {
          console.log(
            `handled ${request.method} ${new URL(request.url).pathname} with ${outcome.emittedEvents.length} emitted event(s)`,
          );
        }

        return outcome.response;
      },
    );
  }

  async start(options: { runWorker?: boolean; serve?: boolean } = {}): Promise<StartedHost<this, TServer>> {
    const runServe = options.serve ?? !options.runWorker;
    const runWorker = options.runWorker ?? !options.serve;
    const worker = runWorker ? this.startWorker() : null;
    const server = runServe ? this.serve() : null;
    this.debug("runtime starting", {
      workerConcurrency: runWorker ? this.getWorkerConcurrency() : 0,
      port: runServe ? this.config.server.port : null,
      runServe,
      runWorker,
      storage: this.config.storage.engine,
    });
    this.engine.startEventBus();

    for (const hook of this.registry.onStartHooks) {
      await this.engine.executeLifecycleHook(hook.handler);
    }

    return {
      host: this,
      server,
      stop: async () => {
        for (const hook of this.registry.onStopHooks) {
          try {
            await hook.handler();
          } catch (err) {
            console.error(`onStop hook "${hook.name}" failed:`, err);
          }
        }
        if (server) {
          await this.runtime.server.stop(server);
        }
        await worker?.stop();
        this.engine.stopEventBus();
        this.debug("runtime stopped");
      },
    };
  }

  startWorker(): WorkerHandle {
    let stopped = false;
    if (!this.supportsConcurrentWorkers) {
      let running = false;
      const activeTicks = new Set<Promise<void>>();

      const tick = async () => {
        if (running || stopped) {
          return;
        }

        running = true;
        try {
          while (!stopped) {
            const outcome = await this.drain({ maxRuns: 1 });
            if (outcome.idle) {
              break;
            }
          }
        } catch (error) {
          console.error(`[${this.runtime.debugNamespace}][worker]`, error);
        } finally {
          running = false;
        }
      };

      const scheduleTick = () => {
        const runningTick = tick().finally(() => {
          activeTicks.delete(runningTick);
        });
        activeTicks.add(runningTick);
      };

      scheduleTick();
      const interval = setInterval(() => {
        scheduleTick();
      }, this.config.worker.pollIntervalMs);

      return {
        async stop() {
          stopped = true;
          clearInterval(interval);
          await Promise.allSettled([...activeTicks]);
        },
      };
    }

    const lanes = this.createWorkerLanes();
    const activeTicks = new Set<Promise<void>>();
    const tickLane = async (lane: WorkerLane) => {
      if (lane.running || stopped) {
        return;
      }

      lane.running = true;
      try {
        while (!stopped) {
          const outcome = await this.runWorkerLaneOperation(lane, async () => {
            await this.syncCronSchedulesIfNeeded();
            const scheduled = await lane.engine.processNextCronSchedule();
            const queueJobs = await lane.engine.processNextQueueJobs(this.getWorkerQueueBatchSize());
            return {
              cronSchedules: scheduled ? 1 : 0,
              idle: !scheduled && queueJobs.length === 0,
              queueJobs: queueJobs.length,
              runs: (scheduled ? 1 : 0) + queueJobs.length,
              stopReason: !scheduled && queueJobs.length === 0 ? "idle" : "max_runs",
            } satisfies DrainResult;
          });
          this.debug("worker drain completed", {
            cronSchedules: outcome.cronSchedules,
            lane: lane.id,
            queueJobs: outcome.queueJobs,
            runs: outcome.runs,
            stopReason: outcome.stopReason,
          });
          if (outcome.idle) {
            break;
          }
        }
      } catch (error) {
        console.error(`[${this.runtime.debugNamespace}][worker]`, error);
      } finally {
        lane.running = false;
      }
    };

    const scheduleTick = (lane: WorkerLane) => {
      const runningTick = tickLane(lane).finally(() => {
        activeTicks.delete(runningTick);
      });
      activeTicks.add(runningTick);
    };

    for (const lane of lanes) {
      scheduleTick(lane);
    }
    const interval = setInterval(() => {
      for (const lane of lanes) {
        scheduleTick(lane);
      }
    }, this.config.worker.pollIntervalMs);

    return {
      async stop() {
        stopped = true;
        clearInterval(interval);
        await Promise.allSettled([...activeTicks]);
      },
    };
  }

  drainTelemetryRecords(): TelemetryRecord[] {
    return this.engine.drainTelemetryRecords();
  }

  async close(): Promise<void> {
    this.engine.stopEventBus();
    const [sinkResult, storageResult] = await Promise.allSettled([
      Promise.resolve().then(() => this.engine.shutdownSinks()),
      Promise.resolve().then(() => this.storage.close()),
    ]);

    if (sinkResult.status === "rejected" && storageResult.status === "rejected") {
      throw new AggregateError([sinkResult.reason, storageResult.reason], "runtime cleanup failed");
    }
    if (sinkResult.status === "rejected") {
      throw sinkResult.reason;
    }
    if (storageResult.status === "rejected") {
      throw storageResult.reason;
    }
    this.debug("runtime closed");
  }

  private async runEngineOperation<TResult>(operation: () => Promise<TResult>): Promise<TResult> {
    const queued = this.serializedEngineOperations
      .catch(() => undefined)
      .then(operation);

    this.serializedEngineOperations = queued.then(() => undefined, () => undefined);
    return await queued;
  }

  private createWorkerLanes(): WorkerLane[] {
    return Array.from({ length: this.getWorkerConcurrency() }, (_, index) => ({
      engine: this.createWorkerEngine(),
      id: index + 1,
      running: false,
      serializedOperations: Promise.resolve(),
    }));
  }

  private getWorkerConcurrency(): number {
    const configured = Math.max(1, Math.floor(this.config.worker.concurrency));
    if (!this.supportsConcurrentWorkers) {
      return 1;
    }

    return configured;
  }

  private getWorkerQueueBatchSize(): number {
    if (!this.supportsConcurrentWorkers) {
      return 1;
    }

    if (this.getWorkerConcurrency() > 1) {
      return 1;
    }

    return POSTGRES_WORKER_QUEUE_BATCH_SIZE;
  }

  private async runWorkerLaneOperation<TResult>(lane: WorkerLane, operation: () => Promise<TResult>): Promise<TResult> {
    const queued = lane.serializedOperations
      .catch(() => undefined)
      .then(operation);

    lane.serializedOperations = queued.then(() => undefined, () => undefined);
    return await queued;
  }

  private debug(message: string, details?: Record<string, unknown>): void {
    if (!this.debugEnabled) {
      return;
    }

    if (details && Object.keys(details).length > 0) {
      console.debug(`[${this.runtime.debugNamespace}][debug]`, message, details);
      return;
    }

    console.debug(`[${this.runtime.debugNamespace}][debug]`, message);
  }

  private async syncCronSchedulesIfNeeded(): Promise<void> {
    if (!this.cronRegistryDirty) {
      return;
    }

    if (this.cronSyncPromise) {
      await this.cronSyncPromise;
      return;
    }

    this.cronSyncPromise = (async () => {
      while (this.cronRegistryDirty) {
        this.cronRegistryDirty = false;

        try {
          await this.engine.syncRegisteredCrons();
        } catch (error) {
          this.cronRegistryDirty = true;
          throw error;
        }
      }
    })();

    try {
      await this.cronSyncPromise;
    } finally {
      this.cronSyncPromise = null;
    }
  }
}

export async function createRuntimeHost<TServer, THost extends ChimpbaseHost<TServer>>(
  HostClass: new (options: RuntimeHostInstanceOptions<TServer>) => THost,
  runtime: ChimpbaseRuntimeShim<TServer>,
  options: CreateHostOptions,
): Promise<THost> {
  const projectDir = resolve(options.projectDir ?? ".");
  const platform = options.platform ?? createDefaultChimpbasePlatformShim();
  const registry = createChimpbaseRegistry();
  const storageResources = await runtime.storage.open(
    projectDir,
    options.config,
    platform,
    listChimpbaseMigrationsForEngine(options.app?.migrations ?? options.migrations, options.config.storage.engine),
    options.migrationSource
      ?? (options.app
        ? createStaticMigrationSource([])
        : createLocalMigrationSource(projectDir, options.config, options.migrationsDir ?? null)),
    options.migrationsSql ?? [],
  );
  const secrets = options.secrets ?? await loadLocalSecretStore(projectDir, options.config, {
    env: runtime.env.toObject(),
    envFileDefault: runtime.env.get("CHIMPBASE_ENV_FILE") ?? ".env",
    secretsDirDefault: runtime.env.get("CHIMPBASE_SECRETS_DIR") ?? "/run/secrets",
  });

  const blobsEngineConfig: ChimpbaseBlobsEngineConfig | undefined = options.blobs
    ? { driver: options.blobs.driver, buckets: options.blobs.buckets, signer: options.blobs.signer }
    : undefined;
  const createPrimaryEngine = () => new ChimpbaseEngine({
    adapter: storageResources.createAdapter(),
    blobs: blobsEngineConfig,
    eventBus: storageResources.eventBus,
    platform,
    registry,
    secrets,
    sinks: options.sinks,
    subscriptions: {
      dispatch: options.config.subscriptions.dispatch,
    },
    telemetry: {
      minLevel: options.config.telemetry.minLevel,
      persist: options.config.telemetry.persist,
    },
    worker: options.config.worker,
  });
  const createWorkerEngine = () => new ChimpbaseEngine({
    adapter: storageResources.createAdapter(),
    blobs: blobsEngineConfig,
    eventBus: storageResources.eventBus,
    platform,
    registry: cloneRegistryForWorkerEngine(registry),
    secrets,
    sinks: options.sinks,
    subscriptions: {
      dispatch: options.config.subscriptions.dispatch,
    },
    telemetry: {
      minLevel: options.config.telemetry.minLevel,
      persist: options.config.telemetry.persist,
    },
    worker: options.config.worker,
  });
  const engine = createPrimaryEngine();
  const host = new HostClass({
    config: options.config,
    createWorkerEngine,
    debugEnabled: options.debug ?? false,
    engine,
    platform,
    projectDir,
    registry,
    runtime,
    storage: storageResources.storage,
    supportsConcurrentWorkers: storageResources.supportsConcurrentWorkers,
  });

  if (options.app) {
    applyChimpbaseApp(host, options.app);
  }

  registerInternalCleanupCrons(host, options.config, platform);

  return host;
}

export function getRouteKey(request: RouteRequestLike): string {
  return `${request.method.toUpperCase()} ${new URL(request.url).pathname}`;
}

export function inferServerPort(env: ChimpbaseRuntimeEnvironment): number {
  const value = env.get("CHIMPBASE_SERVER_PORT") ?? env.get("PORT");
  const port = value ? Number(value) : NaN;
  return Number.isFinite(port) ? port : 3000;
}

export function inferStorageEngine(
  env: ChimpbaseRuntimeEnvironment,
  options: { storage?: { engine?: "memory" | "postgres" | "sqlite"; url?: string | null } },
): "memory" | "postgres" | "sqlite" {
  if (options.storage?.engine) {
    return options.storage.engine;
  }

  const envEngine = env.get("CHIMPBASE_STORAGE_ENGINE");
  if (envEngine === "memory") {
    return "memory";
  }

  if (envEngine === "postgres" || options.storage?.url || env.get("CHIMPBASE_DATABASE_URL") || env.get("DATABASE_URL")) {
    return "postgres";
  }

  if (envEngine === "sqlite") {
    return envEngine;
  }

  return "sqlite";
}

export function inferNumberEnv(env: ChimpbaseRuntimeEnvironment, name: string): number | undefined {
  const value = env.get(name);
  if (!value) {
    return undefined;
  }

  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : undefined;
}

export function inferSubscriptionDispatchMode(
  env: ChimpbaseRuntimeEnvironment,
): "async" | "sync" | undefined {
  const value = env.get("CHIMPBASE_SUBSCRIPTION_DISPATCH_MODE");
  return value === "async" || value === "sync" ? value : undefined;
}

export function inferDebugEnabled(env: ChimpbaseRuntimeEnvironment, explicit?: boolean): boolean {
  if (explicit !== undefined) {
    return explicit;
  }

  return isTruthyEnv(env.get("CHIMPBASE_DEBUG"));
}

export function inferMigrationsDir(
  projectDir: string,
  configuredDir: string | undefined,
  options: { migrationsSql?: string[] },
): string | null {
  if (configuredDir) {
    return resolve(projectDir, configuredDir);
  }

  if (options.migrationsSql !== undefined) {
    return null;
  }

  return null;
}

function applyChimpbaseApp<TServer>(host: ChimpbaseHost<TServer>, app: ChimpbaseAppDefinition): void {
  if (app.registrations.length > 0) {
    host.register(app.registrations);
  }

  host.setHttpHandler(app.httpHandler);
}

function createLocalMigrationSource(
  projectDir: string,
  config: ChimpbaseProjectConfig,
  migrationsDir: string | null,
): ChimpbaseMigrationSource {
  return {
    async list(): Promise<ChimpbaseMigration[]> {
      return await loadProjectMigrations(projectDir, config.storage.engine, { migrationsDir });
    },
  };
}

function createStaticMigrationSource(migrations: readonly ChimpbaseMigration[]): ChimpbaseMigrationSource {
  return {
    async list(): Promise<ChimpbaseMigration[]> {
      return [...migrations];
    },
  };
}

function cloneRegistryForWorkerEngine(source: ChimpbaseRegistry): ChimpbaseRegistry {
  return {
    actions: new Map(source.actions),
    contextExtensions: [...source.contextExtensions],
    crons: new Map(source.crons),
    httpHandler: source.httpHandler,
    onStartHooks: [],
    onStopHooks: [],
    routes: [...source.routes],
    subscriptions: new Map(
      [...source.subscriptions.entries()].map(([eventName, entries]) => [eventName, [...entries]]),
    ),
    telemetryOverrides: new Map(source.telemetryOverrides),
    workers: new Map(
      [...source.workers.entries()]
        .filter(([name]) => !RESERVED_ENGINE_QUEUE_NAMES.has(name))
        .map(([name, registration]) => [
          name,
          {
            definition: { ...registration.definition },
            handler: registration.handler,
            name: registration.name,
          },
        ]),
    ),
    workflows: new Map(
      [...source.workflows.entries()].map(([name, versions]) => [name, new Map(versions)]),
    ),
  };
}

function registerInternalCleanupCrons<TServer>(
  host: ChimpbaseHost<TServer>,
  config: ChimpbaseProjectConfig,
  platform: ChimpbasePlatformShim,
): void {
  if (config.telemetry.retention.enabled) {
    host.registerCron(
      "__chimpbase.telemetry.cleanup",
      config.telemetry.retention.schedule,
      async (ctx) => {
        const cutoffMs = platform.now() - config.telemetry.retention.maxAgeDays * 86_400_000;
        const cutoffTimestamp = new Date(cutoffMs).toISOString();
        await ctx.db.query(
          `DELETE FROM _chimpbase_stream_events WHERE stream_name IN ('_chimpbase.logs', '_chimpbase.metrics', '_chimpbase.traces') AND created_at < ?1`,
          [cutoffTimestamp],
        );
      },
    );
    host.setTelemetryOverride("cron:__chimpbase.telemetry.cleanup", false);
  }

  if (config.subscriptions.idempotency.retention.enabled) {
    host.registerCron(
      "__chimpbase.subscription.idempotency.cleanup",
      config.subscriptions.idempotency.retention.schedule,
      async (ctx) => {
        const cutoffMs = platform.now() - config.subscriptions.idempotency.retention.maxAgeDays * 86_400_000;
        const keys = await ctx.kv.list({ prefix: IDEMPOTENT_SUBSCRIPTION_MARKER_PREFIX });

        for (const key of keys) {
          const [row] = await ctx.db.query<{ updated_at: unknown }>(
            "SELECT updated_at FROM _chimpbase_kv WHERE key = ?1 LIMIT 1",
            [key],
          );
          const updatedAtMs = parseDatabaseTimestampMs(row?.updated_at);
          if (updatedAtMs !== null && updatedAtMs < cutoffMs) {
            await ctx.kv.delete(key);
          }
        }
      },
    );
    host.setTelemetryOverride("cron:__chimpbase.subscription.idempotency.cleanup", false);
  }

  if (config.kv.retention.enabled) {
    host.registerCron(
      "__chimpbase.kv.cleanup",
      config.kv.retention.schedule,
      async (ctx) => {
        await ctx.db.query(
          "DELETE FROM _chimpbase_kv WHERE expires_at IS NOT NULL AND expires_at <= CURRENT_TIMESTAMP",
        );
      },
    );
    host.setTelemetryOverride("cron:__chimpbase.kv.cleanup", false);
  }
}

function normalizeActionExecutionArgs(args: unknown[] | unknown): unknown[] {
  if (args === undefined) {
    return [];
  }

  return Array.isArray(args) ? args : [args];
}

function formatError(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function normalizeReferenceInvocationArgs(
  reference: ChimpbaseActionRegistrationLike,
  args: unknown[],
): unknown[] {
  if (reference.args) {
    if (args.length > 1) {
      throw new Error(`action ${reference.name} expects a single argument`);
    }

    return args.length === 0 ? [] : [args[0]];
  }

  return args;
}

function parseDatabaseTimestampMs(value: unknown): number | null {
  if (value instanceof Date) {
    const ms = value.getTime();
    return Number.isFinite(ms) ? ms : null;
  }

  if (typeof value !== "string") {
    return null;
  }

  const normalized = value.includes("T") ? value : value.replace(" ", "T");
  const withTimezone = /[zZ]|[+-]\d{2}(?::?\d{2})?$/.test(normalized)
    ? normalized
    : `${normalized}Z`;
  const parsed = Date.parse(withTimezone);
  return Number.isFinite(parsed) ? parsed : null;
}

function isTruthyEnv(value: string | undefined): boolean {
  if (!value) {
    return false;
  }

  return value === "1"
    || value === "true"
    || value === "yes"
    || value === "on";
}
