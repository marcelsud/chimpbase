import type { Kysely } from "kysely";

import type {
  ChimpbaseActionInvoker,
  ChimpbaseActionRegistration,
  ChimpbaseActionRegistrationLike,
  ChimpbaseBlobCopyOptions,
  ChimpbaseBlobCreateUploadOptions,
  ChimpbaseBlobDeleteManyResult,
  ChimpbaseBlobGetOptions,
  ChimpbaseBlobGetResult,
  ChimpbaseBlobListOptions,
  ChimpbaseBlobListResult,
  ChimpbaseBlobMetadata,
  ChimpbaseBlobPutOptions,
  ChimpbaseBlobPutResult,
  ChimpbaseBlobSignOptions,
  ChimpbaseBlobUpload,
  ChimpbaseBlobUploadListOptions,
  ChimpbaseBlobUploadListResult,
  ChimpbaseBlobsClient,
  ChimpbaseCollectionFilter,
  ChimpbaseCollectionFindOptions,
  ChimpbaseCollectionPatch,
  ChimpbaseContext,
  ChimpbaseCronInvocation,
  ChimpbaseDlqEnvelope,
  ChimpbaseKvListOptions,
  ChimpbaseLogger,
  ChimpbaseQueueEnqueueOptions,
  ChimpbaseRouteEnv,
  ChimpbaseStreamEvent,
  ChimpbaseStreamReadOptions,
  ChimpbaseTelemetryAttributes,
  ChimpbaseTraceSpan,
  ChimpbaseWorkflowDefinition,
  ChimpbaseWorkflowRunContext,
  ChimpbaseWorkflowRunResult,
  ChimpbaseWorkflowRuntimeState,
  ChimpbaseWorkflowWaitForSignalDirective,
  ChimpbaseWorkflowInstance,
  ChimpbaseWorkflowRegistration,
  ChimpbaseWorkflowStartOptions,
  ChimpbaseWorkflowStartResult,
  ChimpbaseWorkflowStepsDraftDefinition,
  ChimpbaseWorkflowStepDefinition,
  ChimpbaseTelemetrySink,
  ChimpbaseSinkSpan,
} from "@chimpbase/runtime";
import { resolveChimpbaseActionRegistrationName, runWithActionInvoker } from "@chimpbase/runtime";

import { computeNextCronFireTime } from "./cron.ts";
import { NoopEventBus, type ChimpbaseEventBus } from "./event-bus.ts";
import {
  createDefaultChimpbasePlatformShim,
  type ChimpbaseDrainOptions,
  type ChimpbaseDrainResult,
  type ChimpbasePlatformShim,
  type ChimpbaseSecretsSource,
} from "./host.ts";
import type { ChimpbaseRegistry, ChimpbaseTelemetryPersistOverride } from "./index.ts";

export interface ChimpbaseExecutionScope {
  kind: "action" | "cron" | "queue" | "subscription";
  name: string;
}

export interface ChimpbaseEventRecord {
  id?: number;
  name: string;
  payload: unknown;
  payloadJson: string;
}

export interface ChimpbaseQueueJobRecord {
  attempt_count: number;
  id: number;
  payload_json: string;
  queue_name: string;
}

export type ChimpbaseTelemetryRecord =
  | {
      attributes: ChimpbaseTelemetryAttributes;
      kind: "log";
      level: "debug" | "info" | "warn" | "error";
      message: string;
      scope: ChimpbaseExecutionScope;
      timestamp: string;
    }
  | {
      kind: "metric";
      labels: ChimpbaseTelemetryAttributes;
      name: string;
      scope: ChimpbaseExecutionScope;
      timestamp: string;
      value: number;
    }
  | {
      attributes: ChimpbaseTelemetryAttributes;
      kind: "trace";
      name: string;
      phase: "start" | "end";
      scope: ChimpbaseExecutionScope;
      status?: "error" | "ok";
      timestamp: string;
    };

export interface ChimpbaseActionExecutionResult {
  emittedEvents: ChimpbaseEventRecord[];
  result: unknown;
}

export interface ChimpbaseQueueExecutionResult {
  emittedEvents: ChimpbaseEventRecord[];
  jobId: number;
  queueName: string;
}

export interface ChimpbaseRouteExecutionResult {
  emittedEvents: ChimpbaseEventRecord[];
  response: Response | null;
}

export interface ChimpbaseCronScheduleExecutionResult {
  fireAt: string;
  fireAtMs: number;
  nextFireAt: string;
  nextFireAtMs: number;
  scheduleName: string;
}

type ChimpbaseWorkflowStatus =
  | "completed"
  | "failed"
  | "running"
  | "sleeping"
  | "waiting_signal";

interface PersistedWorkflowInstanceRow {
  current_step_id: string | null;
  current_step_index: number;
  input_json: string;
  last_error: string | null;
  status: ChimpbaseWorkflowStatus;
  state_json: string;
  wake_at_ms: number | null;
  workflow_id: string;
  workflow_name: string;
  workflow_version: number;
}

interface PersistedWorkflowSignalRow {
  id: number;
  payload_json: string;
}

interface PersistedCronScheduleRow {
  cron_expression: string;
  next_fire_at_ms: number;
  schedule_name: string;
}

interface ClaimedCronScheduleRow extends PersistedCronScheduleRow {
  lease_token: string;
}

interface WorkflowQueuePayload {
  workflowId: string;
}

interface CronQueuePayload {
  fireAtMs: number;
  scheduleName: string;
}

interface SubscriptionQueuePayload {
  eventId?: number;
  eventName: string;
  payload: unknown;
  payloadJson: string;
}

type WorkflowRunDirective = ChimpbaseWorkflowRunResult<unknown, unknown>;

const INTERNAL_CRON_QUEUE_NAME = "__chimpbase.cron.run";
const INTERNAL_SUBSCRIPTION_QUEUE_NAME = "__chimpbase.subscription.run";
const INTERNAL_WORKFLOW_QUEUE_NAME = "__chimpbase.workflow.run";

const TELEMETRY_LOG_STREAM = "_chimpbase.logs";
const TELEMETRY_METRIC_STREAM = "_chimpbase.metrics";
const TELEMETRY_TRACE_STREAM = "_chimpbase.traces";
const LOG_LEVEL_ORDER: Record<string, number> = { debug: 0, info: 1, warn: 2, error: 3 };

export interface ChimpbaseEngineAdapter {
  advanceCronSchedule(
    scheduleName: string,
    fireAtMs: number,
    nextFireAtMs: number,
    leaseToken: string,
  ): Promise<void>;
  beginTransaction(): Promise<void>;
  claimNextCronSchedule(leaseMs: number): Promise<ClaimedCronScheduleRow | null>;
  claimNextQueueJob(leaseMs: number, queueNames: readonly string[]): Promise<ChimpbaseQueueJobRecord | null>;
  claimNextQueueJobs?(
    leaseMs: number,
    limit: number,
    queueNames: readonly string[],
  ): Promise<ChimpbaseQueueJobRecord[]>;
  collectionDelete(name: string, filter?: ChimpbaseCollectionFilter): Promise<number>;
  collectionFind<TDocument = Record<string, unknown>>(
    name: string,
    filter?: ChimpbaseCollectionFilter,
    options?: ChimpbaseCollectionFindOptions,
  ): Promise<TDocument[]>;
  collectionFindOne<TDocument = Record<string, unknown>>(
    name: string,
    filter: ChimpbaseCollectionFilter,
  ): Promise<TDocument | null>;
  collectionInsert<TDocument extends Record<string, unknown>>(
    name: string,
    document: TDocument,
  ): Promise<string>;
  collectionList(): Promise<string[]>;
  collectionUpdate(
    name: string,
    filter: ChimpbaseCollectionFilter,
    patch: ChimpbaseCollectionPatch,
  ): Promise<number>;
  commitTransaction(events: ChimpbaseEventRecord[]): Promise<void>;
  completeQueueJob(jobId: number): Promise<void>;
  deleteCronSchedule(scheduleName: string): Promise<void>;
  getQueueJobPayload(jobId: number): Promise<string | null>;
  insertCronRun(scheduleName: string, fireAtMs: number): Promise<boolean>;
  kvDelete(key: string): Promise<void>;
  kvGet<TValue = unknown>(key: string): Promise<TValue | null>;
  kvList(options?: ChimpbaseKvListOptions): Promise<string[]>;
  kvSet<TValue = unknown>(key: string, value: TValue, ttlMs?: number): Promise<void>;
  listCronSchedules(): Promise<PersistedCronScheduleRow[]>;
  markQueueJobFailure(
    jobId: number,
    status: "dlq" | "failed" | "pending",
    nextAvailableAtMs: number,
    errorMessage: string,
  ): Promise<void>;
  createKysely<TDatabase = Record<string, never>>(): Kysely<TDatabase>;
  query<T = Record<string, unknown>>(sql: string, params?: readonly unknown[]): Promise<T[]>;
  queueEnqueue<TPayload = unknown>(
    name: string,
    payload: TPayload,
    options?: ChimpbaseQueueEnqueueOptions,
  ): Promise<void>;
  releaseCronScheduleLease(scheduleName: string, leaseToken: string): Promise<void>;
  rollbackTransaction(): Promise<void>;
  streamAppend<TPayload = unknown>(stream: string, event: string, payload: TPayload): Promise<number>;
  streamRead<TPayload = unknown>(
    stream: string,
    options?: ChimpbaseStreamReadOptions,
  ): Promise<ChimpbaseStreamEvent<TPayload>[]>;
  upsertCronSchedule(
    scheduleName: string,
    cronExpression: string,
    nextFireAtMs: number,
  ): Promise<void>;
  blobPutMetadata(row: ChimpbaseBlobMetaRow): Promise<void>;
  blobGetMetadata(bucket: string, key: string): Promise<ChimpbaseBlobMetaRow | null>;
  blobDeleteMetadata(bucket: string, key: string): Promise<boolean>;
  blobListMetadata(bucket: string, options: ChimpbaseBlobListOptions): Promise<ChimpbaseBlobListMetaResult>;
  blobInitUpload(row: ChimpbaseBlobUploadRow): Promise<void>;
  blobGetUpload(uploadId: string): Promise<ChimpbaseBlobUploadRow | null>;
  blobRecordPart(row: ChimpbaseBlobPartRow): Promise<void>;
  blobListParts(uploadId: string): Promise<ChimpbaseBlobPartRow[]>;
  blobFinalizeUpload(uploadId: string, finalMeta: ChimpbaseBlobMetaRow): Promise<void>;
  blobAbortUpload(uploadId: string): Promise<void>;
  blobListUploads(bucket: string, options: ChimpbaseBlobUploadListOptions): Promise<ChimpbaseBlobUploadListMetaResult>;
  blobGcExpiredUploads(nowMs: number): Promise<string[]>;
}

export interface ChimpbaseBlobMetaRow {
  bucket: string;
  key: string;
  size: number;
  etag: string;
  contentType: string;
  metadata: Record<string, string>;
  driverRef: string;
  createdAt: string;
  updatedAt: string;
}

export interface ChimpbaseBlobUploadRow {
  uploadId: string;
  bucket: string;
  key: string;
  contentType: string | null;
  metadata: Record<string, string>;
  driverRef: string;
  createdAtMs: number;
  expiresAtMs: number;
}

export interface ChimpbaseBlobPartRow {
  uploadId: string;
  partNumber: number;
  size: number;
  etag: string;
  driverRef: string;
  createdAt: string;
}

export interface ChimpbaseBlobListMetaResult {
  entries: ChimpbaseBlobMetaRow[];
  commonPrefixes: string[];
  nextCursor: string | null;
}

export interface ChimpbaseBlobUploadListMetaResult {
  uploads: ChimpbaseBlobUploadRow[];
  nextCursor: string | null;
}

export interface ChimpbaseBlobDriverPutResult {
  driverRef: string;
  size: number;
  sha256: string;
}

export interface ChimpbaseBlobDriverGetResult {
  body: ReadableStream<Uint8Array>;
  size: number;
}

export interface ChimpbaseBlobDriverRange {
  start: number;
  end?: number;
}

export interface ChimpbaseBlobDriver {
  ensureBucket(bucket: string): Promise<void>;
  put(
    bucket: string,
    key: string,
    body: ReadableStream<Uint8Array>,
    hint?: { sizeHint?: number },
  ): Promise<ChimpbaseBlobDriverPutResult>;
  get(
    bucket: string,
    key: string,
    driverRef: string,
    range?: ChimpbaseBlobDriverRange,
  ): Promise<ChimpbaseBlobDriverGetResult | null>;
  delete(bucket: string, key: string, driverRef: string): Promise<void>;
  copy(
    src: { bucket: string; key: string; driverRef: string },
    dst: { bucket: string; key: string },
  ): Promise<ChimpbaseBlobDriverPutResult>;
  putPart(
    uploadId: string,
    partNumber: number,
    body: ReadableStream<Uint8Array>,
  ): Promise<ChimpbaseBlobDriverPutResult>;
  assemble(
    uploadId: string,
    parts: readonly { partNumber: number; driverRef: string }[],
    finalBucket: string,
    finalKey: string,
  ): Promise<ChimpbaseBlobDriverPutResult>;
  abortUpload(uploadId: string): Promise<void>;
}

export interface ChimpbaseBlobsEngineConfig {
  driver: ChimpbaseBlobDriver;
  buckets: readonly string[];
  signer?: ChimpbaseBlobSigner;
}

export interface ChimpbaseBlobSigner {
  sign(options: ChimpbaseBlobSignOptions): string;
}

export interface ChimpbaseEngineOptions {
  adapter: ChimpbaseEngineAdapter;
  blobs?: ChimpbaseBlobsEngineConfig;
  eventBus?: ChimpbaseEventBus;
  platform?: ChimpbasePlatformShim;
  registry: ChimpbaseRegistry;
  secrets: ChimpbaseSecretsSource;
  sinks?: ChimpbaseTelemetrySink[];
  subscriptions: {
    dispatch: "async" | "sync";
  };
  telemetry: {
    minLevel: "debug" | "info" | "warn" | "error";
    persist: { log: boolean; metric: boolean; trace: boolean };
  };
  worker: {
    leaseMs: number;
    maxAttempts: number;
    retryDelayMs: number;
  };
}

export class ChimpbaseEngine {
  private readonly adapter: ChimpbaseEngineAdapter;
  private readonly blobsConfig: ChimpbaseBlobsEngineConfig | null;
  private blobsBucketsReady = false;
  private readonly committedEvents: ChimpbaseEventRecord[] = [];
  private readonly eventBus: ChimpbaseEventBus;
  private readonly pendingEvents: ChimpbaseEventRecord[] = [];
  private readonly platform: ChimpbasePlatformShim;
  private readonly registry: ChimpbaseRegistry;
  private readonly secrets: ChimpbaseEngineOptions["secrets"];
  private readonly sinks: ChimpbaseTelemetrySink[];
  private readonly subscriptionsConfig: ChimpbaseEngineOptions["subscriptions"];
  private readonly telemetryConfig: ChimpbaseEngineOptions["telemetry"];
  private readonly telemetryRecords: ChimpbaseTelemetryRecord[] = [];
  private transactionDepth = 0;
  private readonly worker: ChimpbaseEngineOptions["worker"];

  constructor(options: ChimpbaseEngineOptions) {
    this.adapter = options.adapter;
    this.blobsConfig = options.blobs ?? null;
    this.eventBus = options.eventBus ?? new NoopEventBus();
    this.platform = options.platform ?? createDefaultChimpbasePlatformShim();
    this.registry = options.registry;
    this.secrets = options.secrets;
    this.sinks = options.sinks ?? [];
    this.subscriptionsConfig = options.subscriptions;
    this.telemetryConfig = options.telemetry;
    this.worker = options.worker;

    if (this.registry.workers.has(INTERNAL_CRON_QUEUE_NAME)) {
      throw new Error(`reserved queue name already registered: ${INTERNAL_CRON_QUEUE_NAME}`);
    }

    this.registry.workers.set(INTERNAL_CRON_QUEUE_NAME, {
      definition: { dlq: false },
      handler: async (_ctx, payload) => {
        const message = payload as CronQueuePayload;
        await this.processCronQueuePayload(message);
      },
      name: INTERNAL_CRON_QUEUE_NAME,
    });

    if (this.registry.workers.has(INTERNAL_SUBSCRIPTION_QUEUE_NAME)) {
      throw new Error(`reserved queue name already registered: ${INTERNAL_SUBSCRIPTION_QUEUE_NAME}`);
    }

    this.registry.workers.set(INTERNAL_SUBSCRIPTION_QUEUE_NAME, {
      definition: { dlq: false },
      handler: async (_ctx, payload) => {
        const message = payload as SubscriptionQueuePayload;
        await this.dispatchSubscriptions([
          {
            id: message.eventId,
            name: message.eventName,
            payload: message.payload,
            payloadJson: message.payloadJson,
          },
        ]);
      },
      name: INTERNAL_SUBSCRIPTION_QUEUE_NAME,
    });

    if (this.registry.workers.has(INTERNAL_WORKFLOW_QUEUE_NAME)) {
      throw new Error(`reserved queue name already registered: ${INTERNAL_WORKFLOW_QUEUE_NAME}`);
    }

    this.registry.workers.set(INTERNAL_WORKFLOW_QUEUE_NAME, {
      definition: { dlq: false },
      handler: async (_ctx, payload) => {
        const message = payload as WorkflowQueuePayload;
        await this.processWorkflowQueuePayload(message);
      },
      name: INTERNAL_WORKFLOW_QUEUE_NAME,
    });
  }

  startEventBus(): void {
    this.eventBus.start(async (events, ack) => {
      if (this.subscriptionsConfig.dispatch === "async") {
        await this.enqueueSubscriptionDispatchJobs(events);
      } else {
        await this.dispatchSubscriptions(events);
      }
      await ack?.();
    });
  }

  stopEventBus(): void {
    this.eventBus.stop();
  }

  async executeAction(name: string, args: unknown[] = []): Promise<ChimpbaseActionExecutionResult> {
    const telemetryStart = this.telemetryRecords.length;
    const scope: ChimpbaseExecutionScope = { kind: "action", name };
    const handlerSpans = this.sinks.map((sink) => sink.startHandlerSpan(scope));

    try {
      let invoke: () => Promise<unknown> = () => this.invokeActionByName(name, args);
      for (const span of handlerSpans) {
        if (span.runInContext) {
          const prev = invoke;
          invoke = () => span.runInContext!(prev) as Promise<unknown>;
        }
      }

      const result = await invoke();
      const emittedEvents = this.takeCommittedEvents();
      const allEmittedEvents = await this.handleCommittedEvents(emittedEvents, scope, telemetryStart);

      for (const span of handlerSpans) span.end("ok");

      return {
        emittedEvents: [...emittedEvents, ...allEmittedEvents],
        result,
      };
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      for (const span of handlerSpans) span.end("error", message);
      throw error;
    }
  }

  async executeRoute(request: Request): Promise<ChimpbaseRouteExecutionResult> {
    const telemetryStart = this.telemetryRecords.length;
    const url = new URL(request.url, "http://localhost");
    const scope: ChimpbaseExecutionScope = { kind: "action", name: `route:${request.method} ${url.pathname}` };
    const handlerSpans = this.sinks.map((sink) => sink.startHandlerSpan(scope));

    try {
      const routeEnv = this.createRouteEnv();
      let invoke = async () => await this.runWithActionInvoker(async () => {
        for (const route of this.registry.routes) {
          const matched = await route.handler(request, routeEnv);
          if (matched !== null && matched !== undefined) {
            return matched;
          }
        }

        if (!this.registry.httpHandler) {
          return null;
        }

        return await this.registry.httpHandler(request, routeEnv);
      });

      for (const span of handlerSpans) {
        if (span.runInContext) {
          const prev = invoke;
          invoke = () => span.runInContext!(prev) as Promise<Response | null>;
        }
      }

      const response = await invoke();
      const emittedEvents = this.takeCommittedEvents();
      const allEmittedEvents = await this.handleCommittedEvents(emittedEvents, undefined, telemetryStart);

      for (const span of handlerSpans) span.end("ok");

      return {
        emittedEvents: [...emittedEvents, ...allEmittedEvents],
        response,
      };
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      for (const span of handlerSpans) span.end("error", message);
      throw error;
    }
  }

  async processNextCronSchedule(): Promise<ChimpbaseCronScheduleExecutionResult | null> {
    const claimed = await this.adapter.claimNextCronSchedule(this.worker.leaseMs);
    if (!claimed) {
      return null;
    }

    const registration = this.registry.crons.get(claimed.schedule_name);
    if (!registration) {
      await this.adapter.releaseCronScheduleLease(claimed.schedule_name, claimed.lease_token);
      await this.adapter.deleteCronSchedule(claimed.schedule_name);
      return null;
    }

    const now = this.platform.now();
    const { fireAtMs, nextFireAtMs } = this.resolveNextCronExecutionWindow(
      registration.schedule,
      claimed.next_fire_at_ms,
      now,
    );

    try {
      await this.runInTransaction(async () => {
        const inserted = await this.adapter.insertCronRun(claimed.schedule_name, fireAtMs);
        if (inserted) {
          await this.adapter.queueEnqueue(INTERNAL_CRON_QUEUE_NAME, {
            fireAtMs,
            scheduleName: claimed.schedule_name,
          } satisfies CronQueuePayload);
        }

        await this.adapter.advanceCronSchedule(
          claimed.schedule_name,
          claimed.next_fire_at_ms,
          nextFireAtMs,
          claimed.lease_token,
        );
      });
    } catch (error) {
      await this.adapter.releaseCronScheduleLease(claimed.schedule_name, claimed.lease_token);
      throw error;
    }

    return {
      fireAt: this.toTimestampIsoString(fireAtMs),
      fireAtMs,
      nextFireAt: this.toTimestampIsoString(nextFireAtMs),
      nextFireAtMs,
      scheduleName: claimed.schedule_name,
    };
  }

  private resolveNextCronExecutionWindow(
    schedule: string,
    persistedFireAtMs: number,
    now: number,
  ): { fireAtMs: number; nextFireAtMs: number } {
    let fireAtMs = persistedFireAtMs;
    let nextFireAtMs = computeNextCronFireTime(schedule, fireAtMs);

    while (now >= nextFireAtMs) {
      fireAtMs = nextFireAtMs;
      nextFireAtMs = computeNextCronFireTime(schedule, fireAtMs);
    }

    return { fireAtMs, nextFireAtMs };
  }

  async syncRegisteredCrons(): Promise<void> {
    const persisted = await this.adapter.listCronSchedules();
    const persistedByName = new Map(persisted.map((row) => [row.schedule_name, row]));
    const now = this.platform.now();

    for (const [name, registration] of this.registry.crons) {
      const existing = persistedByName.get(name);
      if (existing && existing.cron_expression === registration.schedule) {
        persistedByName.delete(name);
        continue;
      }

      const nextFireAtMs = computeNextCronFireTime(registration.schedule, now);
      await this.adapter.upsertCronSchedule(name, registration.schedule, nextFireAtMs);
      persistedByName.delete(name);
    }

    for (const staleName of persistedByName.keys()) {
      await this.adapter.deleteCronSchedule(staleName);
    }
  }

  async processNextQueueJob(): Promise<ChimpbaseQueueExecutionResult | null> {
    const [job] = await this.processNextQueueJobs(1);
    return job ?? null;
  }

  async processNextQueueJobs(limit: number): Promise<ChimpbaseQueueExecutionResult[]> {
    const batchSize = Math.max(1, Math.floor(limit));
    const queueNames = this.getClaimableQueueNames();
    const jobs = this.adapter.claimNextQueueJobs
      ? await this.adapter.claimNextQueueJobs(this.worker.leaseMs, batchSize, queueNames)
      : [];
    const claimedJobs = jobs.length > 0
      ? jobs
      : await this.claimSingleQueueJob(queueNames);

    const results: ChimpbaseQueueExecutionResult[] = [];
    for (const job of claimedJobs) {
      results.push(await this.processClaimedQueueJob(job));
    }

    return results;
  }

  private async claimSingleQueueJob(queueNames: readonly string[]): Promise<ChimpbaseQueueJobRecord[]> {
    const job = await this.adapter.claimNextQueueJob(this.worker.leaseMs, queueNames);
    return job ? [job] : [];
  }

  private getClaimableQueueNames(): string[] {
    const queueNames = [...this.registry.workers.keys()].filter((name) =>
      name !== INTERNAL_CRON_QUEUE_NAME
      && name !== INTERNAL_SUBSCRIPTION_QUEUE_NAME
      && name !== INTERNAL_WORKFLOW_QUEUE_NAME
    );

    if (this.registry.crons.size > 0) {
      queueNames.push(INTERNAL_CRON_QUEUE_NAME);
    }

    if (this.subscriptionsConfig.dispatch === "async" && this.registry.subscriptions.size > 0) {
      queueNames.push(INTERNAL_SUBSCRIPTION_QUEUE_NAME);
    }

    if (this.registry.workflows.size > 0) {
      queueNames.push(INTERNAL_WORKFLOW_QUEUE_NAME);
    }

    return queueNames;
  }

  private async processClaimedQueueJob(job: ChimpbaseQueueJobRecord): Promise<ChimpbaseQueueExecutionResult> {
    const telemetryStart = this.telemetryRecords.length;
    const worker = this.registry.workers.get(job.queue_name);
    if (!worker) {
      await this.failQueueJob(job.id, job.queue_name, `queue handler not found: ${job.queue_name}`, job.attempt_count);
      throw new Error(`queue handler not found: ${job.queue_name}`);
    }

    const scope: ChimpbaseExecutionScope = { kind: "queue", name: job.queue_name };
    const handlerSpans = this.sinks.map((sink) => sink.startHandlerSpan(scope));

    try {
      const payload = JSON.parse(job.payload_json) as unknown;

      let invoke = async () => {
        await this.runInTransaction(async () => {
          await this.runWithActionInvoker(async () => {
            await worker.handler(this.createContext(scope), payload);
          });
        });
      };

      for (const span of handlerSpans) {
        if (span.runInContext) {
          const prev = invoke;
          invoke = () => span.runInContext!(prev) as Promise<void>;
        }
      }

      await invoke();

      const emittedEvents = this.takeCommittedEvents();
      const allEmittedEvents = await this.handleCommittedEvents(
        emittedEvents,
        scope,
        telemetryStart,
      );
      const combinedEvents = [...emittedEvents, ...allEmittedEvents];

      await this.adapter.completeQueueJob(job.id);

      for (const span of handlerSpans) span.end("ok");

      return {
        emittedEvents: combinedEvents,
        jobId: job.id,
        queueName: job.queue_name,
      };
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      await this.failQueueJob(job.id, job.queue_name, message, job.attempt_count);
      for (const span of handlerSpans) span.end("error", message);
      throw error;
    }
  }

  async drain(options: ChimpbaseDrainOptions = {}): Promise<ChimpbaseDrainResult> {
    const startedAtMs = this.platform.now();
    const maxDurationMs = normalizeDrainMaxDuration(options.maxDurationMs);
    const maxRuns = normalizeDrainMaxRuns(options.maxRuns);
    let cronSchedules = 0;
    let nextPriority: "cron" | "queue" = "cron";
    let queueJobs = 0;

    while (cronSchedules + queueJobs < maxRuns) {
      if (this.platform.now() - startedAtMs >= maxDurationMs) {
        return {
          cronSchedules,
          idle: false,
          queueJobs,
          runs: cronSchedules + queueJobs,
          stopReason: "max_duration",
        };
      }

      const outcomes: Awaited<ReturnType<typeof this.drainInPriorityOrder>> = nextPriority === "cron"
        ? await this.drainInPriorityOrder(["cron", "queue"], startedAtMs, maxDurationMs)
        : await this.drainInPriorityOrder(["queue", "cron"], startedAtMs, maxDurationMs);

      if (outcomes.stopReason === "max_duration") {
        return {
          cronSchedules,
          idle: false,
          queueJobs,
          runs: cronSchedules + queueJobs,
          stopReason: "max_duration",
        };
      }

      if (outcomes.cronSchedules > 0 || outcomes.queueJobs > 0) {
        cronSchedules += outcomes.cronSchedules;
        queueJobs += outcomes.queueJobs;
        nextPriority = outcomes.cronSchedules > 0 ? "queue" : "cron";
        continue;
      }

      return {
        cronSchedules,
        idle: true,
        queueJobs,
        runs: cronSchedules + queueJobs,
        stopReason: "idle",
      };
    }

    return {
      cronSchedules,
      idle: false,
      queueJobs,
      runs: cronSchedules + queueJobs,
      stopReason: "max_runs",
    };
  }

  private async drainInPriorityOrder(
    priorities: readonly ["cron", "queue"] | readonly ["queue", "cron"],
    startedAtMs: number,
    maxDurationMs: number,
  ): Promise<{ cronSchedules: number; queueJobs: number; stopReason?: "max_duration" }> {
    for (const priority of priorities) {
      if (this.platform.now() - startedAtMs >= maxDurationMs) {
        return { cronSchedules: 0, queueJobs: 0, stopReason: "max_duration" };
      }

      if (priority === "cron") {
        const scheduled = await this.processNextCronSchedule();
        if (scheduled) {
          return { cronSchedules: 1, queueJobs: 0 };
        }
        continue;
      }

      const job = await this.processNextQueueJob();
      if (job) {
        return { cronSchedules: 0, queueJobs: 1 };
      }
    }

    return { cronSchedules: 0, queueJobs: 0 };
  }

  drainTelemetryRecords(): ChimpbaseTelemetryRecord[] {
    return this.telemetryRecords.splice(0);
  }

  async shutdownSinks(): Promise<void> {
    for (const sink of this.sinks) {
      await sink.shutdown?.();
    }
  }

  private async flushTelemetryToStreams(scope?: ChimpbaseExecutionScope, fromIndex = 0): Promise<void> {
    const override = scope
      ? this.registry.telemetryOverrides.get(`${scope.kind}:${scope.name}`)
      : undefined;

    const persist = this.resolveTelemetryPersist(override);
    if (!persist.log && !persist.metric && !persist.trace) {
      return;
    }

    const minLevelOrder = LOG_LEVEL_ORDER[this.telemetryConfig.minLevel] ?? 0;

    for (let i = fromIndex; i < this.telemetryRecords.length; i++) {
      const record = this.telemetryRecords[i];
      switch (record.kind) {
        case "log": {
          if (!persist.log) break;
          if ((LOG_LEVEL_ORDER[record.level] ?? 0) < minLevelOrder) break;
          await this.adapter.streamAppend(TELEMETRY_LOG_STREAM, `log.${record.level}`, {
            attributes: record.attributes,
            message: record.message,
            scope: record.scope,
            timestamp: record.timestamp,
          });
          break;
        }
        case "metric": {
          if (!persist.metric) break;
          await this.adapter.streamAppend(TELEMETRY_METRIC_STREAM, "metric", {
            labels: record.labels,
            name: record.name,
            scope: record.scope,
            timestamp: record.timestamp,
            value: record.value,
          });
          break;
        }
        case "trace": {
          if (!persist.trace) break;
          await this.adapter.streamAppend(TELEMETRY_TRACE_STREAM, `trace.${record.phase}`, {
            attributes: record.attributes,
            name: record.name,
            phase: record.phase,
            scope: record.scope,
            status: record.status,
            timestamp: record.timestamp,
          });
          break;
        }
      }
    }
  }

  private resolveTelemetryPersist(
    override?: ChimpbaseTelemetryPersistOverride,
  ): { log: boolean; metric: boolean; trace: boolean } {
    const global = this.telemetryConfig.persist;
    if (override === undefined) {
      return global;
    }
    if (typeof override === "boolean") {
      return { log: override, metric: override, trace: override };
    }
    return {
      log: override.log ?? global.log,
      metric: override.metric ?? global.metric,
      trace: override.trace ?? global.trace,
    };
  }

  createRouteEnv(): ChimpbaseRouteEnv {
    const contextMap = new Map<string, unknown>();
    const blobsClient = this.createBlobsClient();
    const env: ChimpbaseRouteEnv = {
      action: async <TArgs extends unknown[] = unknown[], TResult = unknown>(
        nameOrReference: string | ChimpbaseActionRegistrationLike,
        ...args: TArgs
      ): Promise<TResult> => await this.invokeAction<TResult>(nameOrReference, args),
      blobs: blobsClient,
      get<T = unknown>(key: string): T | undefined {
        return contextMap.get(key) as T | undefined;
      },
      set(key: string, value: unknown): void {
        contextMap.set(key, value);
      },
    };
    return this.applyRouteEnvExtensions(env);
  }

  async executeLifecycleHook(
    handler: (ctx: ChimpbaseContext) => Promise<void> | void,
  ): Promise<void> {
    await this.runWithActionInvoker(async () => {
      const ctx = this.createContext({ kind: "action", name: "__lifecycle" });
      await handler(ctx);
    });
  }

  private async processCronQueuePayload(payload: CronQueuePayload): Promise<void> {
    if (
      !payload
      || typeof payload.scheduleName !== "string"
      || payload.scheduleName.length === 0
      || !Number.isFinite(payload.fireAtMs)
    ) {
      throw new Error("cron queue payload requires scheduleName and fireAtMs");
    }

    const registration = this.registry.crons.get(payload.scheduleName);
    if (!registration) {
      throw new Error(`cron handler not found: ${payload.scheduleName}`);
    }

    const invocation: ChimpbaseCronInvocation = {
      fireAt: this.toTimestampIsoString(payload.fireAtMs),
      fireAtMs: payload.fireAtMs,
      name: payload.scheduleName,
      schedule: registration.schedule,
    };

    const scope: ChimpbaseExecutionScope = { kind: "cron", name: payload.scheduleName };
    const handlerSpans = this.sinks.map((sink) => sink.startHandlerSpan(scope));

    try {
      let invoke = async () => {
        await this.runWithActionInvoker(async () => await registration.handler(
          this.createContext(scope),
          invocation,
        ));
      };

      for (const span of handlerSpans) {
        if (span.runInContext) {
          const prev = invoke;
          invoke = () => span.runInContext!(prev) as Promise<void>;
        }
      }

      await invoke();
      for (const span of handlerSpans) span.end("ok");
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      for (const span of handlerSpans) span.end("error", message);
      throw error;
    }
  }

  private createContext(scope: ChimpbaseExecutionScope): ChimpbaseContext {
    const publishEvent = (topic: string, payload: unknown) => {
      const event = {
        name: topic,
        payload,
        payloadJson: JSON.stringify(payload ?? null),
      };

      if (this.transactionDepth > 0) {
        this.pendingEvents.push(event);
      } else {
        this.committedEvents.push(event);
      }
    };

    const enqueue = async <TPayload = unknown>(
      name: string,
      payload: TPayload,
      options?: ChimpbaseQueueEnqueueOptions,
    ): Promise<void> => await this.adapter.queueEnqueue(name, payload, options);

    const context: ChimpbaseContext = {
      db: {
        query: <T = Record<string, unknown>>(sql: string, params: readonly unknown[] = []) =>
          this.adapter.query<T>(sql, params),
        kysely: <TDatabase = Record<string, never>>() =>
          this.adapter.createKysely<TDatabase>(),
      },
      pubsub: {
        publish: (topic: string, payload: unknown) => {
          publishEvent(topic, payload);
        },
      },
      secret: (name: string) => this.secrets.get(name),
      kv: {
        delete: async (key: string) => await this.adapter.kvDelete(key),
        get: async <TValue = unknown>(key: string): Promise<TValue | null> => await this.adapter.kvGet<TValue>(key),
        list: async (options?: ChimpbaseKvListOptions): Promise<string[]> => await this.adapter.kvList(options),
        set: async <TValue = unknown>(key: string, value: TValue, options?: { ttlMs?: number }) => await this.adapter.kvSet(key, value, options?.ttlMs),
      },
      collection: {
        delete: async (name: string, filter: ChimpbaseCollectionFilter = {}): Promise<number> =>
          await this.adapter.collectionDelete(name, filter),
        find: async <TDocument = Record<string, unknown>>(
          name: string,
          filter: ChimpbaseCollectionFilter = {},
          options?: ChimpbaseCollectionFindOptions,
        ): Promise<TDocument[]> => await this.adapter.collectionFind<TDocument>(name, filter, options),
        findOne: async <TDocument = Record<string, unknown>>(
          name: string,
          filter: ChimpbaseCollectionFilter,
        ): Promise<TDocument | null> => await this.adapter.collectionFindOne<TDocument>(name, filter),
        insert: async <TDocument extends Record<string, unknown>>(name: string, document: TDocument): Promise<string> =>
          await this.adapter.collectionInsert(name, document),
        list: async (): Promise<string[]> => await this.adapter.collectionList(),
        update: async (name: string, filter: ChimpbaseCollectionFilter, patch: ChimpbaseCollectionPatch): Promise<number> =>
          await this.adapter.collectionUpdate(name, filter, patch),
      },
      stream: {
        append: async <TPayload = unknown>(stream: string, event: string, payload: TPayload): Promise<number> =>
          await this.adapter.streamAppend(stream, event, payload),
        read: async <TPayload = unknown>(
          stream: string,
          options?: ChimpbaseStreamReadOptions,
        ): Promise<ChimpbaseStreamEvent<TPayload>[]> => await this.adapter.streamRead<TPayload>(stream, options),
      },
      blobs: this.createBlobsClient(),
      enqueue,
      queue: {
        enqueue,
      },
      workflow: {
        get: async <TInput = unknown, TState = unknown>(
          workflowId: string,
        ): Promise<ChimpbaseWorkflowInstance<TInput, TState> | null> =>
          await this.getWorkflowInstance<TInput, TState>(workflowId),
        signal: async <TPayload = unknown>(
          workflowId: string,
          signalName: string,
          payload: TPayload,
        ): Promise<void> => {
          await this.signalWorkflow(workflowId, signalName, payload);
        },
        start: async <TInput = unknown, TState = unknown>(
          definition:
            | string
            | ChimpbaseWorkflowDefinition<TInput, TState>
            | ChimpbaseWorkflowRegistration<TInput, TState>,
          input: TInput,
          options?: ChimpbaseWorkflowStartOptions,
        ): Promise<ChimpbaseWorkflowStartResult> =>
          await this.startWorkflow(definition, input, options),
      },
      log: createLogger((level, message, attributes) => {
        this.recordLog(scope, level, message, attributes);
      }),
      metric: (name, value, labels = {}) => {
        this.telemetryRecords.push({
          kind: "metric",
          labels,
          name,
          scope,
          timestamp: this.timestampNow(),
          value,
        });
        for (const sink of this.sinks) sink.onMetric(scope, name, value, labels);
      },
      trace: async <TResult>(
        name: string,
        callback: (span: ChimpbaseTraceSpan) => TResult | Promise<TResult>,
        attributes: ChimpbaseTelemetryAttributes = {},
      ): Promise<TResult> => {
        const spanAttributes: ChimpbaseTelemetryAttributes = { ...attributes };
        const span: ChimpbaseTraceSpan = {
          setAttribute(key, value) {
            spanAttributes[key] = value;
          },
        };

        this.telemetryRecords.push({
          attributes: { ...spanAttributes },
          kind: "trace",
          name,
          phase: "start",
          scope,
          timestamp: this.timestampNow(),
        });

        const sinkSpans = this.sinks.map((sink) => sink.startSpan(scope, name, { ...spanAttributes }));

        try {
          let invoke: () => TResult | Promise<TResult> = () => callback(span);
          for (const sinkSpan of sinkSpans) {
            if (sinkSpan.runInContext) {
              const prev = invoke;
              invoke = () => sinkSpan.runInContext!(prev) as TResult | Promise<TResult>;
            }
          }
          const result = await invoke();
          this.telemetryRecords.push({
            attributes: { ...spanAttributes },
            kind: "trace",
            name,
            phase: "end",
            scope,
            status: "ok",
            timestamp: this.timestampNow(),
          });
          for (const sinkSpan of sinkSpans) sinkSpan.end("ok");
          return result;
        } catch (error) {
          const errorMessage = error instanceof Error ? error.message : String(error);
          this.telemetryRecords.push({
            attributes: {
              ...spanAttributes,
              error: errorMessage,
            },
            kind: "trace",
            name,
            phase: "end",
            scope,
            status: "error",
            timestamp: this.timestampNow(),
          });
          for (const sinkSpan of sinkSpans) sinkSpan.end("error", errorMessage);
          throw error;
        }
      },
      action: async <TArgs extends unknown[] = unknown[], TResult = unknown>(
        nameOrReference: string | ChimpbaseActionRegistrationLike,
        ...args: TArgs
      ): Promise<TResult> => await this.invokeAction<TResult>(nameOrReference, args),
    };
    return this.applyContextExtensions(context);
  }

  private applyContextExtensions(context: ChimpbaseContext): ChimpbaseContext {
    const extensions = this.registry.contextExtensions;
    if (!extensions || extensions.length === 0) {
      return context;
    }

    const target = context as ChimpbaseContext & Record<string, unknown>;
    for (const extension of extensions) {
      if (!extension.context) {
        continue;
      }

      target[extension.key] = extension.context(context);
    }

    return context;
  }

  private applyRouteEnvExtensions(env: ChimpbaseRouteEnv): ChimpbaseRouteEnv {
    const extensions = this.registry.contextExtensions;
    if (!extensions || extensions.length === 0) {
      return env;
    }

    const target = env as ChimpbaseRouteEnv & Record<string, unknown>;
    for (const extension of extensions) {
      if (!extension.routeEnv) {
        continue;
      }

      target[extension.key] = extension.routeEnv(env);
    }

    return env;
  }

  private async startWorkflow<TInput = unknown, TState = unknown>(
    definitionReference:
      | string
      | ChimpbaseWorkflowDefinition<TInput, TState>
      | ChimpbaseWorkflowRegistration<TInput, TState>,
    input: TInput,
    options?: ChimpbaseWorkflowStartOptions,
  ): Promise<ChimpbaseWorkflowStartResult> {
    const definition = this.resolveWorkflowDefinition(definitionReference);
    const workflowId = options?.workflowId ?? this.platform.randomUUID();

    await this.adapter.query(
      `
        INSERT INTO _chimpbase_workflow_instances (
          workflow_id,
          workflow_name,
          workflow_version,
          status,
          input_json,
          state_json,
          current_step_id,
          current_step_index,
          wake_at_ms,
          last_error,
          lease_token,
          lease_expires_at_ms
        ) VALUES (?1, ?2, ?3, 'running', ?4, ?5, ?6, 0, NULL, NULL, NULL, NULL)
      `,
      [
        workflowId,
        definition.name,
        definition.version,
        JSON.stringify(input ?? null),
        JSON.stringify(definition.initialState(input) ?? null),
        hasWorkflowSteps(definition) ? definition.steps[0]?.id ?? null : null,
      ],
    );

    await this.adapter.queueEnqueue(INTERNAL_WORKFLOW_QUEUE_NAME, { workflowId } satisfies WorkflowQueuePayload);

    return {
      status: "running",
      workflowId,
      workflowName: definition.name,
      workflowVersion: definition.version,
    };
  }

  private async signalWorkflow<TPayload = unknown>(
    workflowId: string,
    signalName: string,
    payload: TPayload,
  ): Promise<void> {
    const instance = await this.loadWorkflowInstanceRow(workflowId);
    if (!instance) {
      throw new Error(`workflow not found: ${workflowId}`);
    }

    if (instance.status === "completed" || instance.status === "failed") {
      throw new Error(`workflow is terminal: ${workflowId}`);
    }

    await this.adapter.query(
      `
        INSERT INTO _chimpbase_workflow_signals (
          workflow_id,
          signal_name,
          payload_json,
          consumed_at
        ) VALUES (?1, ?2, ?3, NULL)
      `,
      [
        workflowId,
        signalName,
        JSON.stringify(payload ?? null),
      ],
    );

    await this.adapter.queueEnqueue(INTERNAL_WORKFLOW_QUEUE_NAME, { workflowId } satisfies WorkflowQueuePayload);
  }

  private async getWorkflowInstance<TInput = unknown, TState = unknown>(
    workflowId: string,
  ): Promise<ChimpbaseWorkflowInstance<TInput, TState> | null> {
    const row = await this.loadWorkflowInstanceRow(workflowId);
    if (!row) {
      return null;
    }

    const definition = this.resolveWorkflowDefinitionByNameAndVersion(
      row.workflow_name,
      row.workflow_version,
    );

    return {
      currentStepId: row.current_step_id
        ?? (hasWorkflowSteps(definition) ? definition.steps[row.current_step_index]?.id ?? null : null),
      input: JSON.parse(row.input_json) as TInput,
      lastError: row.last_error,
      state: JSON.parse(row.state_json) as TState,
      status: row.status,
      wakeAtMs: row.wake_at_ms,
      workflowId: row.workflow_id,
      workflowName: row.workflow_name,
      workflowVersion: row.workflow_version,
    };
  }

  private async processWorkflowQueuePayload(payload: WorkflowQueuePayload): Promise<void> {
    if (!payload || typeof payload.workflowId !== "string" || payload.workflowId.length === 0) {
      throw new Error("workflow queue payload requires workflowId");
    }

    const leaseToken = await this.claimWorkflowLease(payload.workflowId);
    if (!leaseToken) {
      return;
    }

    try {
      await this.runWorkflowInstanceUntilSuspended(payload.workflowId);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      await this.failWorkflowInstance(payload.workflowId, message);
      throw error;
    } finally {
      await this.releaseWorkflowLease(payload.workflowId, leaseToken);
    }
  }

  private async runWorkflowInstanceUntilSuspended(workflowId: string): Promise<void> {
    for (let guard = 0; guard < 1_024; guard += 1) {
      const row = await this.loadWorkflowInstanceRow(workflowId);
      if (!row) {
        return;
      }

      if (row.status === "completed" || row.status === "failed") {
        return;
      }

      if (row.status === "sleeping") {
        if (row.wake_at_ms !== null && row.wake_at_ms > this.platform.now()) {
          return;
        }

        await this.adapter.query(
          `
            UPDATE _chimpbase_workflow_instances
            SET
              status = 'running',
              wake_at_ms = NULL,
              updated_at = CURRENT_TIMESTAMP
            WHERE workflow_id = ?1
          `,
          [workflowId],
        );
        continue;
      }

      const definition = this.resolveWorkflowDefinitionByNameAndVersion(
        row.workflow_name,
        row.workflow_version,
      );
      const input = JSON.parse(row.input_json) as unknown;
      const state = JSON.parse(row.state_json) as unknown;

      if (hasWorkflowRun(definition)) {
        const directive = await this.runWithActionInvoker(async () => await definition.run(
          this.createWorkflowRunContext({
            input,
            state,
            workflowId,
          }),
        ));
        const shouldContinue = await this.applyWorkflowRunDirective(workflowId, row, directive, input, state);
        if (shouldContinue) {
          continue;
        }

        return;
      }

      const step = definition.steps[row.current_step_index];

      if (!step) {
        await this.adapter.query(
          `
            UPDATE _chimpbase_workflow_instances
            SET
              status = 'completed',
              current_step_id = NULL,
              current_step_index = ?2,
              wake_at_ms = NULL,
              last_error = NULL,
              lease_token = NULL,
              lease_expires_at_ms = NULL,
              updated_at = CURRENT_TIMESTAMP,
              completed_at = CURRENT_TIMESTAMP
            WHERE workflow_id = ?1
          `,
          [workflowId, definition.steps.length],
        );
        return;
      }

      switch (step.kind) {
        case "workflow_action": {
          const args = step.args
            ? step.args({ input, state, workflowId })
            : [];
          const actionName = typeof step.action === "string"
            ? step.action
            : resolveChimpbaseActionRegistrationName(step.action);
          const result = await this.invokeActionByName(actionName, normalizeActionArgs(args));
          const nextState = step.onResult
            ? step.onResult({ input, result, state, workflowId })
            : state;

          await this.adapter.query(
            `
              UPDATE _chimpbase_workflow_instances
              SET
                state_json = ?2,
                status = 'running',
                current_step_id = ?3,
                current_step_index = ?4,
                wake_at_ms = NULL,
                last_error = NULL,
                updated_at = CURRENT_TIMESTAMP
              WHERE workflow_id = ?1
            `,
            [
              workflowId,
              JSON.stringify(nextState ?? null),
              definition.steps[row.current_step_index + 1]?.id ?? null,
              row.current_step_index + 1,
            ],
          );
          continue;
        }

        case "workflow_sleep": {
          const rawDelayMs = typeof step.delayMs === "function"
            ? step.delayMs({ input, state, workflowId })
            : step.delayMs;
          const delayMs = Math.max(0, Math.floor(rawDelayMs));

          if (delayMs === 0) {
            await this.adapter.query(
              `
                UPDATE _chimpbase_workflow_instances
                SET
                  current_step_id = ?2,
                  current_step_index = ?3,
                  updated_at = CURRENT_TIMESTAMP
                WHERE workflow_id = ?1
              `,
              [
                workflowId,
                definition.steps[row.current_step_index + 1]?.id ?? null,
                row.current_step_index + 1,
              ],
            );
            continue;
          }

          await this.adapter.query(
            `
              UPDATE _chimpbase_workflow_instances
              SET
                status = 'sleeping',
                current_step_id = ?2,
                current_step_index = ?3,
                wake_at_ms = ?4,
                last_error = NULL,
                updated_at = CURRENT_TIMESTAMP
              WHERE workflow_id = ?1
            `,
            [
              workflowId,
              definition.steps[row.current_step_index + 1]?.id ?? null,
              row.current_step_index + 1,
              this.platform.now() + delayMs,
            ],
          );
          await this.adapter.queueEnqueue(
            INTERNAL_WORKFLOW_QUEUE_NAME,
            { workflowId } satisfies WorkflowQueuePayload,
            { delayMs },
          );
          return;
        }

        case "workflow_wait_for_signal": {
          const signal = await this.findPendingWorkflowSignal(workflowId, step.signal);
          if (signal) {
            await this.adapter.query(
              `
                UPDATE _chimpbase_workflow_signals
                SET consumed_at = CURRENT_TIMESTAMP
                WHERE id = ?1 AND consumed_at IS NULL
              `,
              [signal.id],
            );

            const payloadValue = JSON.parse(signal.payload_json) as unknown;
            const nextState = step.onSignal
              ? step.onSignal({
                input,
                payload: payloadValue,
                state,
                workflowId,
              })
              : state;

            await this.adapter.query(
              `
                UPDATE _chimpbase_workflow_instances
                SET
                  state_json = ?2,
                  status = 'running',
                  current_step_id = ?3,
                  current_step_index = ?4,
                  wake_at_ms = NULL,
                  last_error = NULL,
                  updated_at = CURRENT_TIMESTAMP
                WHERE workflow_id = ?1
              `,
              [
                workflowId,
                JSON.stringify(nextState ?? null),
                definition.steps[row.current_step_index + 1]?.id ?? null,
                row.current_step_index + 1,
              ],
            );
            continue;
          }

          const timeoutMs = typeof step.timeoutMs === "function"
            ? step.timeoutMs({ input, state, workflowId })
            : step.timeoutMs;
          const now = this.platform.now();
          const wakeAtMs = typeof timeoutMs === "number"
            ? now + Math.max(0, Math.floor(timeoutMs))
            : null;

          if (row.status === "waiting_signal") {
            if (row.wake_at_ms !== null && row.wake_at_ms <= now) {
              await this.handleWorkflowSignalTimeout(workflowId, row, step, input, state);
              continue;
            }

            return;
          }

          if (wakeAtMs !== null && wakeAtMs <= now) {
            await this.handleWorkflowSignalTimeout(workflowId, row, step, input, state);
            continue;
          }

          await this.adapter.query(
            `
              UPDATE _chimpbase_workflow_instances
              SET
                status = 'waiting_signal',
                current_step_id = ?2,
                wake_at_ms = ?3,
                last_error = NULL,
                updated_at = CURRENT_TIMESTAMP
              WHERE workflow_id = ?1
            `,
            [workflowId, step.id, wakeAtMs],
          );
          if (wakeAtMs !== null) {
            await this.adapter.queueEnqueue(
              INTERNAL_WORKFLOW_QUEUE_NAME,
              { workflowId } satisfies WorkflowQueuePayload,
              { delayMs: Math.max(0, wakeAtMs - now) },
            );
          }
          return;
        }
      }
    }

    throw new Error(`workflow exceeded maximum step iterations: ${workflowId}`);
  }

  private createWorkflowRunContext<TInput = unknown, TState = unknown>(
    params: ChimpbaseWorkflowRuntimeState<TInput, TState>,
  ): ChimpbaseWorkflowRunContext<TInput, TState> {
    return {
      ...params,
      action: async <TResult = unknown>(
        nameOrReference: string | ChimpbaseActionRegistrationLike,
        ...args: unknown[]
      ): Promise<TResult> => await this.invokeAction<TResult>(nameOrReference, args),
      complete: (state = params.state, options) => ({
        kind: "workflow_complete" as const,
        state,
        stepId: options?.stepId,
      }),
      fail: (error: string, options) => ({
        error,
        kind: "workflow_fail" as const,
        state: options?.state ?? params.state,
        stepId: options?.stepId,
      }),
      sleep: (delayMs: number, options) => ({
        delayMs: Math.max(0, Math.floor(delayMs)),
        kind: "workflow_sleep_directive" as const,
        state: options?.state ?? params.state,
        stepId: options?.stepId,
      }),
      transition: (state: TState, options) => ({
        kind: "workflow_transition" as const,
        state,
        stepId: options?.stepId,
      }),
      waitForSignal: <TPayload = unknown>(
        signal: string,
        options: Omit<ChimpbaseWorkflowWaitForSignalDirective<TInput, TState, TPayload>, "kind" | "signal" | "state"> & { state?: TState } = {},
      ) => ({
        kind: "workflow_wait_for_signal_directive" as const,
        onSignal: options.onSignal,
        onTimeout: options.onTimeout,
        signal,
        state: options.state ?? params.state,
        stepId: options.stepId,
        timeoutMs: options.timeoutMs,
      }),
    };
  }

  private async applyWorkflowRunDirective(
    workflowId: string,
    row: PersistedWorkflowInstanceRow,
    directive: WorkflowRunDirective,
    input: unknown,
    state: unknown,
  ): Promise<boolean> {
    if (!directive || typeof directive !== "object" || !("kind" in directive)) {
      throw new Error(`workflow run must return a directive: ${workflowId}`);
    }

    switch (directive.kind) {
      case "workflow_transition": {
        await this.adapter.query(
          `
            UPDATE _chimpbase_workflow_instances
            SET
              state_json = ?2,
              status = 'running',
              current_step_id = ?3,
              current_step_index = 0,
              wake_at_ms = NULL,
              last_error = NULL,
              updated_at = CURRENT_TIMESTAMP
            WHERE workflow_id = ?1
          `,
          [workflowId, JSON.stringify(directive.state ?? null), directive.stepId ?? null],
        );
        return true;
      }

      case "workflow_complete": {
        await this.adapter.query(
          `
            UPDATE _chimpbase_workflow_instances
            SET
              state_json = ?2,
              status = 'completed',
              current_step_id = NULL,
              current_step_index = 0,
              wake_at_ms = NULL,
              last_error = NULL,
              lease_token = NULL,
              lease_expires_at_ms = NULL,
              updated_at = CURRENT_TIMESTAMP,
              completed_at = CURRENT_TIMESTAMP
            WHERE workflow_id = ?1
          `,
          [workflowId, JSON.stringify(directive.state ?? null)],
        );
        return false;
      }

      case "workflow_fail": {
        await this.adapter.query(
          `
            UPDATE _chimpbase_workflow_instances
            SET
              state_json = ?2,
              status = 'failed',
              current_step_id = ?3,
              wake_at_ms = NULL,
              last_error = ?4,
              lease_token = NULL,
              lease_expires_at_ms = NULL,
              updated_at = CURRENT_TIMESTAMP
            WHERE workflow_id = ?1
          `,
          [
            workflowId,
            JSON.stringify(directive.state ?? null),
            directive.stepId ?? null,
            directive.error,
          ],
        );
        return false;
      }

      case "workflow_sleep_directive": {
        if (directive.delayMs <= 0) {
          await this.adapter.query(
            `
              UPDATE _chimpbase_workflow_instances
              SET
                state_json = ?2,
                status = 'running',
                current_step_id = ?3,
                current_step_index = 0,
                wake_at_ms = NULL,
                last_error = NULL,
                updated_at = CURRENT_TIMESTAMP
              WHERE workflow_id = ?1
            `,
            [workflowId, JSON.stringify(directive.state ?? null), directive.stepId ?? null],
          );
          return true;
        }

        await this.adapter.query(
          `
            UPDATE _chimpbase_workflow_instances
            SET
              state_json = ?2,
              status = 'sleeping',
              current_step_id = ?3,
              current_step_index = 0,
              wake_at_ms = ?4,
              last_error = NULL,
              updated_at = CURRENT_TIMESTAMP
            WHERE workflow_id = ?1
          `,
          [
            workflowId,
            JSON.stringify(directive.state ?? null),
            directive.stepId ?? null,
            this.platform.now() + directive.delayMs,
          ],
        );
        await this.adapter.queueEnqueue(
          INTERNAL_WORKFLOW_QUEUE_NAME,
          { workflowId } satisfies WorkflowQueuePayload,
          { delayMs: directive.delayMs },
        );
        return false;
      }

      case "workflow_wait_for_signal_directive":
        return await this.applyWorkflowWaitForSignalDirective(workflowId, row, directive, input, state);
    }
  }

  private async applyWorkflowWaitForSignalDirective(
    workflowId: string,
    row: PersistedWorkflowInstanceRow,
    directive: Extract<WorkflowRunDirective, { kind: "workflow_wait_for_signal_directive" }>,
    input: unknown,
    state: unknown,
  ): Promise<boolean> {
    const signal = await this.findPendingWorkflowSignal(workflowId, directive.signal);
    if (signal) {
      await this.adapter.query(
        `
          UPDATE _chimpbase_workflow_signals
          SET consumed_at = CURRENT_TIMESTAMP
          WHERE id = ?1 AND consumed_at IS NULL
        `,
        [signal.id],
      );

      const payloadValue = JSON.parse(signal.payload_json) as unknown;
      const nextState = directive.onSignal
        ? directive.onSignal({
          input,
          payload: payloadValue,
          state,
          workflowId,
        })
        : state;

      await this.adapter.query(
        `
          UPDATE _chimpbase_workflow_instances
          SET
            state_json = ?2,
            status = 'running',
            current_step_id = ?3,
            current_step_index = 0,
            wake_at_ms = NULL,
            last_error = NULL,
            updated_at = CURRENT_TIMESTAMP
          WHERE workflow_id = ?1
        `,
        [workflowId, JSON.stringify(nextState ?? null), directive.stepId ?? null],
      );
      return true;
    }

    const timeoutMs = typeof directive.timeoutMs === "function"
      ? directive.timeoutMs({ input, state, workflowId })
      : directive.timeoutMs;
    const now = this.platform.now();
    const wakeAtMs = typeof timeoutMs === "number"
      ? now + Math.max(0, Math.floor(timeoutMs))
      : null;

    if (row.status === "waiting_signal") {
      if (row.wake_at_ms !== null && row.wake_at_ms <= now) {
        return await this.handleWorkflowRunSignalTimeout(workflowId, directive, input, state);
      }

      return false;
    }

    if (wakeAtMs !== null && wakeAtMs <= now) {
      return await this.handleWorkflowRunSignalTimeout(workflowId, directive, input, state);
    }

    await this.adapter.query(
      `
        UPDATE _chimpbase_workflow_instances
        SET
          state_json = ?2,
          status = 'waiting_signal',
          current_step_id = ?3,
          current_step_index = 0,
          wake_at_ms = ?4,
          last_error = NULL,
          updated_at = CURRENT_TIMESTAMP
        WHERE workflow_id = ?1
      `,
      [
        workflowId,
        JSON.stringify(directive.state ?? null),
        directive.stepId ?? directive.signal,
        wakeAtMs,
      ],
    );
    if (wakeAtMs !== null) {
      await this.adapter.queueEnqueue(
        INTERNAL_WORKFLOW_QUEUE_NAME,
        { workflowId } satisfies WorkflowQueuePayload,
        { delayMs: Math.max(0, wakeAtMs - now) },
      );
    }

    return false;
  }

  private async handleWorkflowRunSignalTimeout(
    workflowId: string,
    directive: Extract<WorkflowRunDirective, { kind: "workflow_wait_for_signal_directive" }>,
    input: unknown,
    state: unknown,
  ): Promise<boolean> {
    if (directive.onTimeout === "fail") {
      await this.adapter.query(
        `
          UPDATE _chimpbase_workflow_instances
          SET
            status = 'failed',
            current_step_id = ?2,
            wake_at_ms = NULL,
            last_error = ?3,
            lease_token = NULL,
            lease_expires_at_ms = NULL,
            updated_at = CURRENT_TIMESTAMP
          WHERE workflow_id = ?1
        `,
        [workflowId, directive.stepId ?? directive.signal, `workflow signal timed out: ${workflowId}/${directive.signal}`],
      );
      return false;
    }

    const nextState = typeof directive.onTimeout === "function"
      ? directive.onTimeout({ input, state, workflowId })
      : state;

    await this.adapter.query(
      `
        UPDATE _chimpbase_workflow_instances
        SET
          state_json = ?2,
          status = 'running',
          current_step_id = ?3,
          current_step_index = 0,
          wake_at_ms = NULL,
          last_error = NULL,
          updated_at = CURRENT_TIMESTAMP
        WHERE workflow_id = ?1
      `,
      [workflowId, JSON.stringify(nextState ?? null), directive.stepId ?? null],
    );
    return true;
  }

  private async handleWorkflowSignalTimeout(
    workflowId: string,
    row: PersistedWorkflowInstanceRow,
    step: Extract<ChimpbaseWorkflowStepDefinition, { kind: "workflow_wait_for_signal" }>,
    input: unknown,
    state: unknown,
  ): Promise<void> {
    const definition = this.resolveWorkflowDefinitionByNameAndVersion(
      row.workflow_name,
      row.workflow_version,
    );
    const nextStepId = hasWorkflowSteps(definition)
      ? definition.steps[row.current_step_index + 1]?.id ?? null
      : null;

    if (step.onTimeout === "fail") {
      throw new Error(`workflow signal timed out: ${workflowId}/${step.signal}`);
    }

    const nextState = typeof step.onTimeout === "function"
      ? step.onTimeout({ input, state, workflowId })
      : state;

    await this.adapter.query(
      `
        UPDATE _chimpbase_workflow_instances
        SET
          state_json = ?2,
          status = 'running',
          current_step_id = ?3,
          current_step_index = ?4,
          wake_at_ms = NULL,
          last_error = NULL,
          updated_at = CURRENT_TIMESTAMP
        WHERE workflow_id = ?1
      `,
      [
        workflowId,
        JSON.stringify(nextState ?? null),
        nextStepId,
        row.current_step_index + 1,
      ],
    );
  }

  private async loadWorkflowInstanceRow(workflowId: string): Promise<PersistedWorkflowInstanceRow | null> {
    const [row] = await this.adapter.query<PersistedWorkflowInstanceRow>(
      `
        SELECT
          current_step_id,
          workflow_id,
          workflow_name,
          workflow_version,
          status,
          input_json,
          state_json,
          current_step_index,
          wake_at_ms,
          last_error
        FROM _chimpbase_workflow_instances
        WHERE workflow_id = ?1
        LIMIT 1
      `,
      [workflowId],
    );

    return row ?? null;
  }

  private resolveWorkflowDefinition<TInput = unknown, TState = unknown>(
    reference:
      | string
      | ChimpbaseWorkflowDefinition<TInput, TState>
      | ChimpbaseWorkflowRegistration<TInput, TState>,
  ): ChimpbaseWorkflowDefinition<TInput, TState> {
    if (typeof reference === "string") {
      const versions = this.registry.workflows.get(reference);
      if (!versions || versions.size === 0) {
        throw new Error(`workflow not found: ${reference}`);
      }

      const latestVersion = Math.max(...versions.keys());
      return versions.get(latestVersion) as ChimpbaseWorkflowDefinition<TInput, TState>;
    }

    if ("definition" in reference) {
      return reference.definition;
    }

    return reference;
  }

  private resolveWorkflowDefinitionByNameAndVersion(
    workflowName: string,
    workflowVersion: number,
  ): ChimpbaseWorkflowDefinition {
    const versions = this.registry.workflows.get(workflowName);
    const definition = versions?.get(workflowVersion);

    if (!definition) {
      throw new Error(`workflow definition not found: ${workflowName}@${workflowVersion}`);
    }

    return definition;
  }

  private async claimWorkflowLease(workflowId: string): Promise<string | null> {
    const now = this.platform.now();
    const leaseToken = this.platform.randomUUID();
    const leaseExpiresAtMs = now + this.worker.leaseMs;

    const [row] = await this.adapter.query<{ workflow_id: string }>(
      `
        UPDATE _chimpbase_workflow_instances
        SET
          lease_token = ?2,
          lease_expires_at_ms = ?3,
          updated_at = CURRENT_TIMESTAMP
        WHERE workflow_id = ?1
          AND status IN ('running', 'sleeping', 'waiting_signal')
          AND (
            lease_token IS NULL
            OR lease_expires_at_ms IS NULL
            OR lease_expires_at_ms <= ?4
          )
        RETURNING workflow_id
      `,
      [workflowId, leaseToken, leaseExpiresAtMs, now],
    );

    return row ? leaseToken : null;
  }

  private async releaseWorkflowLease(workflowId: string, leaseToken: string): Promise<void> {
    await this.adapter.query(
      `
        UPDATE _chimpbase_workflow_instances
        SET
          lease_token = NULL,
          lease_expires_at_ms = NULL,
          updated_at = CURRENT_TIMESTAMP
        WHERE workflow_id = ?1 AND lease_token = ?2
      `,
      [workflowId, leaseToken],
    );
  }

  private async failWorkflowInstance(workflowId: string, errorMessage: string): Promise<void> {
    await this.adapter.query(
      `
        UPDATE _chimpbase_workflow_instances
        SET
          status = 'failed',
          wake_at_ms = NULL,
          last_error = ?2,
          lease_token = NULL,
          lease_expires_at_ms = NULL,
          updated_at = CURRENT_TIMESTAMP
        WHERE workflow_id = ?1
      `,
      [workflowId, errorMessage],
    );
  }

  private async findPendingWorkflowSignal(
    workflowId: string,
    signalName: string,
  ): Promise<PersistedWorkflowSignalRow | null> {
    const [row] = await this.adapter.query<PersistedWorkflowSignalRow>(
      `
        SELECT
          id,
          payload_json
        FROM _chimpbase_workflow_signals
        WHERE workflow_id = ?1
          AND signal_name = ?2
          AND consumed_at IS NULL
        ORDER BY id ASC
        LIMIT 1
      `,
      [workflowId, signalName],
    );

    return row ?? null;
  }

  private async invokeAction<TResult = unknown>(
    nameOrReference: string | ChimpbaseActionRegistrationLike,
    args: unknown[],
  ): Promise<TResult> {
    if (typeof nameOrReference === "string") {
      return await this.invokeActionByName<TResult>(nameOrReference, args);
    }

    return await this.invokeActionByName<TResult>(
      nameOrReference.name,
      normalizeActionReferenceArgs(nameOrReference, args),
    );
  }

  private async invokeActionByName<TResult = unknown>(
    name: string,
    args: unknown[],
  ): Promise<TResult> {
    const registration = this.registry.actions.get(name);
    if (!registration) {
      throw new Error(`action not found: ${name}`);
    }

    return await this.runInTransaction(async () => await this.runWithActionInvoker(async () => {
      const context = this.createContext({ kind: "action", name });
      if (registration.args) {
        if (args.length > 1) {
          throw new Error(`action ${name} expects a single argument`);
        }

        const parsedArgs = registration.args.parse(args[0], "args");
        return await (registration.handler as (ctx: ChimpbaseContext, args: unknown) => TResult | Promise<TResult>)(
          context,
          parsedArgs,
        );
      }

      return await (registration.handler as (ctx: ChimpbaseContext, ...args: unknown[]) => TResult | Promise<TResult>)(
        context,
        ...args,
      );
    }));
  }

  private async dispatchSubscriptions(events: ChimpbaseEventRecord[]): Promise<void> {
    for (const event of events) {
      const subscriptions = this.registry.subscriptions.get(event.name) ?? [];
      for (const sub of subscriptions) {
        await this.runInTransaction(async () => {
          if (sub.idempotent && event.id !== undefined) {
            const key = `_chimpbase.sub.seen:${event.id}:${sub.name}`;
            if (await this.adapter.kvGet<boolean>(key)) return;
            await this.runWithActionInvoker(async () => {
              await sub.handler(this.createContext({ kind: "subscription", name: event.name }), event.payload);
            });
            await this.adapter.kvSet(key, true);
          } else {
            await this.runWithActionInvoker(async () => {
              await sub.handler(this.createContext({ kind: "subscription", name: event.name }), event.payload);
            });
          }
        });
      }
    }
  }

  private async enqueueSubscriptionDispatchJobs(events: ChimpbaseEventRecord[]): Promise<void> {
    for (const event of events) {
      if ((this.registry.subscriptions.get(event.name) ?? []).length === 0) {
        continue;
      }

      await this.adapter.queueEnqueue(INTERNAL_SUBSCRIPTION_QUEUE_NAME, {
        eventId: event.id,
        eventName: event.name,
        payload: event.payload,
        payloadJson: event.payloadJson,
      } satisfies SubscriptionQueuePayload);
    }
  }

  private async handleCommittedEvents(
    emittedEvents: ChimpbaseEventRecord[],
    scope: ChimpbaseExecutionScope | undefined,
    telemetryStart: number,
  ): Promise<ChimpbaseEventRecord[]> {
    if (this.subscriptionsConfig.dispatch === "async") {
      await this.enqueueSubscriptionDispatchJobs(emittedEvents);
      await this.flushTelemetryToStreams(scope, telemetryStart);
      return [];
    }

    await this.dispatchSubscriptions(emittedEvents);
    const cascadedEvents = this.takeCommittedEvents();
    await this.flushTelemetryToStreams(scope, telemetryStart);
    return cascadedEvents;
  }

  private async runWithActionInvoker<TResult>(callback: () => TResult | Promise<TResult>): Promise<TResult> {
    const invoker: ChimpbaseActionInvoker = async <TResult = unknown>(
      nameOrReference: string | ChimpbaseActionRegistrationLike,
      args: unknown[],
    ): Promise<TResult> => await this.invokeAction<TResult>(nameOrReference, args);

    return await runWithActionInvoker(invoker, callback);
  }

  private async runInTransaction<T>(callback: () => Promise<T>): Promise<T> {
    const isRootTransaction = this.transactionDepth === 0;

    if (isRootTransaction) {
      await this.adapter.beginTransaction();
    }

    this.transactionDepth += 1;

    try {
      const result = await callback();
      this.transactionDepth -= 1;

      if (isRootTransaction) {
        await this.adapter.commitTransaction(this.pendingEvents);
        const justCommitted = this.pendingEvents.splice(0);
        this.committedEvents.push(...justCommitted);
        if (justCommitted.length > 0) {
          await this.eventBus.publish(justCommitted);
        }
      }

      return result;
    } catch (error) {
      this.transactionDepth = Math.max(0, this.transactionDepth - 1);

      if (isRootTransaction) {
        this.pendingEvents.splice(0);
        await this.adapter.rollbackTransaction();
      }

      throw error;
    }
  }

  private async failQueueJob(
    jobId: number,
    queueName: string,
    errorMessage: string,
    attempts: number,
  ): Promise<void> {
    const worker = this.registry.workers.get(queueName);
    const dlqName = worker?.definition.dlq;
    const shouldDlq = typeof dlqName === "string" && attempts >= this.worker.maxAttempts;

    if (shouldDlq && worker) {
      const payloadJson = await this.adapter.getQueueJobPayload(jobId);
      if (payloadJson) {
        const envelope: ChimpbaseDlqEnvelope = {
          attempts,
          error: errorMessage,
          failedAt: this.timestampNow(),
          payload: JSON.parse(payloadJson) as unknown,
          queue: queueName,
        };
        await this.adapter.queueEnqueue(dlqName, envelope);
      }
    }

    const nextStatus = shouldDlq ? "dlq" : (attempts >= this.worker.maxAttempts ? "failed" : "pending");
    const nextAvailableAtMs = this.platform.now() + this.worker.retryDelayMs;
    await this.adapter.markQueueJobFailure(jobId, nextStatus, nextAvailableAtMs, errorMessage);
  }

  private recordLog(
    scope: ChimpbaseExecutionScope,
    level: "debug" | "info" | "warn" | "error",
    message: string,
    attributes: ChimpbaseTelemetryAttributes = {},
  ): void {
    this.telemetryRecords.push({
      attributes,
      kind: "log",
      level,
      message,
      scope,
      timestamp: this.timestampNow(),
    });
    for (const sink of this.sinks) sink.onLog(scope, level, message, attributes);
  }

  private timestampNow(): string {
    return this.toTimestampIsoString(this.platform.now());
  }

  private toTimestampIsoString(timestampMs: number): string {
    return new Date(timestampMs).toISOString();
  }

  private takeCommittedEvents(): ChimpbaseEventRecord[] {
    return this.committedEvents.splice(0);
  }

  private createBlobsClient(): ChimpbaseBlobsClient {
    const config = this.blobsConfig;
    const disabled = (): never => {
      throw new Error(
        "chimpbase blobs is not configured; pass `blobs: { driver }` to createChimpbase",
      );
    };
    if (!config) {
      return {
        put: disabled,
        get: disabled,
        head: disabled,
        delete: disabled,
        deleteMany: disabled,
        copy: disabled,
        list: disabled,
        createUpload: disabled,
        resumeUpload: disabled,
        listUploads: disabled,
        sign: disabled,
      };
    }

    const allowedBuckets = new Set(config.buckets);
    const assertBucket = (bucket: string): void => {
      if (allowedBuckets.size > 0 && !allowedBuckets.has(bucket)) {
        throw new Error(`unknown blob bucket: ${bucket}`);
      }
    };
    const ensureBuckets = async (): Promise<void> => {
      if (this.blobsBucketsReady) return;
      for (const bucket of config.buckets) {
        await config.driver.ensureBucket(bucket);
      }
      this.blobsBucketsReady = true;
    };
    const adapter = this.adapter;
    const platform = this.platform;
    const driver = config.driver;

    const toStream = (body: Uint8Array | ReadableStream<Uint8Array>): ReadableStream<Uint8Array> => {
      if (body instanceof ReadableStream) return body;
      return new ReadableStream<Uint8Array>({
        start(controller) {
          controller.enqueue(body);
          controller.close();
        },
      });
    };
    const toMetadata = (row: ChimpbaseBlobMetaRow): ChimpbaseBlobMetadata => ({
      bucket: row.bucket,
      key: row.key,
      size: row.size,
      etag: row.etag,
      contentType: row.contentType,
      metadata: row.metadata,
      createdAt: row.createdAt,
      updatedAt: row.updatedAt,
    });
    const buildUpload = (row: ChimpbaseBlobUploadRow): ChimpbaseBlobUpload => ({
      id: row.uploadId,
      bucket: row.bucket,
      key: row.key,
      async writePart(partNumber, body) {
        if (!Number.isInteger(partNumber) || partNumber < 1) {
          throw new Error("partNumber must be a positive integer");
        }
        const result = await driver.putPart(row.uploadId, partNumber, toStream(body));
        await adapter.blobRecordPart({
          uploadId: row.uploadId,
          partNumber,
          size: result.size,
          etag: result.sha256,
          driverRef: result.driverRef,
          createdAt: new Date(platform.now()).toISOString(),
        });
        return { etag: result.sha256, size: result.size };
      },
      async complete() {
        const parts = (await adapter.blobListParts(row.uploadId)).slice().sort(
          (a, b) => a.partNumber - b.partNumber,
        );
        if (parts.length === 0) {
          throw new Error(`upload ${row.uploadId} has no parts`);
        }
        const assembled = await driver.assemble(
          row.uploadId,
          parts.map((part) => ({ partNumber: part.partNumber, driverRef: part.driverRef })),
          row.bucket,
          row.key,
        );
        const compositeEtag = `${await hashPartEtags(parts.map((p) => p.etag))}-${parts.length}`;
        const nowIso = new Date(platform.now()).toISOString();
        const metaRow: ChimpbaseBlobMetaRow = {
          bucket: row.bucket,
          key: row.key,
          size: assembled.size,
          etag: compositeEtag,
          contentType: row.contentType ?? "application/octet-stream",
          metadata: row.metadata,
          driverRef: assembled.driverRef,
          createdAt: nowIso,
          updatedAt: nowIso,
        };
        await adapter.blobFinalizeUpload(row.uploadId, metaRow);
        return {
          bucket: metaRow.bucket,
          key: metaRow.key,
          size: metaRow.size,
          etag: metaRow.etag,
        };
      },
      async abort() {
        await driver.abortUpload(row.uploadId);
        await adapter.blobAbortUpload(row.uploadId);
      },
      async listParts() {
        const parts = await adapter.blobListParts(row.uploadId);
        return parts
          .slice()
          .sort((a, b) => a.partNumber - b.partNumber)
          .map((part) => ({
            partNumber: part.partNumber,
            size: part.size,
            etag: part.etag,
          }));
      },
    });

    return {
      put: async (bucket, key, body, options) => {
        assertBucket(bucket);
        await ensureBuckets();
        if (options?.ifMatch || options?.ifNoneMatch !== undefined) {
          const existing = await adapter.blobGetMetadata(bucket, key);
          if (options.ifNoneMatch === "*" && existing) {
            throw new ChimpbasePreconditionFailedError(`blob ${bucket}/${key} already exists`);
          }
          if (options.ifMatch && (!existing || existing.etag !== options.ifMatch)) {
            throw new ChimpbasePreconditionFailedError(`blob ${bucket}/${key} etag mismatch`);
          }
        }
        const driverResult = await driver.put(bucket, key, toStream(body));
        const nowIso = new Date(platform.now()).toISOString();
        const metadata = options?.metadata ?? {};
        const contentType = options?.contentType ?? "application/octet-stream";
        await adapter.blobPutMetadata({
          bucket,
          key,
          size: driverResult.size,
          etag: driverResult.sha256,
          contentType,
          metadata,
          driverRef: driverResult.driverRef,
          createdAt: nowIso,
          updatedAt: nowIso,
        });
        return { bucket, key, size: driverResult.size, etag: driverResult.sha256 };
      },
      get: async (bucket, key, options) => {
        assertBucket(bucket);
        const row = await adapter.blobGetMetadata(bucket, key);
        if (!row) return null;
        if (options?.ifNoneMatch && options.ifNoneMatch === row.etag) {
          throw new ChimpbaseNotModifiedError(`blob ${bucket}/${key} not modified`);
        }
        const payload = await driver.get(bucket, key, row.driverRef, options?.range);
        if (!payload) return null;
        return {
          ...toMetadata(row),
          size: payload.size,
          body: payload.body,
        };
      },
      head: async (bucket, key) => {
        assertBucket(bucket);
        const row = await adapter.blobGetMetadata(bucket, key);
        return row ? toMetadata(row) : null;
      },
      delete: async (bucket, key) => {
        assertBucket(bucket);
        const row = await adapter.blobGetMetadata(bucket, key);
        if (!row) return false;
        await driver.delete(bucket, key, row.driverRef);
        return await adapter.blobDeleteMetadata(bucket, key);
      },
      deleteMany: async (bucket, keys) => {
        assertBucket(bucket);
        const deleted: string[] = [];
        const errors: { key: string; error: string }[] = [];
        for (const key of keys) {
          try {
            const row = await adapter.blobGetMetadata(bucket, key);
            if (!row) continue;
            await driver.delete(bucket, key, row.driverRef);
            await adapter.blobDeleteMetadata(bucket, key);
            deleted.push(key);
          } catch (error) {
            errors.push({
              key,
              error: error instanceof Error ? error.message : String(error),
            });
          }
        }
        return { deleted, errors };
      },
      copy: async (src, dst, options) => {
        assertBucket(src.bucket);
        assertBucket(dst.bucket);
        const row = await adapter.blobGetMetadata(src.bucket, src.key);
        if (!row) {
          throw new Error(`source blob ${src.bucket}/${src.key} not found`);
        }
        const driverResult = await driver.copy(
          { bucket: src.bucket, key: src.key, driverRef: row.driverRef },
          { bucket: dst.bucket, key: dst.key },
        );
        const nowIso = new Date(platform.now()).toISOString();
        await adapter.blobPutMetadata({
          bucket: dst.bucket,
          key: dst.key,
          size: driverResult.size,
          etag: driverResult.sha256,
          contentType: options?.contentType ?? row.contentType,
          metadata: options?.metadata ?? row.metadata,
          driverRef: driverResult.driverRef,
          createdAt: nowIso,
          updatedAt: nowIso,
        });
        return {
          bucket: dst.bucket,
          key: dst.key,
          size: driverResult.size,
          etag: driverResult.sha256,
        };
      },
      list: async (bucket, options) => {
        assertBucket(bucket);
        const result = await adapter.blobListMetadata(bucket, options ?? {});
        return {
          entries: result.entries.map((row) => ({
            key: row.key,
            size: row.size,
            etag: row.etag,
            contentType: row.contentType,
            updatedAt: row.updatedAt,
          })),
          commonPrefixes: result.commonPrefixes,
          nextCursor: result.nextCursor,
        };
      },
      createUpload: async (bucket, key, options) => {
        assertBucket(bucket);
        await ensureBuckets();
        const uploadId = platform.randomUUID();
        const createdMs = platform.now();
        const ttlMs = options?.ttlMs ?? 24 * 60 * 60 * 1000;
        const row: ChimpbaseBlobUploadRow = {
          uploadId,
          bucket,
          key,
          contentType: options?.contentType ?? null,
          metadata: options?.metadata ?? {},
          driverRef: uploadId,
          createdAtMs: createdMs,
          expiresAtMs: createdMs + ttlMs,
        };
        await adapter.blobInitUpload(row);
        return buildUpload(row);
      },
      resumeUpload: async (uploadId) => {
        const row = await adapter.blobGetUpload(uploadId);
        if (!row) {
          throw new Error(`upload ${uploadId} not found`);
        }
        return buildUpload(row);
      },
      listUploads: async (bucket, options) => {
        assertBucket(bucket);
        const result = await adapter.blobListUploads(bucket, options ?? {});
        return {
          uploads: result.uploads.map((row) => ({
            id: row.uploadId,
            bucket: row.bucket,
            key: row.key,
            createdAt: new Date(row.createdAtMs).toISOString(),
            expiresAt: new Date(row.expiresAtMs).toISOString(),
          })),
          nextCursor: result.nextCursor,
        };
      },
      sign: (options) => {
        assertBucket(options.bucket);
        if (!config.signer) {
          throw new Error("blob signing secret not configured");
        }
        return config.signer.sign(options);
      },
    };
  }

  getBlobsEngineConfig(): ChimpbaseBlobsEngineConfig | null {
    return this.blobsConfig;
  }

  getBlobsAdapter(): ChimpbaseEngineAdapter {
    return this.adapter;
  }
}

export class ChimpbasePreconditionFailedError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ChimpbasePreconditionFailedError";
  }
}

export class ChimpbaseNotModifiedError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ChimpbaseNotModifiedError";
  }
}

async function hashPartEtags(etags: readonly string[]): Promise<string> {
  const bytes = new TextEncoder().encode(etags.join(""));
  const digest = await crypto.subtle.digest("SHA-256", bytes);
  const view = new Uint8Array(digest);
  let out = "";
  for (const byte of view) {
    out += byte.toString(16).padStart(2, "0");
  }
  return out;
}

function normalizeActionArgs(args: unknown): unknown[] {
  if (args === undefined) {
    return [];
  }

  return Array.isArray(args) ? args : [args];
}

function normalizeActionReferenceArgs(
  reference: ChimpbaseActionRegistrationLike,
  args: unknown[],
): unknown[] {
  if (!reference.args) {
    return args;
  }

  if (args.length > 1) {
    throw new Error(`action ${reference.name} expects a single argument`);
  }

  return args.length === 0 ? [] : [args[0]];
}

function hasWorkflowRun<TInput = unknown, TState = unknown>(
  definition: ChimpbaseWorkflowDefinition<TInput, TState>,
): definition is ChimpbaseWorkflowDefinition<TInput, TState> & { run: NonNullable<ChimpbaseWorkflowDefinition<TInput, TState>["run"]> } {
  return typeof (definition as { run?: unknown }).run === "function";
}

function hasWorkflowSteps<TInput = unknown, TState = unknown>(
  definition: ChimpbaseWorkflowDefinition<TInput, TState>,
): definition is ChimpbaseWorkflowDefinition<TInput, TState> & ChimpbaseWorkflowStepsDraftDefinition<TInput, TState> {
  return Array.isArray((definition as { steps?: unknown }).steps);
}

function createLogger(
  record: (
    level: "debug" | "info" | "warn" | "error",
    message: string,
    attributes?: ChimpbaseTelemetryAttributes,
  ) => void,
): ChimpbaseLogger {
  return {
    debug(message, attributes) {
      record("debug", message, attributes);
    },
    info(message, attributes) {
      record("info", message, attributes);
    },
    warn(message, attributes) {
      record("warn", message, attributes);
    },
    error(message, attributes) {
      record("error", message, attributes);
    },
  };
}

function normalizeDrainMaxDuration(maxDurationMs?: number): number {
  if (typeof maxDurationMs !== "number" || !Number.isFinite(maxDurationMs)) {
    return Number.POSITIVE_INFINITY;
  }

  return Math.max(0, Math.floor(maxDurationMs));
}

function normalizeDrainMaxRuns(maxRuns?: number): number {
  if (typeof maxRuns !== "number" || !Number.isFinite(maxRuns)) {
    return Number.POSITIVE_INFINITY;
  }

  return Math.max(0, Math.floor(maxRuns));
}
