import type { ChimpbaseContext, ChimpbaseValidator } from "@chimpbase/runtime";

/** The widest method shape a service can expose. */
export type ServiceMethod = (...args: never[]) => unknown;

/** A bag of service methods whose signatures are no longer statically known. */
export type ServiceMethods = Record<string, ServiceMethod>;

export interface ServiceSelf<TSettings = unknown, TMethods extends ServiceMethods = ServiceMethods> {
  readonly call: <TResult>(
    actionName: string,
    args: unknown,
    result: ChimpbaseValidator<TResult>,
    options?: CallOptions,
  ) => Promise<TResult>;
  readonly emit: <TPayload = unknown>(event: string, payload: TPayload, options?: EmitOptions) => Promise<void>;
  readonly methods: TMethods & ServiceMethods;
  readonly name: string;
  readonly nodeId: string;
  readonly settings: TSettings;
  readonly version: number;
}

export type ServiceActionHandler<
  TSettings = unknown,
  TMethods extends ServiceMethods = ServiceMethods,
  TArgs = unknown,
  TResult = unknown,
> = (
  ctx: ChimpbaseContext,
  args: TArgs,
  self: ServiceSelf<TSettings, TMethods>,
) => Promise<TResult> | TResult;

export type ServiceEventHandler<
  TSettings = unknown,
  TMethods extends ServiceMethods = ServiceMethods,
  TPayload = unknown,
> = (
  ctx: ChimpbaseContext,
  payload: TPayload,
  self: ServiceSelf<TSettings, TMethods>,
) => Promise<void> | void;

export interface ServiceEventDefinition<
  TSettings = unknown,
  TMethods extends ServiceMethods = ServiceMethods,
  TPayload = unknown,
> {
  balanced?: boolean;
  handler: ServiceEventHandler<TSettings, TMethods, TPayload>;
}

export interface ServiceDefinition<
  TSettings = unknown,
  TMethods extends ServiceMethods = ServiceMethods,
> {
  actions?: Record<string, ServiceActionHandler<TSettings, TMethods, never, unknown>>;
  events?: Record<
    string,
    | ServiceEventHandler<TSettings, TMethods, never>
    | ServiceEventDefinition<TSettings, TMethods, never>
  >;
  methods?: TMethods;
  mixins?: readonly AnyServiceDefinition[];
  name: string;
  settings?: TSettings;
  started?: (ctx: ChimpbaseContext, self: ServiceSelf<TSettings, TMethods>) => Promise<void> | void;
  stopped?: () => Promise<void> | void;
  version?: number;
}

// Settings and method bags are invariant, so a definition that has left its
// declaration site is described by the erased shapes below. Dispatching one
// re-applies the concrete argument type at the call.

/** A service action handler whose argument, result, settings, and method types have been erased. */
export type AnyServiceActionHandler = (ctx: ChimpbaseContext, args: never, self: never) => unknown;

/** A service event handler whose payload, settings, and method types have been erased. */
export type AnyServiceEventHandler = (ctx: ChimpbaseContext, payload: never, self: never) => unknown;

/** A service event definition whose payload type has been erased. */
export interface AnyServiceEventDefinition {
  balanced?: boolean;
  handler: AnyServiceEventHandler;
}

/** A service definition whose settings and method types have been erased. */
export interface AnyServiceDefinition {
  actions?: Record<string, AnyServiceActionHandler>;
  events?: Record<string, AnyServiceEventHandler | AnyServiceEventDefinition>;
  methods?: ServiceMethods;
  mixins?: readonly AnyServiceDefinition[];
  name: string;
  settings?: unknown;
  started?: (ctx: ChimpbaseContext, self: never) => unknown;
  stopped?: () => Promise<void> | void;
  version?: number;
}

/** Dispatch shape used when a resolved handler is invoked with runtime values. */
export type ServiceActionDispatch = (
  ctx: ChimpbaseContext,
  args: unknown,
  self: ServiceSelf,
) => Promise<unknown> | unknown;

/** Dispatch shape used when a resolved event handler is invoked with runtime values. */
export type ServiceEventDispatch = (
  ctx: ChimpbaseContext,
  payload: unknown,
  self: ServiceSelf,
) => Promise<unknown> | unknown;

export type LoadBalanceStrategy = "local-first" | "round-robin" | "random" | "cpu";

export interface CallOptions {
  fallback?: (error: Error) => unknown | Promise<unknown>;
  nodeId?: string;
  retry?: { attempts: number; delayMs?: number };
  strategy?: LoadBalanceStrategy;
  timeoutMs?: number;
}

export interface EmitOptions {
  balanced?: boolean;
}

export interface MeshCallFn {
  <TResult>(
    actionName: string,
    args: unknown,
    result: ChimpbaseValidator<TResult>,
    options: CallOptions,
  ): Promise<TResult>;
}

export type MeshCallMiddleware = (next: MeshCallFn) => MeshCallFn;

export interface NodeServiceEntry {
  actions: readonly string[];
  events: readonly string[];
  name: string;
  version: number;
}

export interface NodeRecord {
  advertisedUrl: string | null;
  lastHeartbeatMs: number;
  metadata: Record<string, unknown>;
  nodeId: string;
  services: readonly NodeServiceEntry[];
  startedAtMs: number;
}

export interface ChimpbaseMeshClient {
  call<TResult>(
    actionName: string,
    args: unknown,
    result: ChimpbaseValidator<TResult>,
    options?: CallOptions,
  ): Promise<TResult>;
  emit<TPayload = unknown>(event: string, payload: TPayload, options?: EmitOptions): Promise<void>;
  nodeId(): string;
  peers(): readonly NodeRecord[];
}

export class MeshActionNotFoundError extends Error {
  readonly actionName: string;
  constructor(actionName: string) {
    super(`mesh action not found: ${actionName}`);
    this.name = "MeshActionNotFoundError";
    this.actionName = actionName;
  }
}

export class MeshNoAvailableNodeError extends Error {
  readonly actionName: string;
  constructor(actionName: string) {
    super(`no mesh node currently serves action: ${actionName}`);
    this.name = "MeshNoAvailableNodeError";
    this.actionName = actionName;
  }
}

export class MeshTimeoutError extends Error {
  readonly actionName: string;
  readonly nodeId: string | null;
  constructor(actionName: string, nodeId: string | null, timeoutMs: number) {
    super(`mesh call timed out after ${timeoutMs}ms: ${actionName}`);
    this.name = "MeshTimeoutError";
    this.actionName = actionName;
    this.nodeId = nodeId;
  }
}

export class MeshCallError extends Error {
  readonly actionName: string;
  readonly nodeId: string | null;
  readonly cause?: unknown;
  readonly retryable: boolean;
  constructor(actionName: string, nodeId: string | null, message: string, cause?: unknown, retryable = false) {
    super(message);
    this.name = "MeshCallError";
    this.actionName = actionName;
    this.nodeId = nodeId;
    this.cause = cause;
    this.retryable = retryable;
  }
}

export class MeshConfigurationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "MeshConfigurationError";
  }
}
