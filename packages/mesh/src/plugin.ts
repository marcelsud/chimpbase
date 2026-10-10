import {
  action,
  contextExtension,
  cron,
  isJsonObject,
  onStart,
  onStop,
  plugin,
  route,
  subscription,
  worker,
  type ChimpbaseContext,
  type ChimpbasePluginDependency,
  type ChimpbasePluginRegistration,
  type ChimpbaseRegistrationSource,
  type ChimpbaseRouteHandler,
  type ChimpbaseValidator,
} from "@chimpbase/runtime";

import {
  createCallDispatcher,
  type RemoteDispatcher,
} from "./call.ts";
import {
  INFO_EVENT_ANNOUNCE,
  INFO_EVENT_LEAVE,
  MeshPeerCache,
  type AnnouncePayload,
  type LeavePayload,
} from "./discovery.ts";
import { balancedWorkerName, meshEmit, type BalancedEnvelope } from "./emit.ts";
import {
  assertAdvertisedUrlSafeForPeers,
  generateNodeId,
  requireHttpTransportConfig,
  resolveAdvertisedUrl,
} from "./node-id.ts";
import {
  deleteNode,
  ensureRegistrySchema,
  gcStaleNodes,
  listLiveNodes,
  touchHeartbeat,
  upsertNode,
  type UpsertNodeInput,
} from "./registry.ts";
import {
  prefixedActionName,
  resolveService,
  type ResolvedService,
} from "./service.ts";
import {
  DEFAULT_RPC_PATH,
  MESH_TOKEN_HEADER,
  RPC_EXECUTE_ACTION,
  compareTokens,
  createHttpDispatcher,
} from "./transport-http.ts";
import type {
  AnyServiceDefinition,
  CallOptions,
  ChimpbaseMeshClient,
  EmitOptions,
  LoadBalanceStrategy,
  MeshCallMiddleware,
  NodeServiceEntry,
  ServiceActionDispatch,
  ServiceEventDispatch,
  ServiceSelf,
} from "./types.ts";

export interface ChimpbaseMeshOptions {
  advertisedUrl?: string;
  defaultRetries?: number;
  defaultStrategy?: LoadBalanceStrategy;
  defaultTimeoutMs?: number;
  dependsOn?: readonly ChimpbasePluginDependency[];
  gcAfterMs?: number;
  heartbeatMs?: number;
  meshToken?: string;
  meta?: Record<string, unknown>;
  middleware?: readonly MeshCallMiddleware[];
  name?: string;
  offlineAfterMs?: number;
  rpcPath?: string;
  services: readonly AnyServiceDefinition[];
  transport?: "local-only" | "http";
}
function hasUnref(value: unknown): value is { unref(): void } {
  return typeof value === "object"
    && value !== null
    && "unref" in value
    && typeof value.unref === "function";
}

function isServiceActionDispatch(value: unknown): value is ServiceActionDispatch {
  return typeof value === "function";
}

function isServiceEventDispatch(value: unknown): value is ServiceEventDispatch {
  return typeof value === "function";
}

function isServiceStartedHook(
  value: unknown,
): value is (ctx: ChimpbaseContext, self: ServiceSelf) => unknown {
  return typeof value === "function";
}


export function chimpbaseMesh(options: ChimpbaseMeshOptions): ChimpbasePluginRegistration {
  const transport = options.transport ?? "http";
  const heartbeatMs = options.heartbeatMs ?? 10_000;
  const offlineAfterMs = options.offlineAfterMs ?? 30_000;
  const gcAfterMs = options.gcAfterMs ?? 600_000;
  const defaultStrategy: LoadBalanceStrategy = options.defaultStrategy ?? "local-first";
  const defaultTimeoutMs = options.defaultTimeoutMs ?? 5_000;
  const defaultRetries = options.defaultRetries ?? 0;
  const rpcPath = options.rpcPath ?? DEFAULT_RPC_PATH;
  const middleware = options.middleware ?? [];
  const metadata = { ...options.meta };

  const services = options.services.map((def) => resolveService(def));
  if (services.length === 0) {
    throw new Error("chimpbaseMesh: at least one service is required");
  }

  const serviceEntries = buildServiceEntries(services);
  const localActionNames = new Set<string>(serviceEntries.flatMap((entry) => entry.actions));

  const balancedEventSet = new Set<string>();
  for (const svc of services) {
    for (const [eventName, event] of Object.entries(svc.events)) {
      if ((event.balanced === true)) {
        balancedEventSet.add(eventName);
      }
    }
  }

  const nodeId = generateNodeId();
  const advertisedUrl = resolveAdvertisedUrl({ explicit: options.advertisedUrl ?? null, transport });
  assertAdvertisedUrlSafeForPeers(advertisedUrl, transport);
  requireHttpTransportConfig({ transport, meshToken: options.meshToken, advertisedUrl });

  const cache = new MeshPeerCache(offlineAfterMs);
  const startedAtMs = Date.now();
  const heartbeatState: {
    running: Promise<void> | null;
    timer: ReturnType<typeof setInterval> | null;
  } = { running: null, timer: null };

  const remoteDispatcher: RemoteDispatcher | null = transport === "http"
    ? createHttpDispatcher({
        callerNodeId: nodeId,
        rpcPath,
        tokenProvider: () => currentToken,
      })
    : null;

  let currentToken: string | null = null;

  const dispatcher = createCallDispatcher({
    cache,
    defaultRetries,
    defaultStrategy,
    defaultTimeoutMs,
    localActionNames,
    localMetadata: metadata,
    localNodeId: nodeId,
    middleware,
    remoteDispatcher,
  });

  const clientFor = (ctx: ChimpbaseContext): ChimpbaseMeshClient => ({
    call: async <TResult>(
      actionName: string,
      args: unknown,
      result: ChimpbaseValidator<TResult>,
      opts?: CallOptions,
    ) => {
      currentToken = (options.meshToken !== undefined && options.meshToken.length > 0) ? ctx.secret(options.meshToken) : null;
      return await dispatcher(ctx, actionName, args, result, opts ?? {});
    },
    emit: async (event, payload, opts?: EmitOptions) => {
      await meshEmit(ctx, event, payload, opts ?? {});
    },
    nodeId: () => nodeId,
    peers: () => cache.all(),
  });

  const entries: ChimpbaseRegistrationSource[] = [];

  entries.push(
    contextExtension("mesh", {
      context: (ctx) => clientFor(ctx),
    }),
  );

  entries.push(...buildServiceRegistrations(services));

  entries.push(
    subscription<AnnouncePayload>(INFO_EVENT_ANNOUNCE, async (_ctx, payload) => {
      if (typeof payload?.nodeId !== "string" || payload.nodeId.length === 0 || payload.nodeId === nodeId) {
        return;
      }

      cache.upsert({
        advertisedUrl: payload.advertisedUrl ?? null,
        lastHeartbeatMs: Date.now(),
        metadata: payload.metadata ?? {},
        nodeId: payload.nodeId,
        services: payload.services ?? [],
        startedAtMs: payload.startedAtMs ?? Date.now(),
      });
    }, { dispatch: "sync", idempotent: false }),
  );

  entries.push(
    subscription<LeavePayload>(INFO_EVENT_LEAVE, async (_ctx, payload) => {
      if (typeof payload?.nodeId !== "string" || payload.nodeId.length === 0 || payload.nodeId === nodeId) {
        return;
      }

      cache.remove(payload.nodeId);
    }, { dispatch: "sync", idempotent: false }),
  );

  if (transport === "http") {
    entries.push(
      action(
        RPC_EXECUTE_ACTION,
        async (ctx, rawEnvelope: unknown, providedToken: string | null) => {
          const expected = (options.meshToken !== undefined && options.meshToken.length > 0) ? ctx.secret(options.meshToken) : null;
          if (!compareTokens(expected, providedToken ?? null)) {
            throw new Error("unauthorized mesh rpc");
          }

          if (!isJsonObject(rawEnvelope)
            || typeof rawEnvelope.actionName !== "string" || rawEnvelope.actionName.length === 0
            || typeof rawEnvelope.callerNodeId !== "string" || rawEnvelope.callerNodeId.length === 0
            || typeof rawEnvelope.deadlineMs !== "number" || !Number.isFinite(rawEnvelope.deadlineMs)) {
            throw new Error("invalid rpc envelope");
          }

          if (!localActionNames.has(rawEnvelope.actionName)) {
            throw new Error("mesh rpc action is not registered");
          }

          if (rawEnvelope.deadlineMs <= Date.now()) {
            throw new Error("mesh rpc deadline exceeded");
          }

          return await ctx.action(rawEnvelope.actionName, rawEnvelope.args);
        },
      ),
    );

    entries.push(createRpcRoute(rpcPath));
  }

  entries.push(
    onStart("__chimpbase.mesh.bootstrap", async (ctx) => {
      await ensureRegistrySchema(ctx);
      await upsertNode(ctx, {
        advertisedUrl,
        metadata,
        nodeId,
        services: serviceEntries,
        startedAtMs,
      });

      const announce: AnnouncePayload = {
        advertisedUrl,
        metadata,
        nodeId,
        services: serviceEntries,
        startedAtMs,
      };
      ctx.pubsub.publish(INFO_EVENT_ANNOUNCE, announce);

      const cutoff = Date.now() - offlineAfterMs;
      const live = await listLiveNodes(ctx, cutoff);
      cache.seed(live.filter((peer) => peer.nodeId !== nodeId));

      for (const svc of services) {
        if (svc.started !== undefined) {
          if (!isServiceStartedHook(svc.started)) {
            throw new TypeError(`service ${svc.name} has an invalid started hook`);
          }
          await svc.started(ctx, buildServiceSelf(svc, nodeId, clientFor(ctx)));
        }
      }

      if (heartbeatMs > 0) {
        heartbeatState.timer = setInterval(() => {
          if (heartbeatState.running !== null) return;
          heartbeatState.running = refreshHeartbeat({
            advertisedUrl,
            cache,
            ctx,
            metadata,
            nodeId,
            offlineAfterMs,
            services: serviceEntries,
            startedAtMs,
          }).finally(() => {
            heartbeatState.running = null;
          });
        }, heartbeatMs);
        if (hasUnref(heartbeatState.timer)) {
          heartbeatState.timer.unref();
        }
      }
    }),
  );

  entries.push(
    onStop("__chimpbase.mesh.shutdown", async (ctx) => {
      if ((heartbeatState.timer !== null)) {
        clearInterval(heartbeatState.timer);
        heartbeatState.timer = null;
      }
      await heartbeatState.running;
      await ctx.action("__chimpbase.mesh.deregister");

      for (const svc of services) {
        if ((svc.stopped !== undefined)) {
          await svc.stopped();
        }
      }
    }),
  );

  entries.push(
    action("__chimpbase.mesh.deregister", async (ctx: ChimpbaseContext) => {
      await deleteNode(ctx, nodeId);
      ctx.pubsub.publish(INFO_EVENT_LEAVE, { nodeId } satisfies LeavePayload);
    }),
  );

  entries.push(
    cron("__chimpbase.mesh.gc", "* * * * *", async (ctx) => {
      const cutoff = Date.now() - gcAfterMs;
      await gcStaleNodes(ctx, cutoff);
    }),
  );

  for (const event of balancedEventSet) {
    entries.push(
      worker<BalancedEnvelope>(balancedWorkerName(event), async (ctx, envelope) => {
        for (const svc of services) {
          const eventEntry = svc.events[envelope.event];
          if (!(eventEntry !== null && eventEntry !== undefined) || !(eventEntry.balanced === true)) {
            continue;
          }

          const meshClient = (ctx as ChimpbaseContext & { mesh?: ChimpbaseMeshClient }).mesh;
          if (!isServiceEventDispatch(eventEntry.handler)) {
            throw new TypeError(`service ${svc.name} has an invalid event handler`);
          }
          await eventEntry.handler(ctx, envelope.payload, buildServiceSelf(svc, nodeId, meshClient));
        }
      }),
    );
  }

  return plugin(
    { dependsOn: options.dependsOn, name: options.name ?? "chimpbase-mesh" },
    ...entries,
  );
}

interface HeartbeatArgs extends UpsertNodeInput {
  cache: MeshPeerCache;
  ctx: ChimpbaseContext;
  offlineAfterMs: number;
}

async function refreshHeartbeat(args: HeartbeatArgs): Promise<void> {
  try {
    await touchHeartbeat(args.ctx, args);
    const live = await listLiveNodes(args.ctx, Date.now() - args.offlineAfterMs);
    args.cache.seed(live.filter((peer) => peer.nodeId !== args.nodeId));
  } catch (error) {
    args.ctx.log.warn("mesh heartbeat failed", {
      error: error instanceof Error ? error.message : String(error),
      nodeId: args.nodeId,
    });
  }
}

function buildServiceEntries(services: readonly ResolvedService[]): NodeServiceEntry[] {
  return services.map((svc) => ({
    actions: Object.keys(svc.actions).map((name) => prefixedActionName(svc.name, svc.version, name)),
    events: Object.keys(svc.events),
    name: svc.name,
    version: svc.version,
  }));
}

function buildServiceRegistrations(
  services: readonly ResolvedService[],
): ChimpbaseRegistrationSource[] {
  const entries: ChimpbaseRegistrationSource[] = [];

  for (const svc of services) {
    for (const [actionName, handler] of Object.entries(svc.actions)) {
      const fullName = prefixedActionName(svc.name, svc.version, actionName);
      entries.push(
        action(fullName, async (ctx: ChimpbaseContext, ...args: unknown[]) => {
          const meshClient = (ctx as ChimpbaseContext & { mesh?: ChimpbaseMeshClient }).mesh;
          const self = buildServiceSelf(svc, meshClient?.nodeId() ?? "", meshClient);
          const actionArgs = args.length <= 1 ? args[0] : args;
          if (!isServiceActionDispatch(handler)) {
            throw new TypeError(`service ${svc.name} has an invalid action handler`);
          }
          return await handler(ctx, actionArgs, self);
        }),
      );
    }

    for (const [eventName, event] of Object.entries(svc.events)) {
      if ((event.balanced === true)) {
        continue;
      }

      entries.push(
        subscription(eventName, async (ctx, payload) => {
          const meshClient = (ctx as ChimpbaseContext & { mesh?: ChimpbaseMeshClient }).mesh;
          const self = buildServiceSelf(svc, meshClient?.nodeId() ?? "", meshClient);
          if (!isServiceEventDispatch(event.handler)) {
            throw new TypeError(`service ${svc.name} has an invalid event handler`);
          }
          await event.handler(ctx, payload, self);
        }, { dispatch: "sync" }),
      );
    }
  }

  return entries;
}

function buildServiceSelf(
  svc: ResolvedService,
  nodeId: string,
  meshClient: ChimpbaseMeshClient | undefined,
): ServiceSelf {
  return {
    call: async <TResult>(
      actionName: string,
      args: unknown,
      result: ChimpbaseValidator<TResult>,
      options?: CallOptions,
    ) => {
      if (!(meshClient !== undefined)) {
        throw new Error("mesh client is not available in this context");
      }
      return await meshClient.call(actionName, args, result, options);
    },
    emit: async (event, payload, options?: EmitOptions) => {
      if (!(meshClient !== undefined)) {
        throw new Error("mesh client is not available in this context");
      }
      await meshClient.emit(event, payload, options);
    },
    methods: svc.methods,
    name: svc.name,
    nodeId,
    settings: svc.settings,
    version: svc.version,
  };
}

function createRpcRoute(rpcPath: string) {
  const handler: ChimpbaseRouteHandler = async (request, env) => {
    const url = new URL(request.url);
    if (url.pathname !== rpcPath) {
      return null;
    }

    if (request.method !== "POST") {
      return new Response("mesh rpc requires POST", { status: 405 });
    }

    const token = request.headers.get(MESH_TOKEN_HEADER);

    let envelope: unknown;
    try {
      envelope = await request.json();
    } catch {
      return new Response(JSON.stringify({ ok: false, error: "invalid json body" }), {
        headers: { "content-type": "application/json" },
        status: 400,
      });
    }

    try {
      const result = await env.action(RPC_EXECUTE_ACTION, envelope, token);
      return new Response(JSON.stringify({ ok: true, result }), {
        headers: { "content-type": "application/json" },
        status: 200,
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      const status = message === "unauthorized mesh rpc" ? 401
        : message === "invalid rpc envelope" ? 400
        : message === "mesh rpc action is not registered" ? 403
        : message === "mesh rpc deadline exceeded" ? 408
        : 500;
      throw new Response(
        JSON.stringify({ error: message, ok: false }),
        { headers: { "content-type": "application/json" }, status },
      );
    }
  };

  return route("__chimpbase.mesh.rpc.route", handler);
}
