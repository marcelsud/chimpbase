import {
  action,
  contextExtension,
  cron,
  isArrayValue,
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
} from "@chimpbase/runtime";

import {
  createCallDispatcher,
  type RemoteDispatcher,
} from "./call.ts";
import {
  INFO_EVENT_ANNOUNCE,
  INFO_EVENT_HEARTBEAT,
  INFO_EVENT_LEAVE,
  MeshPeerCache,
  type AnnouncePayload,
  type HeartbeatPayload,
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
  type RpcEnvelope,
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
  const metaBase = options.meta ?? {};

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
  const heartbeatState: { timer: ReturnType<typeof setInterval> | null } = { timer: null };

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
    localNodeId: nodeId,
    middleware,
    remoteDispatcher,
  });

  const clientFor = (ctx: ChimpbaseContext): ChimpbaseMeshClient => ({
    call: async <TResult = unknown>(actionName: string, args?: unknown, opts?: CallOptions) => {
      currentToken = (options.meshToken !== undefined && options.meshToken.length > 0) ? ctx.secret(options.meshToken) : null;
      return await dispatcher<TResult>(ctx, actionName, args, opts ?? {});
    },
    emit: async (event, payload, opts?: EmitOptions) => {
      await meshEmit(ctx, event, payload, opts ?? {}, balancedEventSet);
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
      if (!(payload?.nodeId.length > 0) || payload.nodeId === nodeId) {
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
    }, { idempotent: false }),
  );

  entries.push(
    subscription<LeavePayload>(INFO_EVENT_LEAVE, async (_ctx, payload) => {
      if (!(payload?.nodeId.length > 0) || payload.nodeId === nodeId) {
        return;
      }

      cache.remove(payload.nodeId);
    }, { idempotent: false }),
  );

  entries.push(
    subscription<HeartbeatPayload>(INFO_EVENT_HEARTBEAT, async (_ctx, payload) => {
      if (!(payload?.nodeId.length > 0) || payload.nodeId === nodeId) {
        return;
      }

      cache.touch(payload.nodeId, payload.lastHeartbeatMs ?? Date.now(), payload.metadata ?? {});
    }, { idempotent: false }),
  );

  if (transport === "http") {
    entries.push(
      action(
        RPC_EXECUTE_ACTION,
        async (ctx, rawEnvelope: RpcEnvelope, providedToken: string | null) => {
          const expected = (options.meshToken !== undefined && options.meshToken.length > 0) ? ctx.secret(options.meshToken) : null;
          if (!compareTokens(expected, providedToken ?? null)) {
            throw new Error("unauthorized mesh rpc");
          }

          if (!(rawEnvelope !== null && rawEnvelope !== undefined) || typeof rawEnvelope.actionName !== "string") {
            throw new Error("invalid rpc envelope");
          }

          const invocationArgs = isArrayValue(rawEnvelope.args) ? rawEnvelope.args : [rawEnvelope.args];
          return await ctx.action<unknown[], unknown>(rawEnvelope.actionName, ...invocationArgs);
        },
      ),
    );

    entries.push(createRpcRoute(rpcPath));
  }

  entries.push(
    onStart("__chimpbase.mesh.bootstrap", async (ctx) => {
      await ensureRegistrySchema(ctx);
      const metadata = { ...metaBase };
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
        if ((svc.started !== undefined)) {
          const started = svc.started as (ctx: ChimpbaseContext, self: ServiceSelf) => unknown;
          await started(ctx, buildServiceSelf(svc, nodeId, clientFor(ctx)));
        }
      }

      if (heartbeatMs > 0) {
        heartbeatState.timer = setInterval(() => {
          void emitHeartbeat({
            ctx,
            metadata,
            nodeId,
          });
        }, heartbeatMs);
        (heartbeatState.timer as unknown as { unref?: () => void }).unref?.();
      }
    }),
  );

  entries.push(
    onStop("__chimpbase.mesh.shutdown", async () => {
      if ((heartbeatState.timer !== null)) {
        clearInterval(heartbeatState.timer);
        heartbeatState.timer = null;
      }

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
          await (eventEntry.handler as ServiceEventDispatch)(
            ctx,
            envelope.payload,
            buildServiceSelf(svc, nodeId, meshClient),
          );
        }
      }),
    );
  }

  return plugin(
    { dependsOn: options.dependsOn, name: options.name ?? "chimpbase-mesh" },
    ...entries,
  );
}

interface HeartbeatArgs {
  ctx: ChimpbaseContext;
  metadata: Record<string, unknown>;
  nodeId: string;
}

async function emitHeartbeat(args: HeartbeatArgs): Promise<void> {
  try {
    const heartbeatAt = await touchHeartbeat(args.ctx, args.nodeId, args.metadata);
    if (heartbeatAt > 0) {
      args.ctx.pubsub.publish(INFO_EVENT_HEARTBEAT, {
        lastHeartbeatMs: heartbeatAt,
        metadata: args.metadata,
        nodeId: args.nodeId,
      } satisfies HeartbeatPayload);
    }
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
          return await (handler as ServiceActionDispatch)(ctx, actionArgs, self);
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
          await (event.handler as ServiceEventDispatch)(ctx, payload, self);
        }),
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
    call: async <TResult = unknown>(actionName: string, args?: unknown, options?: CallOptions) => {
      if (!(meshClient !== undefined)) {
        throw new Error("mesh client is not available in this context");
      }
      return await meshClient.call<TResult>(actionName, args, options);
    },
    emit: async (event, payload, options?: EmitOptions) => {
      if (!(meshClient !== undefined)) {
        throw new Error("mesh client is not available in this context");
      }
      await meshClient.emit(event, payload, options);
    },
    methods: svc.methods as Record<string, never>,
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

    let envelope: RpcEnvelope;
    try {
      envelope = (await request.json()) as RpcEnvelope;
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
      const status = message === "unauthorized mesh rpc" ? 401 : 500;
      return new Response(
        JSON.stringify({ error: message, ok: false }),
        { headers: { "content-type": "application/json" }, status },
      );
    }
  };

  return route("__chimpbase.mesh.rpc.route", handler);
}
