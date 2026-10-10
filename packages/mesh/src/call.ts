import { AsyncLocalStorage } from "node:async_hooks";

import type { ChimpbaseContext, ChimpbaseValidator } from "@chimpbase/runtime";

import type { MeshPeerCache } from "./discovery.ts";
import type {
  CallOptions,
  LoadBalanceStrategy,
  MeshCallFn,
  MeshCallMiddleware,
  NodeRecord,
} from "./types.ts";
import {
  MeshCallError,
  MeshNoAvailableNodeError,
  MeshTimeoutError,
} from "./types.ts";

export interface CallResolverOptions {
  cache: MeshPeerCache;
  defaultRetries: number;
  defaultStrategy: LoadBalanceStrategy;
  defaultTimeoutMs: number;
  localActionNames: ReadonlySet<string>;
  localMetadata?: Readonly<Record<string, unknown>>;
  localNodeId: string;
  middleware: readonly MeshCallMiddleware[];
  remoteDispatcher: RemoteDispatcher | null;
}

export type RemoteDispatcher = (params: {
  actionName: string;
  args: unknown;
  deadlineMs: number;
  peer: NodeRecord;
}) => Promise<unknown>;

export function createCallDispatcher(options: CallResolverOptions) {
  const contexts = new AsyncLocalStorage<ChimpbaseContext>();
  const roundRobinCounters = new Map<string, number>();
  const core = async <TResult>(
    ctx: ChimpbaseContext,
    actionName: string,
    args: unknown,
    resultValidator: ChimpbaseValidator<TResult>,
    callOpts: CallOptions,
  ): Promise<TResult> => {
    const strategy = callOpts.strategy ?? options.defaultStrategy;
    const timeoutMs = callOpts.timeoutMs ?? options.defaultTimeoutMs;
    const retryAttempts = callOpts.retry?.attempts ?? options.defaultRetries;
    const retryDelayMs = callOpts.retry?.delayMs ?? 100;
    const excludedRemoteNodeIds = new Set<string>();

    let localAttempt = false;
    const attempt = async (): Promise<unknown> => {
      localAttempt = false;
      const target = pickTarget({
        actionName,
        cache: options.cache,
        excludedRemoteNodeIds,
        localActionNames: options.localActionNames,
        localMetadata: options.localMetadata,
        localNodeId: options.localNodeId,
        pinnedNodeId: callOpts.nodeId,
        roundRobinCounters,
        strategy,
      });

      if (!(target !== null)) {
        throw new MeshNoAvailableNodeError(actionName);
      }

      let result: unknown;
      if (target.kind === "local") {
        localAttempt = true;
        result = await withTimeout(
          invokeLocal(ctx, actionName, args),
          timeoutMs,
          actionName,
          options.localNodeId,
          true,
        );
      } else {
        if (!(options.remoteDispatcher !== null)) {
          throw new MeshCallError(
            actionName,
            target.peer.nodeId,
            `remote dispatch is disabled for action: ${actionName}`,
          );
        }

        const deadlineMs = Date.now() + timeoutMs;
        try {
          result = await withTimeout(
            options.remoteDispatcher({
              actionName,
              args,
              deadlineMs,
              peer: target.peer,
            }),
            timeoutMs,
            actionName,
            target.peer.nodeId,
          );
        } catch (error) {
          excludedRemoteNodeIds.add(target.peer.nodeId);
          throw error;
        }
      }

      return result;
    };

    let lastError: Error | null = null;
    for (let i = 0; i <= retryAttempts; i++) {
      let result: unknown;
      try {
        result = await attempt();
      } catch (error) {
        lastError = error instanceof Error ? error : new Error(String(error));
        // Local failures share the caller's transaction and must reach its rollback boundary.
        if (localAttempt) throw lastError;
        const retryable = error instanceof MeshNoAvailableNodeError
          || error instanceof MeshTimeoutError
          || (error instanceof MeshCallError && error.retryable);
        if (!retryable) break;
        if (i < retryAttempts) {
          await delay(retryDelayMs);
        }
        continue;
      }
      return resultValidator.parse(result, `mesh action ${actionName} result`);
    }

    if ((callOpts.fallback !== undefined)) {
      const fallbackResult = await callOpts.fallback(lastError ?? new Error("mesh call failed"));
      return resultValidator.parse(fallbackResult, `mesh action ${actionName} fallback result`);
    }

    throw lastError ?? new MeshCallError(actionName, null, "mesh call failed");
  };

  let wrapped: MeshCallFn = (actionName, args, result, opts) =>
    core(currentCtx(), actionName, args, result, opts);
  for (let i = options.middleware.length - 1; i >= 0; i--) {
    wrapped = options.middleware[i](wrapped);
  }

  const currentCtx = (): ChimpbaseContext => {
    const ctx = contexts.getStore();
    if (ctx === undefined) {
      throw new MeshCallError("", null, "mesh dispatcher invoked without an active context");
    }
    return ctx;
  };

  return async <TResult>(
    ctx: ChimpbaseContext,
    actionName: string,
    args: unknown,
    result: ChimpbaseValidator<TResult>,
    callOpts: CallOptions,
  ): Promise<TResult> => {
    return await contexts.run(ctx, () => wrapped(actionName, args, result, callOpts));
  };
}

interface PickTargetArgs {
  actionName: string;
  cache: MeshPeerCache;
  excludedRemoteNodeIds: ReadonlySet<string>;
  localActionNames: ReadonlySet<string>;
  localMetadata?: Readonly<Record<string, unknown>>;
  localNodeId: string;
  pinnedNodeId?: string;
  roundRobinCounters: Map<string, number>;
  strategy: LoadBalanceStrategy;
}

type PickResult =
  | { kind: "local"; nodeId: string }
  | { kind: "remote"; peer: NodeRecord };

function pickTarget(args: PickTargetArgs): PickResult | null {
  const localAvailable = args.localActionNames.has(args.actionName);

  if ((args.pinnedNodeId !== undefined && args.pinnedNodeId.length > 0)) {
    if (args.pinnedNodeId === args.localNodeId) {
      return localAvailable ? { kind: "local", nodeId: args.localNodeId } : null;
    }

    const pinned = args.cache.findByAction(args.actionName)
      .find((peer) => peer.nodeId === args.pinnedNodeId);
    if (pinned === undefined) {
      return null;
    }

    return { kind: "remote", peer: pinned };
  }

  const peers = args.cache.findByAction(args.actionName).filter((peer) =>
    peer.nodeId !== args.localNodeId && !args.excludedRemoteNodeIds.has(peer.nodeId),
  );

  if (args.strategy === "local-first") {
    if (localAvailable) {
      return { kind: "local", nodeId: args.localNodeId };
    }

    return peers.length === 0 ? null : { kind: "remote", peer: peers[0] };
  }

  const candidates: Array<PickResult> = [];
  if (localAvailable) {
    candidates.push({ kind: "local", nodeId: args.localNodeId });
  }
  for (const peer of peers) {
    candidates.push({ kind: "remote", peer });
  }

  if (candidates.length === 0) {
    return null;
  }

  switch (args.strategy) {
    case "random":
      return candidates[Math.floor(Math.random() * candidates.length)];
    case "round-robin": {
      const counter = (args.roundRobinCounters.get(args.actionName) ?? 0) + 1;
      args.roundRobinCounters.set(args.actionName, counter);
      return candidates[counter % candidates.length];
    }
    case "cpu":
      return candidates.reduce((best, current) => {
        const bestLoad = loadOf(best, args.localMetadata);
        const currentLoad = loadOf(current, args.localMetadata);
        return currentLoad < bestLoad ? current : best;
      });
    default:
      return candidates[0];
  }
}

function loadOf(result: PickResult, localMetadata: Readonly<Record<string, unknown>> = {}): number {
  const cpu = (result.kind === "local" ? localMetadata : result.peer.metadata)["cpuLoad"];
  return typeof cpu === "number" ? cpu : 0.5;
}

function invokeLocal(ctx: ChimpbaseContext, actionName: string, args: unknown): Promise<unknown> {
  return ctx.action(actionName, args);
}

async function withTimeout<T>(
  promise: Promise<T>,
  timeoutMs: number,
  actionName: string,
  nodeId: string | null,
  waitForSettlement = false,
): Promise<T> {
  if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) {
    return await promise;
  }

  let timer: ReturnType<typeof setTimeout> | null = null;
  const timeout = new Promise<T>((_, reject) => {
    timer = setTimeout(() => {
      reject(new MeshTimeoutError(actionName, nodeId, timeoutMs));
    }, timeoutMs);
  });

  try {
    return await Promise.race([promise, timeout]);
  } catch (error) {
    // A local action cannot be cancelled; keep its transaction alive through late writes.
    if (waitForSettlement) await promise.catch(() => {});
    throw error;
  } finally {
    if ((timer !== null)) {
      clearTimeout(timer);
    }
  }
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
