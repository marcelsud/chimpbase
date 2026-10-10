import { describe, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { createChimpbase } from "../packages/bun/src/library.ts";
import { createCallDispatcher, type RemoteDispatcher } from "../packages/mesh/src/call.ts";
import { MeshPeerCache } from "../packages/mesh/src/discovery.ts";
import {
  chimpbaseMesh,
  MeshNoAvailableNodeError,
  MeshTimeoutError,
  service,
  type NodeRecord,
} from "../packages/mesh/src/index.ts";
import { onStart, v, type ChimpbaseContext } from "../packages/runtime/index.ts";

const actionName = "v1.identity.get";

async function withContext(
  run: (ctx: ChimpbaseContext) => Promise<void>,
  localResult = "local",
): Promise<void> {
  const projectDir = await mkdtemp(join(tmpdir(), "chimpbase-mesh-routing-"));
  const host = await createChimpbase({
    project: { name: "mesh-routing-test" },
    projectDir,
    storage: { engine: "memory" },
  });
  let ctx: ChimpbaseContext | undefined;
  try {
    host.register(
      chimpbaseMesh({
        services: [service({ name: "identity", actions: { get: () => localResult } })],
        transport: "local-only",
      }),
      onStart("capture-routing-context", (context) => { ctx = context; }),
    );
    const started = await host.start({ serve: false, runWorker: false });
    try {
      if (ctx === undefined) throw new Error("routing context missing");
      await run(ctx);
    } finally {
      await started.stop();
    }
  } finally {
    await host.close();
    await rm(projectDir, { recursive: true, force: true });
  }
}

function peer(
  nodeId: string,
  actions: readonly string[] = [actionName],
  lastHeartbeatMs = Date.now(),
): NodeRecord {
  return {
    advertisedUrl: null,
    lastHeartbeatMs,
    metadata: {},
    nodeId,
    services: [{ actions, events: [], name: "identity", version: 1 }],
    startedAtMs: 0,
  };
}

function dispatcherFor(
  peers: readonly NodeRecord[],
  remoteDispatcher: RemoteDispatcher,
  localAvailable = false,
) {
  const cache = new MeshPeerCache(30_000);
  cache.seed(peers);
  return createCallDispatcher({
    cache,
    defaultRetries: 0,
    defaultStrategy: "local-first",
    defaultTimeoutMs: 1_000,
    localActionNames: new Set(localAvailable ? [actionName] : []),
    localNodeId: "local",
    middleware: [],
    remoteDispatcher,
  });
}

describe("@chimpbase/mesh routing", () => {
  test("local-first retries another remote node and resets exclusions for each call", async () => {
    await withContext(async (ctx) => {
      const visited: string[] = [];
      const dispatcher = dispatcherFor([peer("bad"), peer("healthy")], async ({ peer: target }) => {
        visited.push(target.nodeId);
        if (target.nodeId === "bad") throw new MeshTimeoutError(actionName, target.nodeId, 1_000);
        return "ok";
      });

      for (let i = 0; i < 2; i++) {
        expect(await dispatcher(ctx, actionName, {}, v.string(), {
          retry: { attempts: 2, delayMs: 0 },
        })).toBe("ok");
      }
      expect(visited).toEqual(["bad", "healthy", "bad", "healthy"]);
    });
  });

  test("a remote deadline also makes the next attempt choose another node", async () => {
    await withContext(async (ctx) => {
      const visited: string[] = [];
      const dispatcher = dispatcherFor([peer("slow"), peer("healthy")], async ({ peer: target }) => {
        visited.push(target.nodeId);
        if (target.nodeId === "slow") return await new Promise<never>(() => {});
        return "ok";
      });

      expect(await dispatcher(ctx, actionName, {}, v.string(), {
        retry: { attempts: 1, delayMs: 0 },
        timeoutMs: 5,
      })).toBe("ok");
      expect(visited).toEqual(["slow", "healthy"]);
    });
  });

  test("unpinned retries stop dispatching once every remote candidate has failed", async () => {
    await withContext(async (ctx) => {
      const visited: string[] = [];
      const dispatcher = dispatcherFor([peer("bad-a"), peer("bad-b")], async ({ peer: target }) => {
        visited.push(target.nodeId);
        throw new MeshTimeoutError(actionName, target.nodeId, 1_000);
      });

      await expect(dispatcher(ctx, actionName, {}, v.string(), {
        retry: { attempts: 3, delayMs: 0 },
      })).rejects.toBeInstanceOf(MeshNoAvailableNodeError);
      expect(visited).toEqual(["bad-a", "bad-b"]);
    });
  });

  test("pinned retries keep using the requested remote node", async () => {
    await withContext(async (ctx) => {
      const visited: string[] = [];
      const dispatcher = dispatcherFor([peer("bad"), peer("healthy")], async ({ peer: target }) => {
        visited.push(target.nodeId);
        if (target.nodeId === "bad") throw new MeshTimeoutError(actionName, target.nodeId, 1_000);
        return "ok";
      }, true);

      await expect(dispatcher(ctx, actionName, {}, v.string(), {
        nodeId: "bad",
        retry: { attempts: 2, delayMs: 0 },
      })).rejects.toBeInstanceOf(MeshTimeoutError);
      expect(visited).toEqual(["bad", "bad", "bad"]);
    });
  });

  test("pinned remote targets must be fresh and advertise the requested action", async () => {
    await withContext(async (ctx) => {
      const visited: string[] = [];
      const dispatcher = dispatcherFor([
        peer("stale", [actionName], 0),
        peer("unsupported", []),
        peer("other-action", ["v1.identity.other"]),
        peer("healthy"),
      ], async ({ peer: target }) => {
        visited.push(target.nodeId);
        return "ok";
      }, true);

      for (const nodeId of ["stale", "unsupported", "other-action", "missing"]) {
        await expect(dispatcher(ctx, actionName, {}, v.string(), { nodeId }))
          .rejects.toBeInstanceOf(MeshNoAvailableNodeError);
      }
      expect(visited).toEqual([]);
      expect(await dispatcher(ctx, actionName, {}, v.string(), { nodeId: "healthy" })).toBe("ok");
      expect(visited).toEqual(["healthy"]);
    });
  });

  test("local pins retain the local action capability check", async () => {
    await withContext(async (ctx) => {
      const visited: string[] = [];
      const remote: RemoteDispatcher = async ({ peer: target }) => {
        visited.push(target.nodeId);
        return "remote";
      };
      expect(await dispatcherFor([peer("healthy")], remote, true)(
        ctx, actionName, {}, v.string(), { nodeId: "local" },
      )).toBe("local");
      await expect(dispatcherFor([peer("healthy")], remote)(
        ctx, actionName, {}, v.string(), { nodeId: "local" },
      )).rejects.toBeInstanceOf(MeshNoAvailableNodeError);
      expect(visited).toEqual([]);
    });
  });

  test("round-robin advances independently in dispatchers on different hosts", async () => {
    await withContext(async (ctxA) => {
      await withContext(async (ctxB) => {
        const dispatcherA = dispatcherFor([peer("remote-a")], async () => "remote-a", true);
        const dispatcherB = dispatcherFor([peer("remote-b")], async () => "remote-b", true);
        const resultsA: string[] = [];
        const resultsB: string[] = [];
        for (let i = 0; i < 4; i++) {
          resultsA.push(await dispatcherA(ctxA, actionName, {}, v.string(), { strategy: "round-robin" }));
          resultsB.push(await dispatcherB(ctxB, actionName, {}, v.string(), { strategy: "round-robin" }));
        }
        expect(resultsA).toEqual(["remote-a", "local-a", "remote-a", "local-a"]);
        expect(resultsB).toEqual(["remote-b", "local-b", "remote-b", "local-b"]);
      }, "local-b");
    }, "local-a");
  });
});
