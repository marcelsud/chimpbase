import { afterEach, describe, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { chimpbaseMesh, MeshConfigurationError, service } from "../packages/mesh/src/index.ts";
import { createChimpbase } from "../packages/bun/src/library.ts";
import { onStart, v } from "../packages/runtime/index.ts";
import { upsertNode } from "../packages/mesh/src/registry.ts";
import { INFO_EVENT_ANNOUNCE } from "../packages/mesh/src/discovery.ts";

const cleanupDirs: string[] = [];

afterEach(async () => {
  while (cleanupDirs.length > 0) {
    const dir = cleanupDirs.pop();
    if ((dir !== undefined && dir.length > 0)) {
      await rm(dir, { recursive: true, force: true });
    }
  }
});

async function createMeshHost() {
  const projectDir = await mkdtemp(join(tmpdir(), "chimpbase-mesh-registry-"));
  cleanupDirs.push(projectDir);

  return await createChimpbase({
    project: { name: "mesh-registry-test" },
    projectDir,
    storage: { engine: "memory" },
  });
}

async function waitFor(predicate: () => Promise<boolean>): Promise<void> {
  const deadline = Date.now() + 2_000;
  while (Date.now() < deadline) {
    if (await predicate()) return;
    await Bun.sleep(10);
  }
  throw new Error("waitFor timed out");
}

describe("@chimpbase/mesh registry", () => {
  test("rejects invalid numeric mesh configuration at registration", () => {
    for (const [name, values] of Object.entries({
      heartbeatMs: [-1, NaN, Infinity, 2_147_483_648],
      offlineAfterMs: [0, -1, 0.5, NaN, Infinity, Number.MAX_SAFE_INTEGER + 1, Number.MAX_VALUE],
      gcAfterMs: [0, -1, 0.5, NaN, Infinity, Number.MAX_SAFE_INTEGER + 1, Number.MAX_VALUE],
      defaultRetries: [-1, 0.5, NaN, Infinity, Number.MAX_SAFE_INTEGER + 1],
      defaultTimeoutMs: [0, -1, NaN, Infinity, 2_147_483_648],
      rpcBodyTimeoutMs: [0, -1, NaN, Infinity, 2_147_483_648],
      rpcMaxBodyBytes: [0, -1, 0.5, NaN, Infinity, Number.MAX_SAFE_INTEGER + 1],
    })) {
      for (const value of values) {
        const configure = () => chimpbaseMesh({
          services: [service({ name: "numeric" })], transport: "local-only", [name]: value,
        });
        expect(configure).toThrow(MeshConfigurationError);
        expect(configure).toThrow(name);
      }
    }
  });

  test("allows disabled heartbeats, zero retries, timer boundaries, and longer registry windows", () => {
    const options = {
      services: [service({ name: "numeric" })], transport: "local-only" as const,
      heartbeatMs: 0, defaultRetries: 0, defaultTimeoutMs: 2_147_483_647,
      rpcBodyTimeoutMs: 2_147_483_647, rpcMaxBodyBytes: Number.MAX_SAFE_INTEGER,
      offlineAfterMs: 2_147_483_648, gcAfterMs: 2_147_483_648,
    };
    expect(() => chimpbaseMesh(options)).not.toThrow();
    expect(() => chimpbaseMesh({ ...options, heartbeatMs: 2_147_483_647, defaultRetries: Number.MAX_SAFE_INTEGER })).not.toThrow();
    expect(() => chimpbaseMesh({ ...options, offlineAfterMs: Number.MAX_SAFE_INTEGER, gcAfterMs: Number.MAX_SAFE_INTEGER })).not.toThrow();
    expect(() => chimpbaseMesh({ ...options, heartbeatMs: 0.5, defaultTimeoutMs: 0.5, rpcBodyTimeoutMs: 0.5 })).not.toThrow();
  });

  test("a later ready hook failure stops mesh heartbeats and releases service resources", async () => {
    const host = await createMeshHost();
    let running = false;
    host.register(chimpbaseMesh({
      heartbeatMs: 10, transport: "local-only",
      services: [service({
        name: "resource", started: () => { running = true; }, stopped: () => { running = false; },
        actions: { nodes: async (ctx) => await ctx.db.query("SELECT node_id FROM _chimpbase_mesh_nodes") },
      })],
    }), onStart("ready-failure", () => { throw new Error("readiness failed"); }, { phase: "ready" }));
    try {
      await expect(host.start({ serve: false, runWorker: false })).rejects.toThrow("readiness failed");
      expect(running).toBe(false);
      await Bun.sleep(40);
      expect((await host.executeAction("v1.resource.nodes")).result).toEqual([]);
    } finally {
      await host.close();
    }
  });

  test("delayed announcements expire from their original timestamps and cannot replace newer peers", async () => {
    const host = await createMeshHost();
    const offlineAfterMs = 1_000;
    host.register(chimpbaseMesh({
      heartbeatMs: 0, offlineAfterMs, transport: "local-only",
      services: [service({ name: "observer", actions: {
        announce: (ctx, payload: unknown) => { ctx.pubsub.publish(INFO_EVENT_ANNOUNCE, payload); },
        peers: (ctx) => ctx.mesh?.peers(),
      } })],
    }));
    const started = await host.start({ serve: false, runWorker: false });
    try {
      const now = Date.now();
      const node = { nodeId: "announced-peer", advertisedUrl: "http://peer.test", metadata: {}, services: [], startedAtMs: now - 60_000 };
      for (const payload of [node, { ...node, lastHeartbeatMs: now - offlineAfterMs - 1 }, { ...node, lastHeartbeatMs: null, startedAtMs: null }]) {
        await host.executeAction("v1.observer.announce", payload);
        expect((await host.executeAction("v1.observer.peers")).result).toEqual([]);
      }
      await host.executeAction("v1.observer.announce", { ...node, lastHeartbeatMs: now - 100, metadata: { revision: "new" } });
      await host.executeAction("v1.observer.announce", { ...node, lastHeartbeatMs: now - 200, metadata: { revision: "old" } });
      expect((await host.executeAction("v1.observer.peers")).result).toEqual([
        { ...node, lastHeartbeatMs: now - 100, metadata: { revision: "new" } },
      ]);
      await host.executeAction("v1.observer.announce", { ...node, nodeId: "legacy-peer", startedAtMs: now - 100 });
      expect((await host.executeAction("v1.observer.peers")).result).toMatchObject([
        { nodeId: "announced-peer", lastHeartbeatMs: now - 100 },
        { nodeId: "legacy-peer", lastHeartbeatMs: now - 100 },
      ]);
    } finally {
      await started.stop();
      await host.close();
    }
  });

  test("advertises the node row on start and removes it on stop", async () => {
    const host = await createMeshHost();
    try {
      const svc = service({
        name: "alpha",
        actions: {
          nodes: async (ctx) => await ctx.db.query(
            "SELECT node_id FROM _chimpbase_mesh_nodes",
            [],
            v.object({ node_id: v.string() }),
          ),
        },
      });

      host.register(chimpbaseMesh({ services: [svc], transport: "local-only" }));

      const started = await host.start({ serve: false, runWorker: false });
      try {
        expect((await host.executeAction("v1.alpha.nodes")).result).toHaveLength(1);
      } finally {
        await started.stop();
      }
      expect((await host.executeAction("v1.alpha.nodes")).result).toEqual([]);
    } finally {
      await host.close();
    }
  });

  test("shutdown waits for an unrelated action rollback before removing its registry row", async () => {
    const host = await createMeshHost();
    let entered!: () => void;
    let release!: () => void;
    const busy = new Promise<void>((resolve) => { entered = resolve; });
    const blocked = new Promise<void>((resolve) => { release = resolve; });
    host.register(chimpbaseMesh({
      heartbeatMs: 0, transport: "local-only",
      services: [service({ name: "busy", actions: {
        hold: async (ctx) => { await ctx.kv.set("pending", true); entered(); await blocked; throw new Error("rollback request"); },
        nodes: async (ctx) => await ctx.db.query("SELECT node_id FROM _chimpbase_mesh_nodes"),
        pending: async (ctx) => await ctx.kv.get("pending"),
      } })],
    }));
    const started = await host.start({ serve: false, runWorker: false });
    let stopping: Promise<void> | undefined;
    let running: Promise<unknown> | undefined;
    let stopped = false;
    try {
      running = host.executeAction("v1.busy.hold").catch((error: unknown) => error);
      await busy;
      stopping = started.stop().then(() => { stopped = true; });
      await Bun.sleep(30);
      expect(stopped).toBe(false);
      release();
      expect(await running).toMatchObject({ message: "rollback request" });
      await stopping;
      expect((await host.executeAction("v1.busy.nodes")).result).toEqual([]);
      expect((await host.executeAction("v1.busy.pending")).result).toBeNull();
    } finally {
      release();
      await running;
      await (stopping ?? started.stop());
      await host.close();
    }
  });

  test("heartbeats discover, expire, and rediscover peers from registry rows", async () => {
    const host = await createMeshHost();
    try {
      const svc = service({
        name: "observer",
        actions: {
          peers: (ctx) => ctx.mesh?.peers().map((peer) => peer.nodeId),
          refreshPeer: async (ctx) => await upsertNode(ctx, {
            advertisedUrl: null,
            metadata: {},
            nodeId: "peer-without-events",
            services: [],
            startedAtMs: Date.now(),
          }),
        },
      });
      host.register(chimpbaseMesh({
        heartbeatMs: 20,
        offlineAfterMs: 150,
        services: [svc],
        transport: "local-only",
      }));
      const started = await host.start({ serve: false, runWorker: false });
      try {
        const peers = async () => (await host.executeAction("v1.observer.peers")).result;
        expect(await peers()).toEqual([]);
        await host.executeAction("v1.observer.refreshPeer");
        await waitFor(async () => JSON.stringify(await peers()) === '["peer-without-events"]');
        await waitFor(async () => JSON.stringify(await peers()) === "[]");
        await host.executeAction("v1.observer.refreshPeer");
        await waitFor(async () => JSON.stringify(await peers()) === '["peer-without-events"]');
      } finally {
        await started.stop();
      }
    } finally {
      await host.close();
    }
  });

  test("slow heartbeats do not overlap and shutdown waits for them", async () => {
    const host = await createMeshHost();
    let releaseHeartbeat = () => {};
    const heartbeatGate = new Promise<void>((resolve) => { releaseHeartbeat = resolve; });
    let heartbeatUpdates = 0;
    let serviceStopped = false;
    const svc = service({
      name: "slow",
      started: (ctx) => {
        ctx.db.query = new Proxy(ctx.db.query, {
          apply(target, thisArg: unknown, args: unknown[]): unknown {
            if (typeof args[0] === "string" && args[0].startsWith("UPDATE _chimpbase_mesh_nodes")) {
              heartbeatUpdates += 1;
              return heartbeatGate.then((): unknown => {
                return Reflect.apply(target, thisArg, args);
              });
            }
            return Reflect.apply(target, thisArg, args);
          },
        });
      },
      stopped: () => { serviceStopped = true; },
    });
    host.register(chimpbaseMesh({ heartbeatMs: 20, services: [svc], transport: "local-only" }));
    const started = await host.start({ serve: false, runWorker: false });
    let stopping: Promise<void> | undefined;
    try {
      await waitFor(async () => heartbeatUpdates > 0);
      await Bun.sleep(70);
      expect(heartbeatUpdates).toBe(1);
      stopping = started.stop();
      await Bun.sleep(30);
      expect(serviceStopped).toBe(false);
      releaseHeartbeat();
      await stopping;
      expect(serviceStopped).toBe(true);
      await Bun.sleep(50);
      expect(heartbeatUpdates).toBe(1);
    } finally {
      releaseHeartbeat();
      await (stopping ?? started.stop());
      await host.close();
    }
  });

  test("balanced event is dispatched via queue worker and runs exactly once per emission", async () => {
    const host = await createMeshHost();
    try {
      let handled = 0;
      const svc = service({
        name: "orders",
        events: {
          "order.paid": {
            balanced: true,
            handler: async () => {
              handled += 1;
            },
          },
        },
        actions: {
          emitPaid: async (ctx) => {
            if (!(ctx.mesh !== undefined)) throw new Error("mesh missing");
            await ctx.mesh.emit("order.paid", { id: "o1" }, { balanced: true });
          },
        },
      });

      host.register(chimpbaseMesh({ services: [svc], transport: "local-only" }));

      const started = await host.start({ serve: false, runWorker: false });
      try {
        await host.executeAction("v1.orders.emitPaid");
        await host.processNextQueueJob();
        expect(handled).toBe(1);
      } finally {
        await started.stop();
      }
    } finally {
      await host.close();
    }
  });

  test("broadcast event delivers via pubsub", async () => {
    const host = await createMeshHost();
    try {
      let received: unknown = null;
      const svc = service({
        name: "news",
        events: {
          "news.published": async (_ctx, payload: { title: string }) => {
            received = payload;
          },
        },
        actions: {
          publish: async (ctx, args: { title: string }) => {
            if (!(ctx.mesh !== undefined)) throw new Error("mesh missing");
            await ctx.mesh.emit("news.published", args);
          },
        },
      });

      host.register(chimpbaseMesh({ services: [svc], transport: "local-only" }));

      const started = await host.start({ serve: false, runWorker: false });
      try {
        await host.executeAction("v1.news.publish", [{ title: "hello" }]);
        expect(received).toEqual({ title: "hello" });
      } finally {
        await started.stop();
      }
    } finally {
      await host.close();
    }
  });

  test("ctx.mesh.peers is accessible and node id is stable across calls", async () => {
    const host = await createMeshHost();
    try {
      let firstNodeId = "";
      let secondNodeId = "";

      const svc = service({
        name: "introspect",
        actions: {
          first: async (ctx) => {
            firstNodeId = ctx.mesh?.nodeId() ?? "";
            return firstNodeId;
          },
          second: async (ctx) => {
            secondNodeId = ctx.mesh?.nodeId() ?? "";
            return secondNodeId;
          },
        },
      });

      host.register(chimpbaseMesh({ services: [svc], transport: "local-only" }));

      const started = await host.start({ serve: false, runWorker: false });
      try {
        await host.executeAction("v1.introspect.first");
        await host.executeAction("v1.introspect.second");
        expect(firstNodeId.length).toBeGreaterThan(0);
        expect(firstNodeId).toBe(secondNodeId);
      } finally {
        await started.stop();
      }
    } finally {
      await host.close();
    }
  });
});
