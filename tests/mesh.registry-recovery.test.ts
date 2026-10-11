import { afterEach, expect, spyOn, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { connect, createServer, type Socket } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Pool } from "pg";

import { createChimpbase } from "../packages/bun/src/library.ts";
import { chimpbaseMesh, DEFAULT_RPC_PATH, MESH_TOKEN_HEADER, service } from "../packages/mesh/src/index.ts";
import { gcStaleNodes, listLiveNodes } from "../packages/mesh/src/registry.ts";
import { action, CHIMPBASE_REQUEST_REJECTED_HEADER, onStart, type ChimpbaseContext } from "../packages/runtime/index.ts";
import { readJsonResponse } from "./support/http.ts";

const pgUrl = process.env.CHIMPBASE_TEST_PG_URL;
const testPg = pgUrl ? test : test.skip;
const cleanupDirs: string[] = [];

afterEach(async () => {
  for (const dir of cleanupDirs.splice(0)) await rm(dir, { recursive: true, force: true });
});

async function createHost(engine: "memory" | "sqlite" | "postgres", queryTimeoutMs?: number) {
  const projectDir = await mkdtemp(join(tmpdir(), "chimpbase-mesh-recovery-"));
  cleanupDirs.push(projectDir);
  return await createChimpbase({
    projectDir,
    storage: engine === "postgres" ? { engine, url: pgUrl, queryTimeoutMs } : { engine },
  });
}

function ownNodeId(ctx: ChimpbaseContext): string {
  if (!ctx.mesh) throw new Error("mesh context missing");
  return ctx.mesh.nodeId();
}

async function ownNode(ctx: ChimpbaseContext) {
  return (await listLiveNodes(ctx, 0)).find((node) => node.nodeId === ownNodeId(ctx)) ?? null;
}

async function waitFor(predicate: () => Promise<boolean>): Promise<void> {
  const deadline = Date.now() + 2_000;
  while (Date.now() < deadline) {
    if (await predicate()) return;
    await Bun.sleep(10);
  }
  throw new Error("registry recovery timed out");
}

for (const engine of ["memory", "sqlite", "postgres"] as const) {
  (engine === "postgres" ? testPg : test)(`heartbeat restores its complete row after GC (${engine})`, async () => {
    const host = await createHost(engine);
    let heartbeatInserts = 0;
    const readNode = action("meshRecoveryRead", ownNode);
    const expireNode = action("meshRecoveryExpire", async (ctx) => {
      await ctx.db.query("UPDATE _chimpbase_mesh_nodes SET last_heartbeat_ms = 0 WHERE node_id = ?1", [ownNodeId(ctx)]);
      await gcStaleNodes(ctx, 1);
      return await ownNode(ctx);
    });
    host.register(chimpbaseMesh({
      advertisedUrl: "http://mesh.example:8080",
      heartbeatMs: 25,
      meshToken: "mesh.token",
      meta: { zone: "south" },
      services: [service({
        name: "recovery", version: 2,
        actions: { status: () => "live" },
        events: { notice: () => {} },
        started: (ctx) => {
          ctx.db.query = new Proxy(ctx.db.query, {
            apply(target, thisArg: unknown, args: unknown[]): unknown {
              if (typeof args[0] === "string" && args[0].startsWith("INSERT INTO _chimpbase_mesh_nodes")) {
                heartbeatInserts += 1;
              }
              return Reflect.apply(target, thisArg, args);
            },
          });
        },
      })],
    }), readNode, expireNode);
    try {
      const started = await host.start({ serve: false, runWorker: false });
      try {
        const before = (await host.executeAction(readNode)).result;
        if (!before) throw new Error("startup registry row missing");
        expect((await host.executeAction(expireNode)).result).toBeNull();
        await waitFor(async () => (await host.executeAction(readNode)).result !== null);
        const restored = (await host.executeAction(readNode)).result;
        if (!restored) throw new Error("recovered registry row missing");
        expect(restored).toEqual({ ...before, lastHeartbeatMs: restored.lastHeartbeatMs });
        expect(restored.lastHeartbeatMs).toBeGreaterThanOrEqual(before.lastHeartbeatMs);
        expect(heartbeatInserts).toBe(1);
        await waitFor(async () => ((await host.executeAction(readNode)).result?.lastHeartbeatMs ?? 0) > restored.lastHeartbeatMs);
        expect(heartbeatInserts).toBe(1);
      } finally {
        await started.stop();
      }
    } finally {
      await host.close();
    }
  });
}

testPg("fresh PostgreSQL schema coordination does not lock peers behind slow service initialization", async () => {
  const pool = new Pool({ connectionString: pgUrl });
  const first = await createHost("postgres");
  const second = await createHost("postgres", 150);
  let entered!: () => void;
  let release!: () => void;
  const firstCreatedSchema = new Promise<void>((resolve) => { entered = resolve; });
  const gate = new Promise<void>((resolve) => { release = resolve; });
  first.register(chimpbaseMesh({
    heartbeatMs: 0, transport: "local-only",
    services: [service({ name: "first", started: async () => { entered(); await gate; } })],
  }));
  second.register(chimpbaseMesh({
    heartbeatMs: 0, transport: "local-only", services: [service({ name: "second" })],
  }));
  try {
    await pool.query("DROP TABLE IF EXISTS _chimpbase_mesh_nodes");
    const firstStart = first.start({ serve: false, runWorker: false });
    await firstCreatedSchema;
    let secondStarted: Awaited<ReturnType<typeof second.start>> | undefined;
    try {
      secondStarted = await second.start({ serve: false, runWorker: false });
      expect((await pool.query<{ node_id: string }>("SELECT node_id FROM _chimpbase_mesh_nodes")).rows).toHaveLength(1);
      release();
      await firstStart;
      expect((await pool.query<{ node_id: string }>("SELECT node_id FROM _chimpbase_mesh_nodes")).rows).toHaveLength(2);
    } finally {
      release();
      await (await firstStart).stop();
      await secondStarted?.stop();
    }
  } finally {
    release();
    await first.close();
    await second.close();
    await pool.end();
  }
});

testPg("readiness discovers peers that registered during late initialization without heartbeat refreshes", async () => {
  const first = await createHost("postgres");
  const second = await createHost("postgres");
  const entered = Promise.withResolvers<void>();
  const release = Promise.withResolvers<void>();
  let bootPeers: string[] = [];
  const peers = action("lateStartupPeers", (ctx) => ctx.mesh?.peers().map((peer) => peer.nodeId) ?? []);
  const nodeId = action("lateStartupNodeId", ownNodeId);
  first.register(chimpbaseMesh({
    heartbeatMs: 0, transport: "local-only",
    services: [service({
      name: "lateFirst", started: (ctx) => { bootPeers = ctx.mesh?.peers().map((peer) => peer.nodeId) ?? []; },
    })],
  }), peers, onStart("late-dependency", async () => { entered.resolve(); await release.promise; }));
  second.register(chimpbaseMesh({
    heartbeatMs: 0, transport: "local-only", services: [service({ name: "lateSecond" })],
  }), nodeId);
  const starting = first.start({ serve: false, runWorker: false });
  let secondStarted: Awaited<ReturnType<typeof second.start>> | undefined;
  try {
    await entered.promise;
    secondStarted = await second.start({ serve: false, runWorker: false });
    const secondId = (await second.executeAction(nodeId)).result;
    expect(bootPeers).not.toContain(secondId);
    release.resolve();
    await starting;
    expect((await first.executeAction(peers)).result).toContain(secondId);
  } finally {
    release.resolve();
    await (await starting).stop();
    await secondStarted?.stop();
    await first.close();
    await second.close();
  }
});

testPg("mesh waits for late initialization before advertising, heartbeating and accepting external RPC", async () => {
  const pool = new Pool({ connectionString: pgUrl });
  const projectDir = await mkdtemp(join(tmpdir(), "chimpbase-mesh-readiness-"));
  cleanupDirs.push(projectDir);
  const host = await createChimpbase({
    projectDir, storage: { engine: "postgres", url: pgUrl },
    secrets: { get: () => "readiness-token" },
  });
  const entered = Promise.withResolvers<void>();
  const release = Promise.withResolvers<void>();
  let initialized = false;
  const server = Bun.serve({ hostname: "127.0.0.1", port: 0, fetch: async (request) =>
    (await host.executeRoute(request)).response ?? new Response("not found", { status: 404 }) });
  const advertisedUrl = `http://127.0.0.1:${server.port}`;
  host.register(chimpbaseMesh({
    advertisedUrl, heartbeatMs: 15, meshToken: "mesh.token",
    services: [service({
      name: "readiness", started: () => { initialized = true; },
      actions: { ready: () => initialized },
    })],
  }), onStart("late-initialization", async () => { entered.resolve(); await release.promise; }));
  const rpc = async () => await fetch(`${advertisedUrl}${DEFAULT_RPC_PATH}`, {
    method: "POST", headers: { [MESH_TOKEN_HEADER]: "readiness-token" },
    body: JSON.stringify({ actionName: "v1.readiness.ready", callerNodeId: "probe", deadlineMs: Date.now() + 1_000 }),
  });
  const row = async () => (await pool.query<{ last_heartbeat_ms: string }>(
    "SELECT last_heartbeat_ms FROM _chimpbase_mesh_nodes WHERE advertised_url = $1", [advertisedUrl],
  )).rows;
  let starting: ReturnType<typeof host.start> | undefined;
  try {
    const beforeStart = await rpc();
    expect(beforeStart.status).toBe(200);
    expect(await readJsonResponse<{ ok: boolean; result: boolean }>(beforeStart)).toEqual({ ok: true, result: false });
    starting = host.start({ serve: false, runWorker: false });
    await entered.promise;
    expect(await row()).toEqual([]);
    const blocked = await rpc();
    expect(blocked.status).toBe(503);
    expect(blocked.headers.get(CHIMPBASE_REQUEST_REJECTED_HEADER)).toBe("1");
    await Bun.sleep(60);
    expect(await row()).toEqual([]);
    release.resolve();
    const started = await starting;
    expect(await row()).toHaveLength(1);
    const before = Number((await row())[0]?.last_heartbeat_ms);
    await waitFor(async () => Number((await row())[0]?.last_heartbeat_ms) > before);
    const ready = await rpc();
    expect(ready.status).toBe(200);
    expect(await readJsonResponse<{ ok: boolean; result: boolean }>(ready)).toEqual({ ok: true, result: true });
    await started.stop();
    await Bun.sleep(40);
    expect(await row()).toEqual([]);
  } finally {
    release.resolve();
    if (starting) await (await starting).stop();
    server.stop(true);
    await host.close();
    await pool.end();
  }
});

testPg("heartbeat recovery commits independently of an action rollback", async () => {
  const pool = new Pool({ connectionString: pgUrl });
  const host = await createHost("postgres");
  let entered!: () => void;
  let release!: () => void;
  const busy = new Promise<void>((resolve) => { entered = resolve; });
  const gate = new Promise<void>((resolve) => { release = resolve; });
  const readNode = action("meshRecoveryRead", ownNode);
  const hold = action("meshRecoveryHold", async (ctx) => {
    await ctx.kv.set("mesh.recovery.pending", true);
    entered();
    await gate;
    throw new Error("rollback recovery action");
  });
  host.register(chimpbaseMesh({
    heartbeatMs: 25, transport: "local-only", services: [service({ name: "recovery" })],
  }), readNode, hold);
  let running: Promise<unknown> | undefined;
  try {
    const started = await host.start({ serve: false, runWorker: false });
    try {
      const before = (await host.executeAction(readNode)).result;
      if (!before) throw new Error("startup registry row missing");
      running = host.executeAction(hold).catch((error: unknown) => error);
      await busy;
      await pool.query("DELETE FROM _chimpbase_mesh_nodes WHERE node_id = $1", [before.nodeId]);
      await waitFor(async () => (await pool.query<{ node_id: string }>("SELECT node_id FROM _chimpbase_mesh_nodes WHERE node_id = $1", [before.nodeId])).rows.length === 1);
      release();
      expect(await running).toMatchObject({ message: "rollback recovery action" });
      const recovered = (await host.executeAction(readNode)).result;
      if (!recovered) throw new Error("recovered registry row missing after rollback");
      expect(recovered).toEqual({ ...before, lastHeartbeatMs: recovered.lastHeartbeatMs });
      expect((await pool.query<{ key: string }>("SELECT key FROM _chimpbase_kv WHERE key = 'mesh.recovery.pending'")).rows).toEqual([]);
    } finally {
      release();
      await running;
      await started.stop();
    }
  } finally {
    release();
    await host.close();
    await pool.end();
  }
});

testPg("shutdown releases every service resource when PostgreSQL connections are unavailable", async () => {
  if (!pgUrl) throw new Error("PostgreSQL test URL missing");
  const upstreamUrl = new URL(pgUrl);
  const sockets = new Set<Socket>();
  const proxy = createServer((client) => {
    const upstream = connect({ host: upstreamUrl.hostname, port: Number(upstreamUrl.port || 5432) });
    sockets.add(client); sockets.add(upstream);
    client.pipe(upstream); upstream.pipe(client);
    client.on("error", () => upstream.destroy());
    upstream.on("error", () => client.destroy());
    client.on("close", () => { sockets.delete(client); upstream.destroy(); });
    upstream.on("close", () => { sockets.delete(upstream); client.destroy(); });
  });
  await new Promise<void>((resolve) => proxy.listen(0, "127.0.0.1", resolve));
  const address = proxy.address();
  if (address === null || typeof address === "string") throw new Error("proxy address missing");
  const url = new URL(pgUrl); url.hostname = "127.0.0.1"; url.port = String(address.port);
  const projectDir = await mkdtemp(join(tmpdir(), "chimpbase-mesh-shutdown-"));
  cleanupDirs.push(projectDir);
  const host = await createChimpbase({
    projectDir, storage: { engine: "postgres", url: url.toString(), connectionTimeoutMs: 100, queryTimeoutMs: 100 },
  });
  const stopped: string[] = [];
  const cleanupFailure = new Error("first resource cleanup failed");
  const reported = spyOn(console, "error").mockImplementation(() => {});
  host.register(chimpbaseMesh({
    heartbeatMs: 20, transport: "local-only", services: [
      service({ name: "first", stopped: () => { stopped.push("first"); throw cleanupFailure; } }),
      service({ name: "second", stopped: () => { stopped.push("second"); } }),
    ],
  }), action("unavailableDatabase", () => "unexpected invocation"));
  try {
    const started = await host.start({ serve: false, runWorker: false });
    for (const socket of sockets) socket.destroy();
    await new Promise<void>((resolve) => proxy.close(() => resolve()));
    await expect(host.executeAction("unavailableDatabase")).rejects.toThrow();
    await started.stop();
    expect(stopped).toEqual(["first", "second"]);
    const reports: readonly (readonly unknown[])[] = reported.mock.calls;
    const shutdownReports = reports.filter((call) => call[0] === 'onStop hook "__chimpbase.mesh.shutdown" failed:');
    expect(shutdownReports).toHaveLength(1);
    const error: unknown = shutdownReports[0]?.[1];
    expect(error).toBeInstanceOf(AggregateError);
    if (!(error instanceof AggregateError)) throw new Error("expected aggregate shutdown error");
    const causes: readonly unknown[] = error.errors;
    expect(causes).toHaveLength(2);
    expect(causes[0]).toBeInstanceOf(Error);
    expect(causes[1]).toBe(cleanupFailure);
  } finally {
    reported.mockRestore();
    await host.close();
    for (const socket of sockets) socket.destroy();
    if (proxy.listening) await new Promise<void>((resolve) => proxy.close(() => resolve()));
  }
});
