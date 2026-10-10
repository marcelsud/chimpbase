import { describe, expect, test } from "bun:test";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { createServer, connect, type Socket } from "node:net";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { Pool } from "pg";

import { createChimpbase } from "../packages/bun/src/library.ts";
import { normalizeProjectConfig } from "../packages/core/index.ts";
import { createDefaultChimpbasePlatformShim } from "../packages/core/host.ts";
import { chimpbaseMesh, service } from "../packages/mesh/src/index.ts";
import { createPostgresEngineAdapter, openPostgresPool } from "../packages/postgres/src/index.ts";
import { CHIMPBASE_REQUEST_REJECTED_HEADER, v } from "../packages/runtime/index.ts";

const PG_URL = process.env.CHIMPBASE_TEST_PG_URL;
const describeIfPg = PG_URL ? describe : describe.skip;
function postgresUrl(): string {
  if (!PG_URL) throw new Error("PostgreSQL integration URL is unavailable");
  return PG_URL;
}
async function waitFor(check: () => boolean | Promise<boolean>): Promise<void> {
  const deadline = performance.now() + 3_000;
  while (performance.now() < deadline) {
    if (await check()) return;
    await Bun.sleep(10);
  }
  throw new Error("waitFor timed out");
}

describeIfPg("PostgreSQL mesh connection resilience", () => {
  test("saturated written transactions allow callbacks and heartbeats, and reject excess admission", async () => {
    const prefix = `resilience.${crypto.randomUUID()}`;
    const a = await createChimpbase({ storage: { engine: "postgres", url: postgresUrl() }, secrets: { get: () => "test" } });
    const b = await createChimpbase({ storage: { engine: "postgres", url: postgresUrl() }, secrets: { get: () => "test" } });
    const pool = new Pool({ connectionString: postgresUrl() });
    const serverA = Bun.serve({ hostname: "127.0.0.1", port: 0, fetch: async (req) => (await a.executeRoute(req)).response! });
    const serverB = Bun.serve({ hostname: "127.0.0.1", port: 0, fetch: async (req) => (await b.executeRoute(req)).response! });
    const entered = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    let starts = 0;
    const nameA = `a${crypto.randomUUID().replaceAll("-", "")}`;
    const nameB = `b${crypto.randomUUID().replaceAll("-", "")}`;
    a.register(chimpbaseMesh({ advertisedUrl: serverA.url.origin, meshToken: "TOKEN", heartbeatMs: 20, defaultTimeoutMs: 2_000,
      services: [service({ name: nameA, actions: {
        leaf: async (ctx, id: number) => { await ctx.kv.set(`${prefix}.leaf.${id}`, id); return id; },
        relay: async (ctx, id: number) => {
          await ctx.kv.set(`${prefix}.root.${id}`, id);
          if (++starts === 10) entered.resolve();
          await release.promise;
          return await ctx.mesh!.call(`v1.${nameB}.callback`, id, v.number());
        },
        peers: (ctx) => ctx.mesh!.peers(), nodeId: (ctx) => ctx.mesh!.nodeId(),
      } })] }));
    b.register(chimpbaseMesh({ advertisedUrl: serverB.url.origin, meshToken: "TOKEN", heartbeatMs: 20, defaultTimeoutMs: 2_000,
      services: [service({ name: nameB, actions: {
        callback: async (ctx, id: number) => {
          await ctx.kv.set(`${prefix}.callback.${id}`, id);
          return await ctx.mesh!.call(`v1.${nameA}.leaf`, id, v.number());
        },
      } })] }));
    const startedA = await a.start({ serve: false, runWorker: false });
    const startedB = await b.start({ serve: false, runWorker: false });
    let running: Promise<unknown>[] = [];
    try {
      await waitFor(async () => ((await a.executeAction(`v1.${nameA}.peers`)).result as unknown[]).length > 0);
      const nodeId = (await a.executeAction(`v1.${nameA}.nodeId`)).result;
      running = Array.from({ length: 10 }, (_, id) => a.executeAction(`v1.${nameA}.relay`, id));
      await entered.promise;
      const heartbeat = async () => Number((await pool.query<{ last_heartbeat_ms: string }>(
        "SELECT last_heartbeat_ms FROM _chimpbase_mesh_nodes WHERE node_id = $1", [nodeId],
      )).rows[0]?.last_heartbeat_ms);
      const before = await heartbeat();
      await waitFor(async () => (await heartbeat()) > before);
      await expect(a.executeAction(`v1.${nameA}.peers`)).rejects.toThrow("capacity exceeded");
      const rejected = await a.executeRoute(new Request("http://localhost/excess"));
      expect(rejected.response?.status).toBe(503);
      expect(rejected.response?.headers.get(CHIMPBASE_REQUEST_REJECTED_HEADER)).toBe("1");
      release.resolve();
      expect((await Promise.all(running)).map((outcome) => (outcome as { result: number }).result)).toEqual(
        Array.from({ length: 10 }, (_, id) => id),
      );
      expect(Number((await pool.query<{ count: string }>(
        "SELECT COUNT(*) FROM _chimpbase_kv WHERE key LIKE $1", [`${prefix}.%`],
      )).rows[0]?.count)).toBe(30);
      await startedA.stop();
      const stopping = await a.executeRoute(new Request("http://localhost/__chimpbase/mesh/rpc", { method: "POST" }));
      expect(stopping.response?.status).toBe(503);
      expect(stopping.response?.headers.get(CHIMPBASE_REQUEST_REJECTED_HEADER)).toBe("1");
    } finally {
      release.resolve();
      await Promise.allSettled(running);
      await startedA.stop();
      await startedB.stop();
      serverA.stop(true);
      serverB.stop(true);
      await a.close();
      await b.close();
      await pool.query("DELETE FROM _chimpbase_kv WHERE key LIKE $1", [`${prefix}.%`]);
      await pool.end();
    }
  });

  test("pool acquisition expires instead of waiting forever", async () => {
    const pool = openPostgresPool(normalizeProjectConfig({ storage: {
      engine: "postgres", url: postgresUrl(), connectionTimeoutMs: 50,
    } }), 1);
    const client = await pool.connect();
    try {
      await expect(pool.query("SELECT 1")).rejects.toThrow("timeout exceeded when trying to connect");
      expect(pool.waitingCount).toBe(0);
    } finally {
      client.release();
      await pool.end();
    }
  });

  test("a blackholed database query is bounded, discarded, and reconnects", async () => {
    const upstreamUrl = new URL(postgresUrl());
    const sockets = new Set<Socket>();
    let blackhole = false;
    const proxy = createServer((client) => {
      const upstream = connect({ host: upstreamUrl.hostname, port: Number(upstreamUrl.port || 5432) });
      sockets.add(client); sockets.add(upstream);
      client.on("data", (chunk) => { if (!blackhole) upstream.write(chunk); });
      upstream.on("data", (chunk) => { if (!blackhole) client.write(chunk); });
      client.on("error", () => upstream.destroy());
      upstream.on("error", () => client.destroy());
      client.on("close", () => { sockets.delete(client); upstream.destroy(); });
      upstream.on("close", () => { sockets.delete(upstream); client.destroy(); });
    });
    await new Promise<void>((resolveListen) => proxy.listen(0, "127.0.0.1", resolveListen));
    const address = proxy.address();
    if (address === null || typeof address === "string") throw new Error("proxy address missing");
    const url = new URL(postgresUrl()); url.hostname = "127.0.0.1"; url.port = String(address.port);
    const pool = openPostgresPool(normalizeProjectConfig({ storage: {
      engine: "postgres", url: url.toString(), connectionTimeoutMs: 500, queryTimeoutMs: 80,
    } }));
    const adapter = createPostgresEngineAdapter(pool, createDefaultChimpbasePlatformShim());
    try {
      await adapter.beginTransaction();
      blackhole = true;
      await expect(adapter.query("SELECT 1", [], v.object({}))).rejects.toThrow("Query read timeout");
      await adapter.rollbackTransaction();
      expect(pool.totalCount).toBe(0);
      blackhole = false;
      await adapter.beginTransaction();
      expect(await adapter.query("SELECT 2 AS value", [], v.object({ value: v.number() }))).toEqual([{ value: 2 }]);
      await adapter.commitTransaction();
      await pool.end();
    } finally {
      blackhole = false;
      await adapter.rollbackTransaction();
      if (!pool.ended) await pool.end();
      for (const socket of sockets) socket.destroy();
      await new Promise<void>((resolveClose) => proxy.close(() => resolveClose()));
    }
  });

  test("a timed-out SQL batch cannot commit late writes or escape rollback when caught", async () => {
    const key = `timeout.${crypto.randomUUID()}`;
    const host = await createChimpbase({ storage: {
      engine: "postgres", url: postgresUrl(), queryTimeoutMs: 80,
    } });
    const pool = new Pool({ connectionString: postgresUrl() });
    host.action("timeout", async (ctx) => {
      await ctx.kv.set(key, "before");
      try { await ctx.db.query("SELECT pg_sleep(1)"); } catch {}
      await ctx.kv.set(key, "after");
    });
    try {
      await expect(host.executeAction("timeout")).rejects.toThrow();
      expect((await pool.query<{ value_json: unknown }>("SELECT value_json FROM _chimpbase_kv WHERE key = $1", [key])).rows).toEqual([]);
      host.action("recover", () => "ready");
      expect((await host.executeAction("recover")).result).toBe("ready");
    } finally {
      await host.close();
      await pool.query("DELETE FROM _chimpbase_kv WHERE key = $1", [key]);
      await pool.end();
    }
  });

  test("the server expires idle transaction locks even when no client query is pending", async () => {
    const key = `idle.${crypto.randomUUID()}`;
    const observer = new Pool({ connectionString: postgresUrl() });
    const pool = openPostgresPool(normalizeProjectConfig({ storage: {
      engine: "postgres", url: postgresUrl(), queryTimeoutMs: 80,
    } }));
    const adapter = createPostgresEngineAdapter(pool, createDefaultChimpbasePlatformShim());
    try {
      await adapter.beginTransaction();
      await adapter.kvSet(key, "uncommitted");
      await waitFor(() => pool.totalCount === 0);
      await expect(adapter.query("SELECT 1", [], v.object({}))).rejects.toThrow("idle-in-transaction timeout");
      await adapter.rollbackTransaction();
      expect((await observer.query<{ value_json: unknown }>("SELECT value_json FROM _chimpbase_kv WHERE key = $1", [key])).rows).toEqual([]);
      await adapter.beginTransaction();
      await adapter.kvSet(key, "recovered");
      await adapter.commitTransaction();
    } finally {
      await adapter.rollbackTransaction();
      await pool.end();
      await observer.query("DELETE FROM _chimpbase_kv WHERE key = $1", [key]);
      await observer.end();
    }
  });

  test("an idle backend disconnect does not terminate Node and the pool reconnects", async () => {
    const directory = await mkdtemp(join(tmpdir(), "chimpbase-pg-node-"));
    try {
      const source = join(directory, "entry.ts");
      await writeFile(source, `
        import { openPostgresPool } from ${JSON.stringify(resolve(import.meta.dir, "../packages/postgres/src/index.ts"))};
        import { Pool } from ${JSON.stringify(resolve(import.meta.dir, "../node_modules/pg/lib/index.js"))};
        const config = { storage: { url: ${JSON.stringify(postgresUrl())} } };
        const pool = openPostgresPool(config);
        const admin = new Pool({ connectionString: config.storage.url });
        const result = await pool.query("SELECT pg_backend_pid() AS pid");
        await admin.query("SELECT pg_terminate_backend($1)", [result.rows[0].pid]);
        await new Promise(resolve => setTimeout(resolve, 100));
        const recovered = await pool.query("SELECT 1 AS value");
        if (recovered.rows[0].value !== 1) throw new Error("pool failed to reconnect");
        await pool.end(); await admin.end();
        console.log("reconnected");
      `);
      const result = await Bun.build({ entrypoints: [source], target: "node", outdir: directory });
      if (!result.success) throw new Error(result.logs.join("\n"));
      const subprocess = Bun.spawn(["node", result.outputs[0]!.path], { stdout: "pipe", stderr: "pipe" });
      const [stdout, stderr, status] = await Promise.all([
        new Response(subprocess.stdout).text(), new Response(subprocess.stderr).text(), subprocess.exited,
      ]);
      expect({ status, stderr }).toEqual({ status: 0, stderr: "" });
      expect(stdout.trim()).toBe("reconnected");
    } finally { await rm(directory, { recursive: true, force: true }); }
  });
});
