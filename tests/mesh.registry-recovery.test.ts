import { afterEach, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Pool } from "pg";

import { createChimpbase } from "../packages/bun/src/library.ts";
import { chimpbaseMesh, service } from "../packages/mesh/src/index.ts";
import { gcStaleNodes, listLiveNodes } from "../packages/mesh/src/registry.ts";
import { action, type ChimpbaseContext } from "../packages/runtime/index.ts";

const pgUrl = process.env.CHIMPBASE_TEST_PG_URL;
const testPg = pgUrl ? test : test.skip;
const cleanupDirs: string[] = [];

afterEach(async () => {
  for (const dir of cleanupDirs.splice(0)) await rm(dir, { recursive: true, force: true });
});

async function createHost(engine: "memory" | "sqlite" | "postgres") {
  const projectDir = await mkdtemp(join(tmpdir(), "chimpbase-mesh-recovery-"));
  cleanupDirs.push(projectDir);
  return await createChimpbase({
    projectDir,
    storage: engine === "postgres" ? { engine, url: pgUrl } : { engine },
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

testPg("fresh PostgreSQL registry serializes concurrent mesh startup", async () => {
  const pool = new Pool({ connectionString: pgUrl });
  const first = await createHost("postgres");
  const second = await createHost("postgres");
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
    const secondStart = second.start({ serve: false, runWorker: false });
    await Bun.sleep(50);
    release();
    const outcomes = await Promise.allSettled([firstStart, secondStart]);
    try {
      expect(outcomes.map((outcome) => outcome.status)).toEqual(["fulfilled", "fulfilled"]);
      expect((await pool.query<{ node_id: string }>("SELECT node_id FROM _chimpbase_mesh_nodes")).rows).toHaveLength(2);
    } finally {
      for (const outcome of outcomes) {
        if (outcome.status === "fulfilled") await outcome.value.stop();
      }
    }
  } finally {
    release();
    await first.close();
    await second.close();
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
