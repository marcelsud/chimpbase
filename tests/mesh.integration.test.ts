import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { Pool } from "pg";

import { chimpbaseMesh, service } from "../packages/mesh/src/index.ts";
import { createChimpbase } from "../packages/bun/src/library.ts";
import { v } from "../packages/runtime/index.ts";

const PG_URL = process.env.CHIMPBASE_TEST_PG_URL;
const describeIfPg = (PG_URL !== undefined && PG_URL.length > 0) ? describe : describe.skip;
function postgresUrl(): string {
  if (PG_URL === undefined || PG_URL.length === 0) {
    throw new Error("PostgreSQL integration URL is unavailable");
  }
  return PG_URL;
}


function waitFor(predicate: () => boolean | Promise<boolean>, timeoutMs = 5000): Promise<void> {
  return new Promise((resolve, reject) => {
    const deadline = Date.now() + timeoutMs;
    const tick = async () => {
      try {
        if (await predicate()) {
          resolve();
          return;
        }
      } catch (error) {
        reject(error);
        return;
      }

      if (Date.now() > deadline) {
        reject(new Error("waitFor timed out"));
        return;
      }

      setTimeout(() => { void tick(); }, 25);
    };
    void tick();
  });
}

async function resetMeshTables(pool: Pool): Promise<void> {
  await pool.query("DROP TABLE IF EXISTS _chimpbase_mesh_nodes");
}

describeIfPg("@chimpbase/mesh (integration — requires CHIMPBASE_TEST_PG_URL)", () => {
  let pool: Pool;

  beforeAll(async () => {
    pool = new Pool({ connectionString: PG_URL });
    await resetMeshTables(pool);
  });

  afterAll(async () => {
    await resetMeshTables(pool);
    await pool.end();
  });

  test("heartbeats stay committed while an unrelated action is active and rolls back", async () => {
    let entered!: () => void;
    let release!: () => void;
    const busy = new Promise<void>((resolve) => { entered = resolve; });
    const blocked = new Promise<void>((resolve) => { release = resolve; });
    const hostA = await createChimpbase({ storage: { engine: "postgres", url: postgresUrl() } });
    const hostB = await createChimpbase({ storage: { engine: "postgres", url: postgresUrl() } });
    hostA.register(chimpbaseMesh({ heartbeatMs: 50, offlineAfterMs: 300, transport: "local-only",
      services: [service({ name: "busy", actions: {
        nodeId: (ctx) => ctx.mesh?.nodeId(),
        pending: async (ctx) => await ctx.kv.get("mesh.pending"),
        hold: async (ctx) => { await ctx.kv.set("mesh.pending", true); entered(); await blocked; throw new Error("rollback request"); },
      } })],
    }));
    hostB.register(chimpbaseMesh({ heartbeatMs: 50, offlineAfterMs: 300, transport: "local-only",
      services: [service({ name: "observer", actions: { peers: (ctx) => ctx.mesh?.peers().map((peer) => peer.nodeId) } })],
    }));
    const startedA = await hostA.start({ serve: false, runWorker: false });
    const startedB = await hostB.start({ serve: false, runWorker: false });
    let running: Promise<unknown> | undefined;
    try {
      const nodeA = v.string().parse((await hostA.executeAction("v1.busy.nodeId")).result);
      const peers = async () => (await hostB.executeAction("v1.observer.peers")).result;
      await waitFor(async () => JSON.stringify(await peers()) === JSON.stringify([nodeA]));
      const heartbeat = async () => Number((await pool.query<{ last_heartbeat_ms: string }>(
        "SELECT last_heartbeat_ms FROM _chimpbase_mesh_nodes WHERE node_id = $1", [nodeA],
      )).rows[0]?.last_heartbeat_ms);
      running = hostA.executeAction("v1.busy.hold").catch((error: unknown) => error);
      await busy;
      const before = await heartbeat();
      await Bun.sleep(650);
      expect(await peers()).toEqual([nodeA]);
      const during = await heartbeat();
      expect(during).toBeGreaterThan(before);
      release();
      expect(await running).toMatchObject({ message: "rollback request" });
      expect(await heartbeat()).toBeGreaterThanOrEqual(during);
      expect((await hostA.executeAction("v1.busy.pending")).result).toBeNull();
    } finally {
      release();
      await running;
      await startedA.stop();
      await startedB.stop();
      await hostA.close();
      await hostB.close();
    }
  });

  test("heartbeats keep peers live and shutdown removes the stopped node", async () => {
    const svcA = service({
      name: "a",
      actions: {
        peers: (ctx) => ctx.mesh?.peers().map((peer) => peer.nodeId),
        nodeId: (ctx) => ctx.mesh?.nodeId(),
      },
    });
    const svcB = service({
      name: "b",
      actions: {
        peers: (ctx) => ctx.mesh?.peers().map((peer) => peer.nodeId),
        nodeId: (ctx) => ctx.mesh?.nodeId(),
      },
    });

    const hostA = await createChimpbase({
      project: { name: "mesh-integ-a" },
      projectDir: process.cwd(),
      storage: { engine: "postgres", url: postgresUrl() },
    });
    const hostB = await createChimpbase({
      project: { name: "mesh-integ-b" },
      projectDir: process.cwd(),
      storage: { engine: "postgres", url: postgresUrl() },
    });

    hostA.register(chimpbaseMesh({
      heartbeatMs: 50,
      offlineAfterMs: 300,
      services: [svcA],
      transport: "local-only",
    }));
    hostB.register(chimpbaseMesh({
      heartbeatMs: 50,
      offlineAfterMs: 300,
      services: [svcB],
      transport: "local-only",
    }));

    const startedA = await hostA.start({ serve: false, runWorker: false });
    const startedB = await hostB.start({ serve: false, runWorker: false });

    let stoppedB = false;
    try {
      const nodeA = (await hostA.executeAction("v1.a.nodeId")).result;
      const nodeB = (await hostB.executeAction("v1.b.nodeId")).result;
      const peersA = async () => (await hostA.executeAction("v1.a.peers")).result;
      const peersB = async () => (await hostB.executeAction("v1.b.peers")).result;
      await waitFor(async () => JSON.stringify(await peersA()) === JSON.stringify([nodeB]));
      expect(await peersB()).toEqual([nodeA]);

      // Cross two offline windows: registry heartbeats must refresh both caches.
      await Bun.sleep(650);
      expect(await peersA()).toEqual([nodeB]);
      expect(await peersB()).toEqual([nodeA]);

      await startedB.stop();
      stoppedB = true;
      expect((await pool.query<{ node_id: string }>(
        "SELECT node_id FROM _chimpbase_mesh_nodes WHERE node_id = $1",
        [nodeB],
      )).rows).toEqual([]);
      await waitFor(async () => JSON.stringify(await peersA()) === "[]");
    } finally {
      await startedA.stop();
      if (!stoppedB) await startedB.stop();
      await hostA.close();
      await hostB.close();
    }
  });
});
