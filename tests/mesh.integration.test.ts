import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { Pool } from "pg";

import { chimpbaseMesh, service } from "../packages/mesh/src/index.ts";
import { createChimpbase } from "../packages/bun/src/library.ts";

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

  test("two hosts see each other in the registry via LISTEN/NOTIFY", async () => {
    const svcA = service({
      name: "a",
      actions: { ping: async () => "pong-a" },
    });
    const svcB = service({
      name: "b",
      actions: { ping: async () => "pong-b" },
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
      heartbeatMs: 500,
      offlineAfterMs: 3_000,
      services: [svcA],
      transport: "local-only",
    }));
    hostB.register(chimpbaseMesh({
      heartbeatMs: 500,
      offlineAfterMs: 3_000,
      services: [svcB],
      transport: "local-only",
    }));

    const startedA = await hostA.start({ serve: false, runWorker: false });
    const startedB = await hostB.start({ serve: false, runWorker: false });

    try {
      await waitFor(async () => {
        const rows = await pool.query<{ node_id: string }>(
          "SELECT node_id FROM _chimpbase_mesh_nodes",
        );
        return rows.rows.length >= 2;
      });

      const rows = await pool.query<{ node_id: string }>(
        "SELECT node_id FROM _chimpbase_mesh_nodes",
      );
      expect(rows.rows.length).toBeGreaterThanOrEqual(2);
    } finally {
      await startedA.stop();
      await startedB.stop();
      await hostA.close();
      await hostB.close();
    }
  });
});
