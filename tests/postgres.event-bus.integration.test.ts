import { describe, expect, spyOn, test } from "bun:test";
import { Pool } from "pg";

import {
  ChimpbaseEngine,
  createChimpbaseRegistry,
  createDefaultChimpbasePlatformShim,
  type ChimpbaseEventBusCallback,
  type ChimpbaseEventRecord,
} from "../packages/core/index.ts";
import {
  createPostgresEngineAdapter,
  ensurePostgresInternalTables,
  PostgresPollingEventBus,
} from "../packages/postgres/src/index.ts";

const PG_URL = process.env.CHIMPBASE_TEST_PG_URL;
const describeIfPg = PG_URL ? describe : describe.skip;

describeIfPg("PostgreSQL event delivery", () => {
  test("concurrent consumers reserve one subscription delivery before running its handler", async () => {
    const pool = new Pool({ connectionString: PG_URL });
    const name = `subscription.${crypto.randomUUID()}`;
    await ensurePostgresInternalTables(pool);
    const { rows: [row] } = await pool.query<{ id: number }>(
      "INSERT INTO _chimpbase_events (event_name, payload_json) VALUES ($1, '{}') RETURNING id::double precision AS id", [name],
    );
    const events = [{ id: row.id, name, payload: {}, payloadJson: "{}" }];
    const entered = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    let calls = 0;
    const consumers = Array.from({ length: 2 }, () => {
      const platform = createDefaultChimpbasePlatformShim();
      const registry = createChimpbaseRegistry();
      registry.subscriptions.set(name, [{
        name, idempotent: true,
        handler: async (ctx) => {
          calls += 1;
          await ctx.kv.set(name, calls);
          entered.resolve();
          await release.promise;
        },
      }]);
      let callback: ChimpbaseEventBusCallback | undefined;
      const engine = new ChimpbaseEngine({
        adapter: createPostgresEngineAdapter(pool, platform), platform, registry,
        eventBus: { publish: async () => {}, start(handler) { callback = handler; }, stop() {} },
        secrets: { get: () => null }, subscriptions: { dispatch: "sync" },
        telemetry: { minLevel: "error", persist: { log: false, metric: false, trace: false } },
        worker: { leaseMs: 30_000, maxAttempts: 5, retryDelayMs: 0 },
      });
      engine.startEventBus();
      return { engine, deliver: async () => {
        if (callback === undefined) throw new Error("bus did not start");
        await callback(events);
      } };
    });
    const deliveries: Promise<void>[] = [];
    try {
      deliveries.push(consumers[0].deliver());
      await entered.promise;
      deliveries.push(consumers[1].deliver());
      await Bun.sleep(30);
      expect(calls).toBe(1);
      release.resolve();
      await Promise.all(deliveries);
      expect(calls).toBe(1);
      expect((await pool.query<{ value_json: number }>("SELECT value_json FROM _chimpbase_kv WHERE key = $1", [name])).rows)
        .toEqual([{ value_json: 1 }]);
    } finally {
      release.resolve();
      await Promise.allSettled(deliveries);
      for (const consumer of consumers) consumer.engine.stopEventBus();
      await pool.query("DELETE FROM _chimpbase_kv WHERE key = ANY($1::text[])", [[name, `_chimpbase.sub.seen:${row.id}:${name}`]]);
      await pool.query("DELETE FROM _chimpbase_events WHERE id = $1", [row.id]);
      await pool.end();
    }
  });

  test("publishing a local event preserves an earlier unseen peer event and filters only local IDs", async () => {
    const pool = new Pool({ connectionString: PG_URL, max: 1 });
    await ensurePostgresInternalTables(pool);
    const bus = new PostgresPollingEventBus({ pool, pollIntervalMs: 200 });
    const delivered = Promise.withResolvers<ChimpbaseEventRecord[]>();
    bus.start(async (events) => { delivered.resolve(events); });
    // A second query on this single-connection pool completes after initialization.
    await pool.query("SELECT 1");
    const names = [`peer.${crypto.randomUUID()}`, `local.${crypto.randomUUID()}`];
    const ids: number[] = [];
    try {
      for (const name of names) {
        const { rows: [row] } = await pool.query<{ id: number }>(
          "INSERT INTO _chimpbase_events (event_name, payload_json) VALUES ($1, '{}') RETURNING id::double precision AS id", [name],
        );
        ids.push(row.id);
      }
      await bus.publish([{ id: ids[1], name: names[1], payload: {}, payloadJson: "{}" }]);
      const events = await delivered.promise;
      expect(events.map((event) => event.name)).toEqual([names[0]]);
    } finally {
      bus.stop();
      await pool.query("DELETE FROM _chimpbase_events WHERE id = ANY($1::bigint[])", [ids]);
      await pool.end();
    }
  });

  test("a failed polling callback retries the same batch before advancing its cursor", async () => {
    const pool = new Pool({ connectionString: PG_URL, max: 1 });
    await ensurePostgresInternalTables(pool);
    const bus = new PostgresPollingEventBus({ pool, pollIntervalMs: 20 });
    const retried = Promise.withResolvers<void>();
    const batches: (number | undefined)[][] = [];
    const errors = spyOn(console, "error").mockImplementation(() => {});
    bus.start(async (events) => {
      batches.push(events.map((event) => event.id));
      if (batches.length === 1) throw new Error("retry delivery");
      retried.resolve();
    });
    await pool.query("SELECT 1");
    let eventId: number | undefined;
    try {
      const { rows: [row] } = await pool.query<{ id: number }>(
        "INSERT INTO _chimpbase_events (event_name, payload_json) VALUES ($1, '{}') RETURNING id::double precision AS id",
        [`retry.${crypto.randomUUID()}`],
      );
      eventId = row.id;
      await retried.promise;
      expect(batches).toEqual([[eventId], [eventId]]);
      expect(errors).toHaveBeenCalledTimes(1);
    } finally {
      bus.stop();
      errors.mockRestore();
      if (eventId !== undefined) await pool.query("DELETE FROM _chimpbase_events WHERE id = $1", [eventId]);
      await pool.end();
    }
  });
});
