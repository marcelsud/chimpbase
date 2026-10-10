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

async function waitFor(predicate: () => boolean): Promise<void> {
  const deadline = performance.now() + 4_000;
  while (performance.now() < deadline) {
    if (predicate()) return;
    await Bun.sleep(5);
  }
  throw new Error("waitFor timed out");
}

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

  test("a failed polling callback retries the same event", async () => {
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

  test("delivers an earlier event that commits after a later ID, including rollback gaps", async () => {
    const reader = new Pool({ connectionString: PG_URL, max: 1 });
    const writer = new Pool({ connectionString: PG_URL });
    await ensurePostgresInternalTables(writer);
    const names = ["slow", "fast", "rolled-back"].map((name) => `${name}.${crypto.randomUUID()}`);
    const received: string[] = [];
    const slow = await writer.connect();
    const bus = new PostgresPollingEventBus({ pool: reader, pollIntervalMs: 10 });
    bus.start(async (events) => { received.push(...events.map((event) => event.name)); });
    try {
      await reader.query("SELECT 1");
      await slow.query("BEGIN");
      await slow.query("INSERT INTO _chimpbase_events (event_name, payload_json) VALUES ($1, '{}')", [names[0]]);
      await writer.query("INSERT INTO _chimpbase_events (event_name, payload_json) VALUES ($1, '{}')", [names[1]]);
      await waitFor(() => received.includes(names[1]));
      await slow.query("COMMIT");
      await waitFor(() => received.includes(names[0]));
      await slow.query("BEGIN");
      await slow.query("INSERT INTO _chimpbase_events (event_name, payload_json) VALUES ($1, '{}')", [names[2]]);
      await slow.query("ROLLBACK");
      await Bun.sleep(50);
      expect(received).toEqual([names[1], names[0]]);
    } finally {
      bus.stop();
      await slow.query("ROLLBACK");
      slow.release();
      await writer.query("DELETE FROM _chimpbase_events WHERE event_name = ANY($1::text[])", [names]);
      await reader.end();
      await writer.end();
    }
  });

  test("delivers a transaction already in flight at startup without replaying committed history", async () => {
    const reader = new Pool({ connectionString: PG_URL, max: 1 });
    const writer = new Pool({ connectionString: PG_URL });
    await ensurePostgresInternalTables(writer);
    const names = ["startup-inflight", "history"].map((name) => `${name}.${crypto.randomUUID()}`);
    const received: string[] = [];
    const slow = await writer.connect();
    const bus = new PostgresPollingEventBus({ pool: reader, pollIntervalMs: 10 });
    try {
      await slow.query("BEGIN");
      await slow.query("INSERT INTO _chimpbase_events (event_name, payload_json) VALUES ($1, '{}')", [names[0]]);
      await writer.query("INSERT INTO _chimpbase_events (event_name, payload_json) VALUES ($1, '{}')", [names[1]]);
      bus.start(async (events) => { received.push(...events.map((event) => event.name)); });
      await reader.query("SELECT 1");
      await slow.query("COMMIT");
      await waitFor(() => received.length > 0);
      await Bun.sleep(50);
      expect(received).toEqual([names[0]]);
    } finally {
      bus.stop();
      await slow.query("ROLLBACK");
      slow.release();
      await writer.query("DELETE FROM _chimpbase_events WHERE event_name = ANY($1::text[])", [names]);
      await reader.end();
      await writer.end();
    }
  });

  test("drains frozen windows larger than a page while newer transactions keep arriving", async () => {
    const reader = new Pool({ connectionString: PG_URL, max: 1 });
    const writer = new Pool({ connectionString: PG_URL });
    await ensurePostgresInternalTables(writer);
    const name = `pages.${crypto.randomUUID()}`;
    const received: number[] = [];
    const slow = await writer.connect();
    let appended = 0;
    const append = async (offset: number, count: number) => {
      await writer.query(
        "INSERT INTO _chimpbase_events (event_name, payload_json) SELECT $1, jsonb_build_object('number', number) FROM generate_series($2::int, $3::int) number",
        [name, offset, offset + count - 1],
      );
    };
    const bus = new PostgresPollingEventBus({ pool: reader, pollIntervalMs: 10 });
    bus.start(async (events) => {
      for (const event of events) {
        if (event.name !== name) continue;
        received.push((event.payload as { number: number }).number);
        if (received.length === 1) await slow.query("COMMIT");
        if (received.length % 100 === 1 && appended < 3) {
          await append(250 + appended * 125, 125);
          appended += 1;
        }
      }
    });
    try {
      await reader.query("SELECT 1");
      await slow.query("BEGIN");
      await slow.query("INSERT INTO _chimpbase_events (event_name, payload_json) VALUES ($1, '{\"number\":-1}')", [name]);
      await append(0, 250);
      await waitFor(() => received.length === 626);
      await Bun.sleep(50);
      expect(received).toEqual([
        ...Array.from({ length: 250 }, (_, index) => index), -1,
        ...Array.from({ length: 375 }, (_, index) => index + 250),
      ]);
    } finally {
      bus.stop();
      await slow.query("ROLLBACK");
      slow.release();
      await writer.query("DELETE FROM _chimpbase_events WHERE event_name = $1", [name]);
      await reader.end();
      await writer.end();
    }
  });

  test("isolates poison events, bounds retries, and does not replay healthy deliveries", async () => {
    const pool = new Pool({ connectionString: PG_URL, max: 1 });
    await ensurePostgresInternalTables(pool);
    const names = ["poison", "healthy"].map((name) => `${name}.${crypto.randomUUID()}`);
    const received: string[] = [];
    let failures = 0;
    const errors = spyOn(console, "error").mockImplementation(() => {});
    const bus = new PostgresPollingEventBus({ pool, pollIntervalMs: 10 });
    bus.start(async (events) => {
      for (const event of events) {
        if (event.name === names[0]) {
          failures += 1;
          throw new Error("poison event");
        }
        received.push(event.name);
      }
    });
    try {
      await pool.query("SELECT 1");
      await pool.query("INSERT INTO _chimpbase_events (event_name, payload_json) VALUES ($1, '{}'), ($2, '{}')", names);
      await waitFor(() => failures === 3);
      await pool.query("INSERT INTO _chimpbase_events (event_name, payload_json) VALUES ($1, '{}')", [names[1]]);
      await waitFor(() => received.length === 2);
      await Bun.sleep(50);
      expect(failures).toBe(3);
      expect(received).toEqual([names[1], names[1]]);
      expect(errors).toHaveBeenCalledTimes(3);
      expect(errors.mock.calls[2]?.[1] as unknown).toMatchObject({ eventName: names[0], attempts: 3, exhausted: true });
    } finally {
      bus.stop();
      errors.mockRestore();
      await pool.query("DELETE FROM _chimpbase_events WHERE event_name = ANY($1::text[])", [names]);
      await pool.end();
    }
  });

  test("a failed callback finishing after stop cannot carry retries into the restarted listener", async () => {
    const pool = new Pool({ connectionString: PG_URL, max: 1 });
    await ensurePostgresInternalTables(pool);
    const names = ["stopped", "restarted"].map((name) => `${name}.${crypto.randomUUID()}`);
    const entered = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    const received: string[] = [];
    const bus = new PostgresPollingEventBus({ pool, pollIntervalMs: 10 });
    bus.start(async () => {
      entered.resolve();
      await release.promise;
      throw new Error("stopped callback");
    });
    try {
      await pool.query("SELECT 1");
      await pool.query("INSERT INTO _chimpbase_events (event_name, payload_json) VALUES ($1, '{}')", [names[0]]);
      await entered.promise;
      bus.stop();
      bus.start(async (events) => { received.push(...events.map((event) => event.name)); });
      await pool.query("SELECT 1");
      release.resolve();
      await pool.query("INSERT INTO _chimpbase_events (event_name, payload_json) VALUES ($1, '{}')", [names[1]]);
      await waitFor(() => received.includes(names[1]));
      await Bun.sleep(50);
      expect(received).toEqual([names[1]]);
    } finally {
      release.resolve();
      bus.stop();
      await pool.query("DELETE FROM _chimpbase_events WHERE event_name = ANY($1::text[])", [names]);
      await pool.end();
    }
  });
});
