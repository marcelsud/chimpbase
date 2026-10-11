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
import { action } from "../packages/runtime/index.ts";

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
  test("polling before a local COMMIT acknowledgement suppresses only the local event", async () => {
    const pool = new Pool({ connectionString: PG_URL });
    const reader = new Pool({ connectionString: PG_URL, max: 1 });
    await ensurePostgresInternalTables(pool);
    const names = ["local", "peer"].map((name) => `${name}.${crypto.randomUUID()}`);
    const platform = createDefaultChimpbasePlatformShim();
    const registry = createChimpbaseRegistry();
    const adapter = createPostgresEngineAdapter(pool, platform);
    const bus = new PostgresPollingEventBus({ pool: reader, pollIntervalMs: 5 });
    const committed = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    const commit = adapter.commitTransaction.bind(adapter);
    adapter.commitTransaction = async () => {
      await commit();
      adapter.commitTransaction = commit;
      committed.resolve();
      await release.promise;
    };
    const calls: string[] = [];
    for (const name of names) registry.subscriptions.set(name, [{
      name, dispatch: "sync", idempotent: false, handler: () => { calls.push(name); },
    }]);
    registry.actions.set("emit", action("emit", (ctx) => { ctx.pubsub.publish(names[0], {}); }));
    const observed: string[] = [];
    const start = bus.start.bind(bus);
    bus.start = (callback) => start(async (events, ack) => {
      observed.push(...events.map((event) => event.name));
      await callback(events, ack);
    });
    const engine = new ChimpbaseEngine({
      adapter, eventBus: bus, platform, registry, secrets: { get: () => null },
      subscriptions: { dispatch: "sync" },
      telemetry: { minLevel: "error", persist: { log: false, metric: false, trace: false } },
      worker: { leaseMs: 30_000, maxAttempts: 5, retryDelayMs: 0 },
    });
    let publishing: Promise<unknown> = Promise.resolve();
    const deliveries: Promise<void>[] = [];
    engine.startEventBus(async (operation) => {
      const delivery = publishing.then(operation);
      deliveries.push(delivery);
      await delivery;
    });
    try {
      await reader.query("SELECT 1");
      publishing = engine.executeAction("emit");
      await committed.promise;
      await pool.query("INSERT INTO _chimpbase_events (event_name, payload_json) VALUES ($1, '{}')", [names[1]]);
      await waitFor(() => observed.length > 0);
      expect(observed).toEqual([names[1]]);
      release.resolve();
      await publishing;
      await waitFor(() => calls.includes(names[1]));
      await Promise.all(deliveries);
      expect(calls).toEqual(names);
      expect(observed).toEqual([names[1]]);
    } finally {
      release.resolve();
      await publishing.catch(() => {});
      engine.stopEventBus();
      await Promise.allSettled(deliveries);
      await pool.query("DELETE FROM _chimpbase_events WHERE event_name = ANY($1::text[])", [names]);
      await reader.end();
      await pool.end();
    }
  });

  for (const failFirst of [false, true]) {
    test(`concurrent consumers reserve all group handlers and ${failFirst ? "retry after rollback" : "skip after commit"}`, async () => {
      const pool = new Pool({ connectionString: PG_URL });
      const name = `subscription.${crypto.randomUUID()}`;
      await ensurePostgresInternalTables(pool);
      const { rows: [row] } = await pool.query<{ id: number }>(
        "INSERT INTO _chimpbase_events (event_name, payload_json) VALUES ($1, '{}') RETURNING id::double precision AS id", [name],
      );
      const events = [{ id: row.id, name, payload: {}, payloadJson: "{}" }];
      const entered = Promise.withResolvers<void>();
      const release = Promise.withResolvers<void>();
      const calls: string[] = [];
      const consumers = Array.from({ length: 2 }, (_, index) => {
        const platform = createDefaultChimpbasePlatformShim();
        const registry = createChimpbaseRegistry();
        registry.subscriptions.set(name, [
          {
            name, idempotent: true,
            handler: async (ctx) => {
              calls.push(`${index}:left`);
              await ctx.kv.set(`${name}.left`, index);
              entered.resolve();
              await release.promise;
            },
          },
          {
            name, idempotent: true,
            handler: async (ctx) => {
              calls.push(`${index}:right`);
              await ctx.kv.set(`${name}.right`, index);
              if (failFirst && index === 0) throw new Error("last handler failed");
            },
          },
        ]);
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
        expect(calls).toEqual(["0:left"]);
        release.resolve();
        const results = await Promise.allSettled(deliveries);
        expect(results.map((result) => result.status)).toEqual([failFirst ? "rejected" : "fulfilled", "fulfilled"]);
        const expectedCalls = failFirst ? ["0:left", "0:right", "1:left", "1:right"] : ["0:left", "0:right"];
        expect(calls).toEqual(expectedCalls);
        expect((await pool.query<{ key: string; value_json: number }>(
          "SELECT key, value_json FROM _chimpbase_kv WHERE key = ANY($1::text[]) ORDER BY key",
          [[`${name}.left`, `${name}.right`]],
        )).rows).toEqual([
          { key: `${name}.left`, value_json: failFirst ? 1 : 0 },
          { key: `${name}.right`, value_json: failFirst ? 1 : 0 },
        ]);
        await Promise.all(consumers.map((consumer) => consumer.deliver()));
        expect(calls).toEqual(expectedCalls);
      } finally {
        release.resolve();
        await Promise.allSettled(deliveries);
        for (const consumer of consumers) consumer.engine.stopEventBus();
        await pool.query("DELETE FROM _chimpbase_kv WHERE key = ANY($1::text[])", [
          [`${name}.left`, `${name}.right`, `_chimpbase.sub.seen:${row.id}:sync:${name}`],
        ]);
        await pool.query("DELETE FROM _chimpbase_events WHERE id = $1", [row.id]);
        await pool.end();
      }
    });
  }

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
