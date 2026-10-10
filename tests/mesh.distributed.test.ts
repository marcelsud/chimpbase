import { afterAll, beforeAll, describe, expect, spyOn, test } from "bun:test";
import { Pool } from "pg";

import { createChimpbase } from "../packages/bun/src/library.ts";
import {
  defineChimpbaseModuleImplementation,
  defineChimpbaseModuleInterface,
} from "../packages/core/index.ts";
import { chimpbaseMesh, service } from "../packages/mesh/src/index.ts";
import { action, contextExtension, route, subscription, v } from "../packages/runtime/index.ts";
import type { StartedBunHost } from "../packages/bun/src/runtime.ts";

const PG_URL = process.env.CHIMPBASE_TEST_PG_URL;
const describeIfPg = PG_URL ? describe : describe.skip;

function postgresUrl(): string {
  if (!PG_URL) throw new Error("PostgreSQL integration URL is unavailable");
  return PG_URL;
}

async function waitFor(predicate: () => boolean | Promise<boolean>): Promise<void> {
  const deadline = performance.now() + 4_000;
  while (performance.now() < deadline) {
    if (await predicate()) return;
    await Bun.sleep(10);
  }
  throw new Error("waitFor timed out");
}

describeIfPg("mesh distributed request and event delivery", () => {
  let pool: Pool;
  beforeAll(() => { pool = new Pool({ connectionString: postgresUrl() }); });
  afterAll(async () => { await pool.end(); });

  test("concurrent requests isolate rollback, route contexts, module access, and telemetry", async () => {
    const prefix = `isolated.${crypto.randomUUID()}`;
    const entered = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    const module = defineChimpbaseModuleInterface({
      name: "request-module", version: 1,
      calls: { identity: { input: v.string(), output: v.string() } }, events: {},
    });
    const host = await createChimpbase({
      storage: { engine: "postgres", url: postgresUrl() },
      telemetry: { persist: { log: true, metric: false, trace: false } },
      modules: [defineChimpbaseModuleImplementation({
        interface: module, calls: { identity: (_ctx, input) => input },
        registrations: [action("internal", () => "private")],
      })],
    });
    host.register(
      contextExtension("requestTag", {
        context: () => "context-extension",
        routeEnv: () => "route-extension",
      }),
      action("hold", async (ctx) => {
        await ctx.kv.set(`${prefix}.rollback`, true);
        await host.routeEnv().action("nested");
        ctx.pubsub.publish(`${prefix}.rollback`, {});
        ctx.log.info(`${prefix}.rollback`);
        entered.resolve();
        await release.promise;
        throw new Error("rollback request");
      }),
      action("nested", async (ctx) => { await ctx.kv.set(`${prefix}.nested`, true); }),
      action("lookup", async (ctx) => {
        expect(v.object({ requestTag: v.string() }).parse(ctx).requestTag).toBe("context-extension");
        const pending = await ctx.kv.get(`${prefix}.rollback`);
        await ctx.kv.set(`${prefix}.commit`, true);
        ctx.pubsub.publish(`${prefix}.commit`, {});
        ctx.log.info(`${prefix}.commit`);
        return { pending, identity: await ctx.call(module.calls.identity, "public") };
      }),
      route("GET /lookup", async (_request, env) => {
        env.set("request-id", "route-id");
        expect(env.get("request-id", v.string())).toBe("route-id");
        expect(v.object({ requestTag: v.string() }).parse(env).requestTag).toBe("route-extension");
        expect(v.object({ requestTag: v.string() }).parse(host.routeEnv()).requestTag).toBe("route-extension");
        return Response.json(await env.action("lookup"));
      }),
    );
    let running: Promise<unknown> | undefined;
    try {
      running = host.executeAction("hold").catch((error: unknown) => error);
      await entered.promise;
      // Registrations added after a request starts must reach the next request engine.
      host.register(action("late", () => "registered-later"));
      expect((await host.executeAction("late")).result).toBe("registered-later");
      const lookup = await host.executeRoute(new Request("http://localhost/lookup"));
      const data: unknown = await lookup.response?.json();
      expect(data).toEqual({ pending: null, identity: "public" });
      expect(lookup.emittedEvents).toMatchObject([{ name: `${prefix}.commit` }]);
      await expect(host.executeAction("internal")).rejects.toThrow("internal operation");
      release.resolve();
      expect(await running).toMatchObject({ message: "rollback request" });
      expect((await pool.query<{ key: string }>("SELECT key FROM _chimpbase_kv WHERE key LIKE $1", [`${prefix}%`])).rows)
        .toEqual([{ key: `${prefix}.commit` }]);
      expect((await pool.query<{ event_name: string }>("SELECT event_name FROM _chimpbase_events WHERE event_name LIKE $1", [`${prefix}%`])).rows)
        .toEqual([{ event_name: `${prefix}.commit` }]);
      expect(host.drainTelemetryRecords().filter((record) => record.kind === "log").map((record) => record.message).sort())
        .toEqual([`${prefix}.commit`, `${prefix}.rollback`]);
      expect((await pool.query<{ count: number }>(
        "SELECT count(*)::int AS count FROM _chimpbase_stream_events WHERE payload_json->>'message' = $1", [`${prefix}.commit`],
      )).rows[0]?.count).toBe(1);
    } finally {
      release.resolve();
      await running;
      await host.close();
      await pool.query("DELETE FROM _chimpbase_kv WHERE key LIKE $1", [`${prefix}%`]);
      await pool.query("DELETE FROM _chimpbase_events WHERE event_name LIKE $1", [`${prefix}%`]);
      await pool.query("DELETE FROM _chimpbase_stream_events WHERE payload_json->>'message' LIKE $1", [`${prefix}%`]);
    }
  });

  test("shutdown drains concurrent PostgreSQL requests before mesh deregistration", async () => {
    const entered = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    const host = await createChimpbase({ storage: { engine: "postgres", url: postgresUrl() } });
    host.register(chimpbaseMesh({ heartbeatMs: 0, transport: "local-only", services: [service({
      name: "shutdown", actions: {
        nodeId: (ctx) => ctx.mesh?.nodeId(),
        hold: async () => { entered.resolve(); await release.promise; throw new Error("rollback request"); },
      },
    })] }));
    const started = await host.start({ serve: false, runWorker: false });
    let running: Promise<unknown> | undefined;
    let stopping: Promise<void> | undefined;
    let stopped = false;
    try {
      const nodeId = (await host.executeAction("v1.shutdown.nodeId")).result;
      running = host.executeAction("v1.shutdown.hold").catch((error: unknown) => error);
      await entered.promise;
      stopping = started.stop().then(() => { stopped = true; });
      expect((await host.executeRoute(new Request("http://localhost/lookup"))).response?.status).toBe(503);
      await Bun.sleep(30);
      expect(stopped).toBe(false);
      expect((await pool.query<{ node_id: string }>("SELECT node_id FROM _chimpbase_mesh_nodes WHERE node_id = $1", [nodeId])).rows).toHaveLength(1);
      release.resolve();
      expect(await running).toMatchObject({ message: "rollback request" });
      await stopping;
      expect((await pool.query<{ node_id: string }>("SELECT node_id FROM _chimpbase_mesh_nodes WHERE node_id = $1", [nodeId])).rows).toEqual([]);
      expect((await host.executeAction("v1.shutdown.nodeId")).result).toBe(nodeId);
    } finally {
      release.resolve();
      await running;
      await (stopping ?? started.stop());
      await host.close();
    }
  });

  test("async hosts broadcast to each node while ordinary handlers stay in shared jobs", async () => {
    const event = `broadcast.${crypto.randomUUID()}`;
    const broadcasts: string[] = [];
    const ordinary: string[] = [];
    const hosts = await Promise.all(["a", "b"].map(async (label) => {
      const host = await createChimpbase({
        storage: { engine: "postgres", url: postgresUrl() }, subscriptions: { dispatch: "async" },
      });
      host.register(chimpbaseMesh({ heartbeatMs: 0, transport: "local-only", services: [service({
        name: label,
        actions: { emit: async (ctx) => {
          await ctx.mesh?.emit(event, { id: "broadcast" });
        } },
        events: { [event]: () => { broadcasts.push(label); } },
      })] }), subscription(event, () => { ordinary.push("mixed"); }, { idempotent: true, name: event }));
      return host;
    }));
    const started: StartedBunHost[] = [];
    try {
      for (const host of hosts) started.push(await host.start({ serve: false, runWorker: false }));
      // Let each polling bus establish its startup high-water mark.
      await Bun.sleep(30);
      await hosts[0].executeAction("v1.a.emit");
      expect(broadcasts).toEqual(["a"]);
      expect(ordinary).toEqual([]);
      await waitFor(() => broadcasts.length === 2);
      expect(broadcasts.sort()).toEqual(["a", "b"]);
      for (const host of hosts) await host.drain();
      expect(ordinary).toEqual(["mixed"]);
      expect(broadcasts.sort()).toEqual(["a", "b"]);
    } finally {
      for (const instance of started) await instance.stop();
      for (const host of hosts) await host.close();
      await pool.query("DELETE FROM _chimpbase_queue_jobs WHERE payload_json->>'eventName' LIKE $1", [`${event}%`]);
      await pool.query("DELETE FROM _chimpbase_kv WHERE key LIKE $1", [`_chimpbase.sub.seen:%:${event}%`]);
      await pool.query("DELETE FROM _chimpbase_events WHERE event_name LIKE $1", [`${event}%`]);
    }
  }, 15_000);

  test("a poison broadcast rolls back independently and stops retrying while healthy events continue", async () => {
    const prefix = `poison.${crypto.randomUUID()}`;
    let failures = 0;
    let healthy = 0;
    const errors = spyOn(console, "error").mockImplementation(() => {});
    const host = await createChimpbase({ storage: { engine: "postgres", url: postgresUrl() } });
    host.register(chimpbaseMesh({ heartbeatMs: 0, transport: "local-only", services: [service({
      name: "events", events: {
        [`${prefix}.bad`]: async (ctx) => {
          failures += 1;
          await ctx.kv.set(`${prefix}.rollback`, failures);
          throw new Error("poison broadcast");
        },
        [`${prefix}.good`]: async (ctx) => {
          healthy += 1;
          await ctx.kv.set(`${prefix}.healthy`, healthy);
        },
      },
    })] }));
    const started = await host.start({ serve: false, runWorker: false });
    try {
      await Bun.sleep(30);
      await pool.query("INSERT INTO _chimpbase_events (event_name, payload_json) VALUES ($1, '{}'), ($2, '{}')", [`${prefix}.bad`, `${prefix}.good`]);
      await waitFor(() => failures === 3);
      await pool.query("INSERT INTO _chimpbase_events (event_name, payload_json) VALUES ($1, '{}')", [`${prefix}.good`]);
      await waitFor(() => healthy === 2);
      await Bun.sleep(1_100);
      expect(failures).toBe(3);
      expect(healthy).toBe(2);
      expect((await pool.query<{ key: string; value_json: number }>("SELECT key, value_json FROM _chimpbase_kv WHERE key LIKE $1", [`${prefix}%`])).rows)
        .toEqual([{ key: `${prefix}.healthy`, value_json: 2 }]);
      expect(errors.mock.calls.some((call: readonly unknown[]) => (call[1] as { exhausted?: boolean })?.exhausted === true)).toBe(true);
    } finally {
      errors.mockRestore();
      await started.stop();
      await host.close();
      await pool.query("DELETE FROM _chimpbase_kv WHERE key LIKE $1", [`${prefix}%`]);
      await pool.query("DELETE FROM _chimpbase_events WHERE event_name LIKE $1", [`${prefix}%`]);
    }
  }, 10_000);

  test("ordinary async subscriptions on a peer still receive events from a producer without subscribers", async () => {
    const event = `peer-only.${crypto.randomUUID()}`;
    let handled = 0;
    const producer = await createChimpbase({
      storage: { engine: "postgres", url: postgresUrl() }, subscriptions: { dispatch: "async" },
    });
    const consumer = await createChimpbase({
      storage: { engine: "postgres", url: postgresUrl() }, subscriptions: { dispatch: "async" },
    });
    producer.register(chimpbaseMesh({ heartbeatMs: 0, transport: "local-only", services: [service({
      name: "producer", actions: { emit: (ctx) => { ctx.pubsub.publish(event, {}); } },
    })] }));
    consumer.register(subscription(event, () => { handled += 1; }), chimpbaseMesh({
      heartbeatMs: 0, transport: "local-only", services: [service({ name: "consumer" })],
    }));
    const startedProducer = await producer.start({ serve: false, runWorker: false });
    const startedConsumer = await consumer.start({ serve: false, runWorker: false });
    try {
      await Bun.sleep(30);
      await producer.executeAction("v1.producer.emit");
      await waitFor(async () => (await pool.query<{ id: string }>(
        "SELECT id FROM _chimpbase_queue_jobs WHERE payload_json->>'eventName' = $1", [event],
      )).rows.length > 0);
      expect(await producer.processNextQueueJob()).toBeNull();
      await consumer.drain();
      expect(handled).toBe(1);
    } finally {
      await startedProducer.stop();
      await startedConsumer.stop();
      await producer.close();
      await consumer.close();
      await pool.query("DELETE FROM _chimpbase_queue_jobs WHERE payload_json->>'eventName' = $1", [event]);
      await pool.query("DELETE FROM _chimpbase_events WHERE event_name = $1", [event]);
    }
  });

  test("a separate producer enqueues a balanced event for a discovered remote worker", async () => {
    const event = `balanced.${crypto.randomUUID()}`;
    const processed: unknown[] = [];
    const producer = await createChimpbase({ storage: { engine: "postgres", url: postgresUrl() } });
    const consumer = await createChimpbase({ storage: { engine: "postgres", url: postgresUrl() } });
    producer.register(chimpbaseMesh({ heartbeatMs: 0, transport: "local-only", services: [service({
      name: "producer", actions: {
        emit: async (ctx) => await ctx.mesh?.emit(event, { id: "remote-job" }, { balanced: true }),
        peers: (ctx) => ctx.mesh?.peers(),
      },
    })] }));
    consumer.register(chimpbaseMesh({ heartbeatMs: 0, transport: "local-only", services: [service({
      name: "consumer", events: { [event]: { balanced: true, handler: (_ctx, payload) => { processed.push(payload); } } },
    })] }));
    const consumerStarted = await consumer.start({ serve: false, runWorker: false });
    const producerStarted = await producer.start({ serve: false, runWorker: false });
    try {
      const peers = v.array(v.object({ services: v.array(v.object({ events: v.array(v.string()) })) }))
        .parse((await producer.executeAction("v1.producer.peers")).result);
      expect(peers.some((peer) => peer.services.some((entry) => entry.events.includes(event)))).toBe(true);
      await producer.executeAction("v1.producer.emit");
      expect(await producer.processNextQueueJob()).toBeNull();
      expect(await consumer.processNextQueueJob()).not.toBeNull();
      expect(processed).toEqual([{ id: "remote-job" }]);
      expect(await consumer.processNextQueueJob()).toBeNull();
    } finally {
      await producerStarted.stop();
      await consumerStarted.stop();
      await producer.close();
      await consumer.close();
      await pool.query("DELETE FROM _chimpbase_queue_jobs WHERE queue_name = $1", [`__chimpbase.mesh.balanced.${event}`]);
    }
  });
});
