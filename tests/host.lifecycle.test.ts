import { describe, expect, spyOn, test } from "bun:test";
import { Database } from "bun:sqlite";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Pool } from "pg";

import { ChimpbaseEngine, createChimpbaseRegistry, createDefaultChimpbasePlatformShim, defineChimpbaseApp, ensureSqliteInternalTables, normalizeProjectConfig, type ChimpbaseEventBusCallback, type ChimpbaseEventRecord, type ChimpbaseQueueJobRecord } from "../packages/core/index.ts";
import { bunRuntimeShim, ChimpbaseBunHost } from "../packages/bun/src/runtime.ts";
import { createSqliteEngineAdapter } from "../packages/bun/src/sqlite_adapter.ts";
import { createChimpbaseRuntimeLibrary } from "../packages/host/src/library.ts";
import { ChimpbaseHost, type ChimpbaseRuntimeShim } from "../packages/host/src/runtime.ts";
import { action, CHIMPBASE_REQUEST_REJECTED_HEADER, onStart, onStop, plugin, route, subscription, v, worker, type ChimpbaseContext } from "../packages/runtime/index.ts";

class TestHost extends ChimpbaseHost<{ port: number }> {}

function cleanupSink(shutdown: () => void | Promise<void>) {
  const span = { setAttribute() {}, end() {} };
  return {
    onLog() {}, onMetric() {}, startSpan: () => span, startHandlerSpan: () => span,
    async shutdown() { await shutdown(); },
  };
}

function createTrackedRuntime(errors: {
  serverStart?: Error; serverStop?: Error; busStart?: Error; busStop?: Error;
  storageClose?: Error; secrets?: Error; adapterCreate?: Error;
} = {}) {
  const calls = { serverStart: 0, serverStop: 0, busStart: 0, busStop: 0, storageClose: 0 };
  let deliver: ChimpbaseEventBusCallback | undefined;
  const runtime: ChimpbaseRuntimeShim<{ port: number }> = {
    ...bunRuntimeShim,
    env: {
      get: () => undefined,
      toObject() {
        if (errors.secrets) throw errors.secrets;
        return {};
      },
    },
    server: {
      create({ port }) {
        calls.serverStart += 1;
        if (errors.serverStart) throw errors.serverStart;
        return { port };
      },
      async stop() {
        calls.serverStop += 1;
        if (errors.serverStop) throw errors.serverStop;
      },
    },
    storage: {
      async open(...args) {
        const resources = await bunRuntimeShim.storage.open(...args);
        return {
          ...resources,
          createAdapter() {
            if (errors.adapterCreate) throw errors.adapterCreate;
            return resources.createAdapter();
          },
          eventBus: {
            async publish() {},
            start(callback) {
              deliver = callback;
              calls.busStart += 1;
              if (errors.busStart) throw errors.busStart;
            },
            stop() {
              calls.busStop += 1;
              if (errors.busStop) throw errors.busStop;
            },
          },
          storage: {
            async close() {
              calls.storageClose += 1;
              await resources.storage.close();
              if (errors.storageClose) throw errors.storageClose;
            },
          },
        };
      },
    },
  };
  return {
    calls, runtime, library: createChimpbaseRuntimeLibrary(TestHost, runtime),
    async deliverEvents(events: ChimpbaseEventRecord[]) {
      if (!deliver) throw new Error("event bus did not start");
      await deliver(events);
    },
  };
}

describe("host lifecycle cleanup", () => {
  for (const transactional of [true, false]) {
    test(`retained lifecycle SQL builders and streams use the detached adapter (${transactional ? "transactional" : "nontransactional"})`, async () => {
      const activeDb = new Database(":memory:");
      const detachedDb = new Database(":memory:");
      const platform = createDefaultChimpbasePlatformShim();
      const registry = createChimpbaseRegistry();
      const adapter = createSqliteEngineAdapter(activeDb, platform);
      const detachedAdapter = createSqliteEngineAdapter(detachedDb, platform);
      const config = normalizeProjectConfig({ storage: { engine: "memory" } });
      const engine = new ChimpbaseEngine({
        adapter, createDetachedAdapter: () => detachedAdapter, platform, registry,
        secrets: { get: () => null }, subscriptions: config.subscriptions,
        telemetry: config.telemetry, worker: config.worker,
      });
      let background!: () => Promise<void>;
      const streamed: string[] = [];
      try {
        for (const db of [activeDb, detachedDb]) {
          ensureSqliteInternalTables(db);
          db.exec("CREATE TABLE effects (label TEXT)");
        }
        await engine.executeLifecycleHook(async (ctx) => {
          const db = ctx.db.kysely<{ effects: { label: string } }>();
          await db.insertInto("effects").values({ label: "startup" }).execute();
          const retainedInsert = db.insertInto("effects").values({ label: "retained" });
          const retainedStream = db.selectFrom("effects").select("label").orderBy("label");
          background = async () => {
            await ctx.db.query("INSERT INTO effects VALUES ('raw')");
            await retainedInsert.execute();
            await ctx.db.kysely<{ effects: { label: string } }>()
              .insertInto("effects").values({ label: "late" }).execute();
            for await (const row of retainedStream.stream(1)) streamed.push(row.label);
          };
        }, null, "background", transactional);
        const failure = new Error("foreground failure");
        registry.actions.set("fail", action("fail", async (ctx) => {
          await ctx.db.query("INSERT INTO effects VALUES ('foreground')");
          await background();
          throw failure;
        }));
        await expect(engine.executeAction("fail", [])).rejects.toBe(failure);
        expect(activeDb.query("SELECT label FROM effects").all()).toEqual([{ label: "startup" }]);
        expect(detachedDb.query("SELECT label FROM effects ORDER BY label").all()).toEqual([
          { label: "late" }, { label: "raw" }, { label: "retained" },
        ]);
        expect(streamed).toEqual(["late", "raw", "retained"]);
      } finally {
        activeDb.close();
        detachedDb.close();
      }
    });
  }

  test("library cleanup preserves the action failure when a host subclass closes synchronously", async () => {
    const failure = new Error("action failed");
    const cleanupFailure = new Error("synchronous cleanup failed");
    const { calls, runtime } = createTrackedRuntime();
    let closing: Promise<void> | undefined;
    class SynchronousCleanupHost extends TestHost {
      override close(): Promise<void> {
        closing = super.close();
        throw cleanupFailure;
      }
    }
    const library = createChimpbaseRuntimeLibrary(SynchronousCleanupHost, runtime);
    try {
      await expect(library.runChimpbaseAppAction({ registrations: [action("fail", () => { throw failure; })] },
        "fail", [], { storage: { engine: "memory" } })).rejects.toBe(failure);
    } finally {
      await closing;
    }
    expect(calls.storageClose).toBe(1);
  });

  for (const operation of ["project action", "app action", "workflow contract sync"] as const) {
    test(`${operation} preserves its failure when cleanup also fails`, async () => {
      const failure = new Error(`${operation} failed`);
      const { calls, library } = createTrackedRuntime({ storageClose: new Error("storage cleanup failed") });
      let sinksClosed = 0;
      const sink = cleanupSink(() => { sinksClosed += 1; throw new Error("sink cleanup failed"); });
      const method = spyOn(TestHost.prototype, operation === "workflow contract sync" ? "syncWorkflowContracts" : "executeAction")
        .mockRejectedValue(failure);
      const projectDir = await mkdtemp(join(tmpdir(), "chimpbase-failed-library-"));
      await writeFile(join(projectDir, "chimpbase.app.ts"), "export default {};\n");
      try {
        const invoke = operation === "app action"
          ? library.runChimpbaseAppAction({}, "fail", [], { storage: { engine: "memory" }, sinks: [sink] })
          : operation === "project action"
            ? library.runChimpbaseAction("fail", [], { projectDir })
            : library.syncChimpbaseWorkflowContracts({ projectDir });
        await expect(invoke).rejects.toBe(failure);
        expect(calls.storageClose).toBe(1);
        if (operation === "app action") expect(sinksClosed).toBe(1);
      } finally {
        method.mockRestore();
        await rm(projectDir, { recursive: true, force: true });
      }
    });
  }

  for (const engine of ["memory", "sqlite", "postgres"] as const) {
    const pgUrl = process.env.CHIMPBASE_TEST_PG_URL;
    (engine === "postgres" && !pgUrl ? test.skip : test)(`idempotency retention uses one database call and preserves cutoff and namespace rules (${engine})`, async () => {
      const projectDir = await mkdtemp(join(tmpdir(), "chimpbase-idempotency-retention-"));
      const schema = `idempotency_${crypto.randomUUID().replaceAll("-", "")}`;
      const pool = engine === "postgres" ? new Pool({ connectionString: pgUrl }) : undefined;
      const scopedUrl = pgUrl ? new URL(pgUrl) : undefined;
      await pool?.query(`CREATE SCHEMA ${schema}`);
      scopedUrl?.searchParams.set("options", `-c search_path=${schema}`);
      const now = Date.parse("2026-10-12T00:00:00.000Z");
      const host = await ChimpbaseBunHost.create({
        projectDir,
        config: normalizeProjectConfig({
          storage: { engine, path: "retention.db", url: scopedUrl?.href },
          subscriptions: { idempotency: { retention: { enabled: true, maxAgeDays: 1 } } },
        }),
        platform: { ...createDefaultChimpbasePlatformShim(), now: () => now },
      });
      const adapter = host.engine.getBlobsAdapter();
      const cutoff = now - 86_400_000;
      const prefix = "_chimpbase.sub.seen:";
      const fixtures: [string, string, string | null, boolean][] = [
        [`${prefix}old-iso`, new Date(cutoff - 1).toISOString(), null, false],
        [`${prefix}old-sqlite`, "2000-01-01 00:00:00", null, false],
        [`${prefix}old-offset`, "2026-10-11T01:59:59.999+02:00", null, false],
        [`${prefix}cutoff`, new Date(cutoff).toISOString(), null, true],
        [`${prefix}cutoff-offset`, "2026-10-11T02:00:00+02:00", null, true],
        [`${prefix}recent`, new Date(cutoff + 1).toISOString(), null, true],
        [`${prefix}expired`, "2000-01-01 00:00:00", "2000-01-01 00:00:00", true],
        [`${prefix}future-expiry`, "2000-01-01 00:00:00", "2040-01-01 00:00:00", false],
        ["xchimpbase.sub.seen:lookalike", "2000-01-01 00:00:00", null, true],
        ["_CHIMPBASE.SUB.SEEN:uppercase", "2000-01-01 00:00:00", null, true],
        ["unrelated-key", "2000-01-01 00:00:00", null, true],
      ];
      if (engine !== "postgres") fixtures.push([`${prefix}invalid-date`, "invalid", null, true]);
      const query = spyOn(adapter, "query");
      const list = spyOn(adapter, "kvList");
      const remove = spyOn(adapter, "kvDelete");
      try {
        for (const [key, updatedAt, expiresAt] of fixtures) {
          await adapter.query("INSERT INTO _chimpbase_kv (key, value_json, updated_at, expires_at) VALUES (?1, ?2, ?3, ?4)",
            [key, "true", updatedAt, expiresAt], v.object({}));
        }
        const cleanup = host.registry.crons.get("__chimpbase.subscription.idempotency.cleanup");
        if (!cleanup) throw new Error("idempotency cleanup cron missing");
        host.registerAction("cleanup", async (ctx) => await cleanup.handler(ctx, {
          fireAt: new Date(now).toISOString(), fireAtMs: now, name: cleanup.name, schedule: cleanup.schedule,
        }));
        query.mockClear();
        await host.engine.executeAction("cleanup");
        expect(query).toHaveBeenCalledTimes(1);
        expect(list).not.toHaveBeenCalled();
        expect(remove).not.toHaveBeenCalled();
        query.mockRestore();
        const rows = await adapter.query("SELECT key FROM _chimpbase_kv ORDER BY key", [], v.object({ key: v.string() }));
        expect(rows.map((row) => row.key)).toEqual(fixtures.filter(([, , , keep]) => keep).map(([key]) => key).sort());
      } finally {
        query.mockRestore();
        list.mockRestore();
        remove.mockRestore();
        await host.close();
        await pool?.query(`DROP SCHEMA ${schema} CASCADE`);
        await pool?.end();
        await rm(projectDir, { recursive: true, force: true });
      }
    });
  }
  test("worker registration rejects duplicate and reserved names without replacing handlers", async () => {
    const { library } = createTrackedRuntime();
    const calls: string[] = [];
    const host = await library.createChimpbase({ storage: { engine: "memory" } });
    try {
      host.registerWorker("job", () => { calls.push("first"); });
      expect(() => host.registerWorker("job", () => { calls.push("second"); })).toThrow("duplicate worker registration: job");
      for (const name of ["__chimpbase.cron.run", "__chimpbase.subscription.run", "__chimpbase.workflow.run"]) {
        expect(() => host.registerWorker(name, () => {})).toThrow("reserved queue name");
      }
      host.register(action("enqueue", async (ctx) => await ctx.enqueue("job", {})));
      await host.executeAction("enqueue");
      await host.processNextQueueJob();
      expect(calls).toEqual(["first"]);
    } finally { await host.close(); }
    await expect(library.createChimpbase({ storage: { engine: "memory" }, registrations: [
      worker("duplicate", () => {}), worker("duplicate", () => {}),
    ] })).rejects.toThrow("duplicate worker registration: duplicate");
  });

  for (const concurrent of [false, true]) {
    test(`worker shutdown aborts I/O and waits for its transaction (${concurrent ? "concurrent" : "serialized"})`, async () => {
      const entered = Promise.withResolvers<void>();
      const aborted = Promise.withResolvers<void>();
      const release = Promise.withResolvers<void>();
      const order: string[] = [];
      let runs = 0;
      const runtime: ChimpbaseRuntimeShim<{ port: number }> = {
        ...bunRuntimeShim,
        server: { create: ({ port }) => ({ port }), async stop() {} },
        storage: { async open(...args) {
          const resources = await bunRuntimeShim.storage.open(...args);
          return { ...resources, supportsConcurrentWorkers: concurrent,
            createAdapter(group) {
              const adapter = resources.createAdapter(group);
              if (concurrent) adapter.claimNextQueueJobs = async (leaseMs, limit, queues, targets) => {
                const jobs: ChimpbaseQueueJobRecord[] = [];
                for (let i = 0; i < limit; i += 1) {
                  const job = await adapter.claimNextQueueJob(leaseMs, queues, targets);
                  if (!job) break;
                  jobs.push(job);
                }
                return jobs;
              };
              return adapter;
            },
          };
        } },
      };
      const library = createChimpbaseRuntimeLibrary(TestHost, runtime);
      const host = await library.createChimpbase({
        storage: { engine: "memory" }, workerRuntime: { concurrency: 1 },
        registrations: [
          action("enqueue", async (ctx) => {
            await ctx.enqueue("job", {});
            await ctx.enqueue("job", {});
          }),
          action("inspect", async (ctx) => await ctx.kv.get("finished")),
          worker("job", async (ctx, _payload, execution) => {
            runs += 1;
            if (!execution) throw new Error("worker execution metadata missing");
            expect(execution.signal.aborted).toBe(false);
            execution.signal.addEventListener("abort", () => { aborted.resolve(); }, { once: true });
            entered.resolve();
            await aborted.promise;
            order.push("aborted");
            await release.promise;
            await ctx.kv.set("finished", true);
            order.push("finished");
          }),
          onStop("cleanup", () => { order.push("cleanup"); }),
        ],
      });
      await host.executeAction("enqueue");
      const started = await host.start({ serve: false, runWorker: true });
      let stopping: Promise<void> | undefined;
      try {
        await entered.promise;
        stopping = started.stop();
        await aborted.promise;
        await Bun.sleep(10);
        expect(order).toEqual(["aborted"]);
        release.resolve();
        await stopping;
        expect(order).toEqual(["aborted", "finished", "cleanup"]);
        expect(runs).toBe(1);
        expect((await host.executeAction("inspect")).result).toBe(true);
        expect(await host.engine.getBlobsAdapter().query(
          "SELECT status, attempt_count, lease_expires_at_ms FROM _chimpbase_queue_jobs ORDER BY id", [],
          v.object({ status: v.string(), attempt_count: v.number(), lease_expires_at_ms: v.number().nullable() }),
        )).toEqual([
          { status: "completed", attempt_count: 1, lease_expires_at_ms: null },
          { status: "pending", attempt_count: 0, lease_expires_at_ms: null },
        ]);
      } finally {
        release.resolve();
        await (stopping ?? started.stop());
        await host.close();
      }
    });
  }

  test("ready hooks run after ordinary hooks, HTTP and the event bus, and unwind on failure", async () => {
    const failure = new Error("ready hook failed");
    const { calls, library } = createTrackedRuntime();
    const order: string[] = [];
    const host = await library.createChimpbase({
      storage: { engine: "memory" },
      registrations: [
        action("markReady", async (ctx) => { await ctx.kv.set("ready", true); }),
        action("readReady", async (ctx) => await ctx.kv.get("ready")),
        onStart("ready", async (ctx) => {
          expect(calls.serverStart).toBe(1);
          expect(calls.busStart).toBe(1);
          order.push("ready");
          await ctx.action("markReady");
          throw failure;
        }, { phase: "ready", transactional: false }),
        onStart("initialize", () => { order.push("initialize"); }),
        onStop("cleanup", () => { order.push("cleanup"); }),
      ],
    });
    try {
      await expect(host.start({ serve: true, runWorker: false })).rejects.toBe(failure);
      expect(order).toEqual(["initialize", "ready", "cleanup"]);
      expect(calls.serverStop).toBe(1);
      expect(calls.busStop).toBe(1);
      expect((await host.executeAction("readReady")).result).toBe(true);
      expect((await host.executeRoute(new Request("http://test/"))).response?.status).toBe(503);
    } finally {
      await host.close();
    }
  });

  for (const transactional of [true, false]) {
    test(`onStop ${transactional ? "rolls back its writes by default" : "can commit an action independently of cleanup errors"}`, async () => {
      const { library } = createTrackedRuntime();
      const failure = new Error("resource cleanup failed");
      let delivered = 0;
      let cleanedUp = false;
      const write = async (ctx: ChimpbaseContext) => {
        await ctx.kv.set("shutdown.write", true);
        ctx.pubsub.publish("shutdown.written", {});
      };
      const reported = spyOn(console, "error").mockImplementation(() => {});
      const host = await library.createChimpbase({
        storage: { engine: "memory" }, registrations: [
          action("shutdownWrite", write),
          action("shutdownRead", async (ctx) => await ctx.kv.get("shutdown.write")),
          subscription("shutdown.written", () => { delivered += 1; }),
          onStop("failure", async (ctx) => {
            if (transactional) await write(ctx);
            else await ctx.action("shutdownWrite");
            throw failure;
          }, transactional ? undefined : { transactional: false }),
          onStop("cleanup", () => { cleanedUp = true; }),
        ],
      });
      try {
        const started = await host.start({ serve: false, runWorker: false });
        await started.stop();
        expect(cleanedUp).toBe(true);
        expect((await host.executeAction("shutdownRead")).result).toBe(transactional ? null : true);
        expect(delivered).toBe(transactional ? 0 : 1);
        expect(reported).toHaveBeenCalledTimes(1);
        const error: unknown = reported.mock.calls[0]?.[1];
        expect(error).toBe(failure);
      } finally {
        reported.mockRestore();
        await host.close();
      }
    });
  }

  test("a request whose preparation finishes after stop cannot execute", async () => {
    const { library } = createTrackedRuntime();
    const entered = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    let executed = false;
    const host = await library.createChimpbase({ storage: { engine: "memory" } });
    host.registerRoute("POST /prepared", async () => null, {
      prepare: async () => {
        entered.resolve();
        await release.promise;
        return async () => { executed = true; return new Response("finished"); };
      },
    });
    const started = await host.start({ serve: false, runWorker: false });
    const pending = host.executeRoute(new Request("http://localhost/prepared", { method: "POST" }));
    try {
      await entered.promise;
      await started.stop();
      release.resolve();
      const outcome = await pending;
      expect(outcome.response?.status).toBe(503);
      expect(outcome.response?.headers.get(CHIMPBASE_REQUEST_REJECTED_HEADER)).toBe("1");
      expect(executed).toBe(false);
    } finally {
      release.resolve();
      await pending;
      await started.stop();
      await host.close();
    }
  });

  test("stop halts event sources, drains active delivery, and ignores late callbacks before onStop", async () => {
    const { calls, deliverEvents, library } = createTrackedRuntime();
    const entered = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    let deliveries = 0;
    let hooks = 0;
    const host = await library.createChimpbase({
      storage: { engine: "memory" }, registrations: [
        subscription("external", async () => {
          deliveries += 1;
          entered.resolve();
          await release.promise;
        }),
        onStop("cleanup", () => { hooks += 1; expect(calls.busStop).toBe(1); }),
      ],
    });
    const started = await host.start({ serve: false, runWorker: false });
    const event = { id: 1, name: "external", payload: {}, payloadJson: "{}" };
    const activeDelivery = deliverEvents([event]);
    let stopping: Promise<void> | undefined;
    try {
      await entered.promise;
      stopping = started.stop();
      expect(calls.busStop).toBe(1);
      await deliverEvents([event]);
      expect(hooks).toBe(0);
      release.resolve();
      await activeDelivery;
      await stopping;
      await deliverEvents([event]);
      expect(hooks).toBe(1);
      expect(deliveries).toBe(1);
    } finally {
      release.resolve();
      await activeDelivery;
      await (stopping ?? started.stop());
      await host.close();
    }
  });

  test("stop rejects new routes and drains active routes before onStop hooks", async () => {
    const { library } = createTrackedRuntime();
    const entered = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    let hooks = 0;
    const host = await library.createChimpbase({
      storage: { engine: "memory" }, registrations: [
        action("inspect", () => "available"),
        route("GET /hold", async () => { entered.resolve(); await release.promise; return new Response("finished"); }),
        onStop("cleanup", () => { hooks += 1; }),
      ],
    });
    const started = await host.start({ serve: false, runWorker: false });
    let stopping: Promise<void> | undefined;
    const request = host.executeRoute(new Request("http://localhost/hold"));
    try {
      await entered.promise;
      stopping = started.stop();
      expect((await host.executeRoute(new Request("http://localhost/hold"))).response?.status).toBe(503);
      await Bun.sleep(10);
      expect(hooks).toBe(0);
      release.resolve();
      expect(await (await request).response?.text()).toBe("finished");
      await stopping;
      expect(hooks).toBe(1);
      expect((await host.executeAction("inspect")).result).toBe("available");
    } finally {
      release.resolve();
      await request;
      await (stopping ?? started.stop());
      await host.close();
    }
  });

  for (const source of ["action", "route", "routeEnv"] as const) {
  test(`close waits for active ${source} operations before closing storage and sinks`, async () => {
    const { calls, library } = createTrackedRuntime();
    const entered = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    let sinksClosed = 0;
    const host = await library.createChimpbase({
      storage: { engine: "memory" }, sinks: [cleanupSink(() => { sinksClosed += 1; })],
      registrations: [action("hold", async (ctx) => {
        entered.resolve();
        await release.promise;
        await ctx.kv.set("finished", true);
      }), action("finishRoute", async (ctx) => { await ctx.kv.set("finished", true); }),
      route("GET", "/hold", async (_request, env) => {
        entered.resolve();
        await release.promise;
        await env.action("finishRoute");
        return new Response("finished");
      })],
    });
    const operation = source === "route"
      ? host.executeRoute(new Request("http://test/hold"))
      : source === "routeEnv" ? host.routeEnv().action("hold") : host.executeAction("hold");
    let closing: Promise<void> | undefined;
    try {
      await entered.promise;
      closing = host.close();
      await Bun.sleep(10);
      expect(calls.storageClose).toBe(0);
      expect(sinksClosed).toBe(0);
      release.resolve();
      await operation;
      await closing;
      expect(calls.storageClose).toBe(1);
      expect(sinksClosed).toBe(1);
    } finally {
      release.resolve();
      await operation;
      await (closing ?? host.close());
    }
  });
  }

  test("failed initialization runs onStop cleanup for resources acquired by earlier hooks", async () => {
    const failure = new Error("initialization failed");
    const { calls, library } = createTrackedRuntime();
    let activeResources = 0;
    const host = await library.createChimpbase({
      storage: { engine: "memory" },
      registrations: [
        onStart("acquire", () => { activeResources += 1; }),
        onStart("fail", () => { throw failure; }),
        onStop("release", () => { activeResources -= 1; }),
      ],
    });
    try {
      await expect(host.start({ serve: true, runWorker: true })).rejects.toBe(failure);
      expect(activeResources).toBe(0);
      expect(calls.serverStart).toBe(0);
      expect(calls.busStart).toBe(0);
      expect(calls.storageClose).toBe(0);
    } finally {
      await host.close();
    }
  });

  test("failed HTTP startup leaves queued work pending and no running worker", async () => {
    const failure = new Error("server unavailable");
    const { calls, library } = createTrackedRuntime({ serverStart: failure });
    let processed = 0;
    let stopped = 0;
    const host = await library.createChimpbase({
      storage: { engine: "memory" }, workerRuntime: { pollIntervalMs: 1 },
      registrations: [
        worker("job", () => { processed += 1; }),
        action("enqueue", async (ctx) => await ctx.enqueue("job", {})),
        onStop("cleanup", () => { stopped += 1; }),
      ],
    });
    try {
      await host.executeAction("enqueue");
      await expect(host.start({ serve: true, runWorker: true })).rejects.toBe(failure);
      await Bun.sleep(15);
      expect(processed).toBe(0);
      expect(stopped).toBe(1);
      expect(calls.busStart).toBe(0);
      expect(calls.storageClose).toBe(0);
      expect(await host.processNextQueueJob()).not.toBeNull();
      expect(processed).toBe(1);
    } finally {
      await host.close();
    }
  });

  test("failed event-bus startup stops HTTP and the worker even when HTTP stop rejects", async () => {
    const failure = new Error("bus unavailable");
    const { calls, library } = createTrackedRuntime({ busStart: failure, serverStop: new Error("stop failed") });
    let processed = 0;
    const host = await library.createChimpbase({
      storage: { engine: "memory" }, workerRuntime: { pollIntervalMs: 1 },
      registrations: [worker("job", () => { processed += 1; }), action("enqueue", async (ctx) => await ctx.enqueue("job", {}))],
    });
    try {
      await expect(host.start({ serve: true, runWorker: true })).rejects.toBe(failure);
      expect(calls.serverStop).toBe(1);
      expect(calls.busStop).toBe(1);
      await host.executeAction("enqueue");
      await Bun.sleep(15);
      expect(processed).toBe(0);
    } finally {
      await host.close();
    }
  });

  for (const phase of ["onStart", "HTTP", "event bus"] as const) {
    test(`startChimpbaseApp closes its inaccessible host after failed ${phase}`, async () => {
      const failure = new Error(`${phase} failed`);
      const { calls, library } = createTrackedRuntime({
        serverStart: phase === "HTTP" ? failure : undefined,
        busStart: phase === "event bus" ? failure : undefined,
        storageClose: new Error("cleanup failed"),
      });
      let sinkClosed = 0;
      await expect(library.startChimpbaseApp({
        app: defineChimpbaseApp({ registrations: phase === "onStart" ? [onStart("fail", () => { throw failure; })] : [] }),
        storage: { engine: "memory" }, serve: true, runWorker: true,
        sinks: [cleanupSink(() => { sinkClosed += 1; })],
      })).rejects.toBe(failure);
      expect(calls.storageClose).toBe(1);
      expect(sinkClosed).toBe(1);
    });
  }

  test("library stop closes storage and sinks and stops the worker after HTTP stop fails", async () => {
    const failure = new Error("stop failed");
    const { calls, library } = createTrackedRuntime({ serverStop: failure, storageClose: new Error("close failed") });
    let sinkClosed = 0;
    let processed = 0;
    const started = await library.startChimpbaseApp({
      app: defineChimpbaseApp({ registrations: [worker("job", () => { processed += 1; })] }),
      storage: { engine: "memory" }, serve: true, runWorker: true, workerRuntime: { pollIntervalMs: 1 },
      sinks: [cleanupSink(() => { sinkClosed += 1; })],
    });
    let workerStopped = false;
    const activeTicks = started.host.drain.bind(started.host);
    started.host.drain = async (...args) => {
      if (calls.storageClose) workerStopped = true;
      return await activeTicks(...args);
    };
    await expect(started.stop()).rejects.toBe(failure);
    await Bun.sleep(15);
    expect(calls.storageClose).toBe(1);
    expect(calls.busStop).toBeGreaterThanOrEqual(1);
    expect(sinkClosed).toBe(1);
    expect(workerStopped).toBe(false);
    expect(processed).toBe(0);
  });

  test("registration failure closes storage and sinks while preserving the dependency error", async () => {
    const { calls, library } = createTrackedRuntime({ storageClose: new Error("cleanup failed") });
    let sinkClosed = 0;
    await expect(library.createChimpbase({
      registrations: [plugin({ name: "broken", dependsOn: ["missing"] })],
      storage: { engine: "memory" }, sinks: [
        cleanupSink(() => { sinkClosed += 1; throw new Error("sink failed"); }),
        cleanupSink(() => { sinkClosed += 1; }),
      ],
    })).rejects.toThrow("missing");
    expect(calls.storageClose).toBe(1);
    expect(sinkClosed).toBe(2);
  });

  test("secret-loading failure closes storage before an engine exists", async () => {
    const failure = new Error("secrets unavailable");
    const { calls, library } = createTrackedRuntime({ secrets: failure, storageClose: new Error("cleanup failed") });
    await expect(library.createChimpbase({ storage: { engine: "memory" } })).rejects.toBe(failure);
    expect(calls.storageClose).toBe(1);
  });

  for (const phase of ["secrets", "adapterCreate"] as const) {
    test(`failed ${phase} closes every supplied sink before an engine exists`, async () => {
      const failure = new Error(`${phase} unavailable`);
      const { calls, library } = createTrackedRuntime({ [phase]: failure, storageClose: new Error("cleanup failed") });
      const closed: number[] = [];
      await expect(library.createChimpbase({
        storage: { engine: "memory" }, sinks: [
          cleanupSink(() => { closed.push(1); throw new Error("sink failed"); }),
          cleanupSink(() => { closed.push(2); }),
        ],
      })).rejects.toBe(failure);
      expect(calls.storageClose).toBe(1);
      expect(closed).toEqual([1, 2]);
    });
  }

  for (const phase of ["named migrations", "inline migrations"] as const) {
    test(`failed SQLite ${phase} closes the database and supplied sinks`, async () => {
      const { library } = createTrackedRuntime();
      const close = spyOn(Database.prototype, "close");
      let sinkClosed = 0;
      try {
        await expect(library.createChimpbase({
          storage: { engine: "memory" },
          ...(phase === "named migrations"
            ? { migrations: { sqlite: [{ name: "invalid", sql: "INVALID MIGRATION" }] } }
            : { migrationsSql: ["INVALID MIGRATION"] }),
          sinks: [cleanupSink(() => { sinkClosed += 1; throw new Error("sink failed"); })],
        })).rejects.toThrow("INVALID");
        expect(sinkClosed).toBe(1);
        expect(close).toHaveBeenCalledTimes(1);
      } finally {
        close.mockRestore();
      }
    });
  }

  test("failed event-bus startup closes sinks and storage even when bus stop throws", async () => {
    const failure = new Error("bus unavailable");
    const { calls, library } = createTrackedRuntime({ busStart: failure, busStop: new Error("bus stop failed") });
    let sinkClosed = 0;
    await expect(library.startChimpbaseApp({
      app: defineChimpbaseApp({}), storage: { engine: "memory" }, serve: true, runWorker: true,
      sinks: [cleanupSink(() => { sinkClosed += 1; })],
    })).rejects.toBe(failure);
    expect(calls.serverStop).toBe(1);
    expect(calls.storageClose).toBe(1);
    expect(sinkClosed).toBe(1);
  });

  test("direct close reports a bus-stop failure after closing sinks and storage", async () => {
    const failure = new Error("bus stop failed");
    const { calls, library } = createTrackedRuntime({ busStop: failure });
    let sinkClosed = 0;
    const host = await library.createChimpbase({
      storage: { engine: "memory" }, sinks: [cleanupSink(() => { sinkClosed += 1; })],
    });
    await expect(host.close()).rejects.toBe(failure);
    expect(calls.storageClose).toBe(1);
    expect(sinkClosed).toBe(1);
  });

  test("direct close reports all sink failures after attempting every shutdown", async () => {
    const { calls, library } = createTrackedRuntime();
    const failures = [new Error("first sink failed"), new Error("second sink failed")];
    const closed: number[] = [];
    const host = await library.createChimpbase({
      storage: { engine: "memory" }, sinks: [
        ...failures.map((failure, index) => cleanupSink(() => { closed.push(index); throw failure; })),
        cleanupSink(() => { closed.push(2); }),
      ],
    });
    const error: unknown = await host.close().catch((failure: unknown) => failure);
    expect(error).toBeInstanceOf(AggregateError);
    if (!(error instanceof AggregateError)) throw new Error("expected aggregate cleanup error");
    expect(error.errors as unknown[]).toEqual(failures);
    expect(closed).toEqual([0, 1, 2]);
    expect(calls.storageClose).toBe(1);
  });

  test("direct close retains independent bus, sink, and storage failures", async () => {
    const failures = [new Error("bus failed"), new Error("sink failed"), new Error("storage failed")];
    const { calls, library } = createTrackedRuntime({ busStop: failures[0], storageClose: failures[2] });
    const host = await library.createChimpbase({
      storage: { engine: "memory" }, sinks: [cleanupSink(() => { throw failures[1]; })],
    });
    const error: unknown = await host.close().catch((failure: unknown) => failure);
    expect(error).toBeInstanceOf(AggregateError);
    if (!(error instanceof AggregateError)) throw new Error("expected aggregate cleanup error");
    expect(error.errors as unknown[]).toEqual(failures);
    expect(calls.storageClose).toBe(1);
  });
});
