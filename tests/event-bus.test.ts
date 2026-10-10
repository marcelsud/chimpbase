import { describe, expect, test } from "bun:test";
import { AsyncResource } from "node:async_hooks";

import {
  createChimpbaseRegistry,
  createDefaultChimpbasePlatformShim,
  normalizeProjectConfig,
  NoopEventBus,
  type ChimpbaseEventBus,
  type ChimpbaseEventBusCallback,
  type ChimpbaseEventRecord,
} from "../packages/core/index.ts";
import { ChimpbaseEngine } from "../packages/core/engine.ts";
import { ChimpbaseHost } from "../packages/host/src/runtime.ts";
import { bunRuntimeShim } from "../packages/bun/src/runtime.ts";
import { Database } from "bun:sqlite";
import {
  createSqliteEngineAdapter,
  ensureSqliteInternalTables,
} from "../packages/bun/src/sqlite_adapter.ts";
import { action, v, type ChimpbaseContext } from "../packages/runtime/index.ts";

async function createTestEngine(
  eventBus: ChimpbaseEventBus,
  dispatch: "async" | "sync" = "sync",
  persistLogs = false,
) {
  const platform = createDefaultChimpbasePlatformShim();
  const registry = createChimpbaseRegistry();
  const db = new Database(":memory:");
  await ensureSqliteInternalTables(db);
  const adapter = createSqliteEngineAdapter(db, platform);

  const engine = new ChimpbaseEngine({
    adapter,
    eventBus,
    platform,
    registry,
    secrets: { get: () => null },
    subscriptions: { dispatch },
    telemetry: { minLevel: "debug", persist: { log: persistLogs, metric: false, trace: false } },
    worker: { leaseMs: 30_000, maxAttempts: 5, retryDelayMs: 0 },
  });

  return { adapter, db, engine, platform, registry };
}

describe("event bus", () => {
  for (const dispatch of ["sync", "async"] as const) {
    test(`${dispatch} bus delivery waits for an unrelated action rollback before ack`, async () => {
      let callback: ChimpbaseEventBusCallback | undefined;
      const bus: ChimpbaseEventBus = {
        publish: async () => {}, start(handler) { callback = handler; }, stop() {},
      };
      const { db, engine, platform, registry } = await createTestEngine(bus, dispatch);
      const entered = Promise.withResolvers<void>();
      const release = Promise.withResolvers<void>();
      registry.actions.set("blocked", action("blocked", async (ctx) => {
        await ctx.kv.set("rolled-back", true);
        entered.resolve();
        await release.promise;
        throw new Error("action failed");
      }));
      registry.actions.set("nested", action("nested", async (ctx) => {
        await ctx.kv.set("delivered", true);
      }));
      registry.subscriptions.set("external", [{
        name: "external", idempotent: true,
        handler: async (ctx) => { await ctx.action("nested"); },
      }]);
      const host = new ChimpbaseHost({
        config: normalizeProjectConfig({ storage: { engine: "memory" }, subscriptions: { dispatch } }),
        createWorkerEngine: () => engine, debugEnabled: false, engine, platform,
        projectDir: process.cwd(), registry, runtime: bunRuntimeShim,
        storage: { close() { db.close(); } }, supportsConcurrentWorkers: false,
      });
      const started = await host.start({ runWorker: false, serve: false });
      let acked = false;
      try {
        let deliver: (() => Promise<void>) | undefined;
        registry.actions.set("bindDelivery", action("bindDelivery", () => {
          deliver = AsyncResource.bind(async () => {
            if (callback === undefined) throw new Error("bus did not start");
            await callback([{ id: 42, name: "external", payload: {}, payloadJson: "{}" }], async () => {
              expect(db.inTransaction).toBe(false);
              acked = true;
            });
          });
        }));
        await host.executeAction("bindDelivery");
        const pendingAction = host.executeAction("blocked").catch((error: unknown) => error);
        await entered.promise;
        if (deliver === undefined) throw new Error("delivery did not bind");
        const delivery = deliver();
        await Promise.resolve();
        expect(acked).toBe(false);
        expect(db.query("SELECT key FROM _chimpbase_kv WHERE key = 'delivered'").all()).toEqual([]);
        release.resolve();
        expect(await pendingAction).toMatchObject({ message: "action failed" });
        await delivery;
        expect(acked).toBe(true);
        if (dispatch === "async") {
          expect(db.query("SELECT status FROM _chimpbase_queue_jobs").all()).toEqual([{ status: "pending" }]);
          await host.processNextQueueJob();
        }
        expect(db.query("SELECT key FROM _chimpbase_kv ORDER BY key").all()).toEqual([
          { key: "_chimpbase.sub.seen:42:external" }, { key: "delivered" },
        ]);
      } finally {
        release.resolve();
        await started.stop();
        await host.close();
      }
    });
  }

  test("failed bus delivery rolls back its idempotency reservation and can retry", async () => {
    let callback: ChimpbaseEventBusCallback | undefined;
    const { db, engine, registry } = await createTestEngine({
      publish: async () => {}, start(handler) { callback = handler; }, stop() {},
    });
    let calls = 0;
    registry.subscriptions.set("external", [{
      name: "retry", idempotent: true,
      handler: async (ctx) => {
        calls += 1;
        await ctx.kv.set("effect", calls);
        if (calls === 1) throw new Error("delivery failed");
      },
    }]);
    engine.startEventBus();
    const events = [{ id: 43, name: "external", payload: {}, payloadJson: "{}" }];
    try {
      if (callback === undefined) throw new Error("bus did not start");
      await expect(callback(events)).rejects.toThrow("delivery failed");
      expect(db.query("SELECT key FROM _chimpbase_kv").all()).toEqual([]);
      await callback(events);
      await callback(events);
      expect(calls).toBe(2);
      expect(db.query("SELECT key, value_json FROM _chimpbase_kv ORDER BY key").all()).toEqual([
        { key: "_chimpbase.sub.seen:43:retry", value_json: "true" }, { key: "effect", value_json: "2" },
      ]);
    } finally { engine.stopEventBus(); db.close(); }
  });

  for (const publisher of ["action", "route", "worker"] as const) {
    test(`failing cascaded sync subscription rolls back its ${publisher} publisher`, async () => {
      const published: ChimpbaseEventRecord[][] = [];
      let callback: ChimpbaseEventBusCallback | undefined;
      const bus: ChimpbaseEventBus = {
        publish: async (events) => {
          published.push(events);
          expect(db.inTransaction).toBe(false);
        },
        start(handler) { callback = handler; }, stop() {},
      };
      const { adapter, db, engine, registry } = await createTestEngine(bus);
      db.exec("CREATE TABLE effects (label TEXT)");
      const emit = async (ctx: ChimpbaseContext) => {
        await ctx.db.query("INSERT INTO effects VALUES ('publisher')");
        ctx.pubsub.publish("first", {});
      };
      registry.actions.set("emit", action("emit", emit));
      registry.httpHandler = async (_request, env) => {
        await env.action("emit");
        return new Response("ok");
      };
      registry.workers.set("emit", { name: "emit", definition: { dlq: false }, handler: emit });
      registry.subscriptions.set("first", [{
        name: "first", idempotent: true,
        handler: async (ctx) => {
          await ctx.db.query("INSERT INTO effects VALUES ('subscriber')");
          await ctx.enqueue("followup", {});
          ctx.pubsub.publish("second", {});
        },
      }]);
      let shouldFail = true;
      registry.subscriptions.set("second", [{
        name: "second", idempotent: true,
        handler: async (ctx) => {
          await ctx.db.query("INSERT INTO effects VALUES ('cascade')");
          if (shouldFail) throw new Error("cascade failed");
        },
      }]);
      const invoke = publisher === "action"
        ? () => engine.executeAction("emit")
        : publisher === "route"
        ? () => engine.executeRoute(new Request("http://localhost/emit"))
        : () => engine.processNextQueueJob();
      try {
        engine.startEventBus();
        if (publisher === "worker") await adapter.queueEnqueue("emit", {});
        await expect(invoke()).rejects.toThrow("cascade failed");
        expect(db.query("SELECT * FROM effects").all()).toEqual([]);
        expect(db.query("SELECT * FROM _chimpbase_events").all()).toEqual([]);
        expect(db.query("SELECT * FROM _chimpbase_kv").all()).toEqual([]);
        expect(db.query("SELECT * FROM _chimpbase_queue_jobs WHERE queue_name = 'followup'").all()).toEqual([]);
        expect(published).toEqual([]);

        shouldFail = false;
        const result = await invoke();
        expect(result?.emittedEvents.map((event) => event.name)).toEqual(["first", "second"]);
        expect(db.query("SELECT label FROM effects ORDER BY rowid").all()).toEqual([
          { label: "publisher" }, { label: "subscriber" }, { label: "cascade" },
        ]);
        const ids = result?.emittedEvents.map((event) => event.id);
        expect(ids).toEqual(db.query("SELECT id FROM _chimpbase_events ORDER BY id").all().map((row) =>
          v.object({ id: v.number() }).parse(row).id
        ));
        expect(db.query("SELECT key FROM _chimpbase_kv ORDER BY key").all()).toEqual([
          { key: `_chimpbase.sub.seen:${ids?.[0]}:first` },
          { key: `_chimpbase.sub.seen:${ids?.[1]}:second` },
        ]);
        expect(published).toHaveLength(1);
        await callback?.(published[0]);
        expect(db.query("SELECT COUNT(*) AS count FROM effects").get()).toEqual({ count: 3 });
      } finally {
        engine.stopEventBus();
        db.close();
      }
    });
  }

  for (const failure of ["completion", "commit"] as const) {
    test(`queue ${failure} failure rolls back worker effects before retry`, async () => {
      const { adapter, db, engine, registry } = await createTestEngine(new NoopEventBus());
      db.exec("CREATE TABLE effects (label TEXT)");
      registry.workers.set("work", {
        name: "work", definition: { dlq: false },
        handler: async (ctx) => {
          await ctx.db.query("INSERT INTO effects VALUES ('work')");
          ctx.pubsub.publish("worked", {});
        },
      });
      const complete = adapter.completeQueueJob.bind(adapter);
      const commit = adapter.commitTransaction.bind(adapter);
      if (failure === "completion") {
        adapter.completeQueueJob = async (id) => {
          await complete(id);
          throw new Error("completion failed");
        };
      } else {
        adapter.commitTransaction = async () => {
          adapter.commitTransaction = commit;
          throw new Error("commit failed");
        };
      }
      try {
        await adapter.queueEnqueue("work", {});
        await expect(engine.processNextQueueJob()).rejects.toThrow(`${failure} failed`);
        expect(db.query("SELECT * FROM effects").all()).toEqual([]);
        expect(db.query("SELECT * FROM _chimpbase_events").all()).toEqual([]);
        expect(db.query("SELECT status, attempt_count FROM _chimpbase_queue_jobs").all())
          .toEqual([{ status: "pending", attempt_count: 1 }]);

        adapter.completeQueueJob = complete;
        adapter.commitTransaction = commit;
        const result = await engine.processNextQueueJob();
        expect(result?.emittedEvents.map((event) => event.name)).toEqual(["worked"]);
        expect(db.query("SELECT label FROM effects").all()).toEqual([{ label: "work" }]);
        expect(db.query("SELECT status, attempt_count FROM _chimpbase_queue_jobs").all())
          .toEqual([{ status: "completed", attempt_count: 2 }]);
        expect(await engine.processNextQueueJob()).toBeNull();
      } finally {
        db.close();
      }
    });
  }

  test("telemetry failure after commit does not retry a completed worker or call onFailure", async () => {
    const { adapter, db, engine, registry } = await createTestEngine(new NoopEventBus(), "sync", true);
    db.exec("CREATE TABLE effects (label TEXT)");
    let failures = 0;
    registry.workers.set("work", {
      name: "work", definition: { dlq: false, onFailure: () => { failures += 1; } },
      handler: async (ctx) => {
        await ctx.db.query("INSERT INTO effects VALUES ('work')");
        ctx.log.info("work completed");
      },
    });
    adapter.streamAppend = async () => { throw new Error("telemetry failed"); };
    try {
      await adapter.queueEnqueue("work", {});
      await expect(engine.processNextQueueJob()).rejects.toThrow("telemetry failed");
      expect(db.query("SELECT label FROM effects").all()).toEqual([{ label: "work" }]);
      expect(db.query("SELECT status, attempt_count FROM _chimpbase_queue_jobs").all())
        .toEqual([{ status: "completed", attempt_count: 1 }]);
      expect(failures).toBe(0);
      expect(await engine.processNextQueueJob()).toBeNull();
    } finally {
      db.close();
    }
  });

  for (const dispatch of ["sync", "async"] as const) {
    test(`${dispatch} subscriptions deliver a three-event chain once in order`, async () => {
      const { db, engine, registry } = await createTestEngine(new NoopEventBus(), dispatch);
      const calls: string[] = [];
      registry.actions.set("cascade", action("cascade", async (ctx) => {
        ctx.pubsub.publish("first", { position: 1 });
      }));
      for (const [index, name] of ["first", "second", "third"].entries()) {
        registry.subscriptions.set(name, [{
          name: `on-${name}`, idempotent: true,
          handler: async (ctx, payload) => {
            calls.push(name);
            expect(v.object({ position: v.number() }).parse(payload)).toEqual({ position: index + 1 });
            const next = ["second", "third"][index];
            if (next !== undefined) ctx.pubsub.publish(next, { position: index + 2 });
          },
        }]);
      }
      try {
        const result = await engine.executeAction("cascade");
        if (dispatch === "sync") {
          expect(result.emittedEvents.map((event) => event.name)).toEqual(["first", "second", "third"]);
          expect(calls).toEqual(["first", "second", "third"]);
        } else {
          expect(result.emittedEvents.map((event) => event.name)).toEqual(["first"]);
          expect(calls).toEqual([]);
          for (const name of ["first", "second", "third"]) {
            expect(await engine.processNextQueueJob()).not.toBeNull();
            expect(calls.at(-1)).toBe(name);
          }
          expect(calls).toEqual(["first", "second", "third"]);
        }
        expect(await engine.processNextQueueJob()).toBeNull();
      } finally {
        db.close();
      }
    });
  }

  test("NoopEventBus publish and start are no-ops", async () => {
    const bus = new NoopEventBus();
    const received: ChimpbaseEventRecord[][] = [];

    await bus.publish([{ name: "test", payload: {}, payloadJson: "{}" }]);
    bus.start(async (events) => { received.push(events); });
    bus.stop();

    expect(received).toEqual([]);
  });

  test("custom event bus receives publish calls after action commit", async () => {
    const published: ChimpbaseEventRecord[][] = [];

    class SpyEventBus implements ChimpbaseEventBus {
      async publish(events: ChimpbaseEventRecord[]): Promise<void> {
        published.push(events);
      }
      start(_callback: ChimpbaseEventBusCallback): void {}
      stop(): void {}
    }

    const { engine, registry } = await createTestEngine(new SpyEventBus());

    registry.actions.set("emitEvent", action("emitEvent", async (ctx) => {
      ctx.pubsub.publish("order.created", { orderId: "123" });
    }));

    await engine.executeAction("emitEvent");

    expect(published).toHaveLength(1);
    expect(published[0]).toHaveLength(1);
    expect(published[0][0].name).toBe("order.created");
    expect(published[0][0].payload).toEqual({ orderId: "123" });
  });
  test("async subscriptions preserve undefined payloads", async () => {
    const { engine, registry } = await createTestEngine(new NoopEventBus(), "async");
    let received: unknown = "not-called";
    registry.actions.set("emitUndefined", action("emitUndefined", async (ctx) => {
      ctx.pubsub.publish("optional.payload", undefined);
    }));
    registry.subscriptions.set("optional.payload", [{
      handler: async (_ctx, payload) => {
        received = payload;
      },
      idempotent: false,
      name: "optional-payload",
    }]);

    await engine.executeAction("emitUndefined");
    await engine.processNextQueueJob();

    expect(received).toBeUndefined();
  });


  test("ack callback is invoked after subscriptions are dispatched", async () => {
    const ackCalls: number[] = [];
    const dispatched: string[] = [];
    let ackCallOrder = 0;

    class AckEventBus implements ChimpbaseEventBus {
      private callback: ChimpbaseEventBusCallback | null = null;

      async publish(_events: ChimpbaseEventRecord[]): Promise<void> {}

      start(callback: ChimpbaseEventBusCallback): void {
        this.callback = callback;
      }

      stop(): void {
        this.callback = null;
      }

      async simulateExternalEvents(events: ChimpbaseEventRecord[]): Promise<void> {
        if (!(this.callback !== null)) throw new Error("not started");
        await this.callback(events, async () => {
          ackCallOrder++;
          ackCalls.push(ackCallOrder);
        });
      }
    }

    const bus = new AckEventBus();
    const { engine, registry } = await createTestEngine(bus);

    registry.subscriptions.set("order.created", [
      {
        handler: async (_ctx, payload) => {
          dispatched.push(v.object({ orderId: v.string() }).parse(payload, "order event").orderId);
          _ctx.pubsub.publish("order.followup", payload);
        },
        idempotent: false,
        name: "",
      },
    ]);

    registry.subscriptions.set("order.followup", [{
      handler: async (ctx, payload) => { ctx.pubsub.publish("order.finished", payload); },
      idempotent: false, name: "followup",
    }]);
    registry.subscriptions.set("order.finished", [{
      handler: async (_ctx, payload) => {
        dispatched.push(`finished:${v.object({ orderId: v.string() }).parse(payload).orderId}`);
        expect(ackCalls).toEqual([]);
      },
      idempotent: true, name: "finished",
    }]);

    engine.startEventBus();

    await bus.simulateExternalEvents([
      { name: "order.created", payload: { orderId: "abc" }, payloadJson: '{"orderId":"abc"}' },
      { name: "order.created", payload: { orderId: "def" }, payloadJson: '{"orderId":"def"}' },
    ]);

    // Subscription handlers ran before ack
    expect(dispatched).toEqual(["abc", "def", "finished:abc", "finished:def"]);
    // Ack was called exactly once, after dispatch
    expect(ackCalls).toEqual([1]);

    engine.stopEventBus();
  });

  test("ack is not required — absent ack does not throw", async () => {
    class NoAckEventBus implements ChimpbaseEventBus {
      private callback: ChimpbaseEventBusCallback | null = null;

      async publish(_events: ChimpbaseEventRecord[]): Promise<void> {}

      start(callback: ChimpbaseEventBusCallback): void {
        this.callback = callback;
      }

      stop(): void {
        this.callback = null;
      }

      async simulateExternalEvents(events: ChimpbaseEventRecord[]): Promise<void> {
        if (!(this.callback !== null)) throw new Error("not started");
        await this.callback(events);
      }
    }

    const bus = new NoAckEventBus();
    const dispatched: string[] = [];
    const { engine, registry } = await createTestEngine(bus);

    registry.subscriptions.set("ping", [
      {
        handler: async (_ctx, payload) => {
          dispatched.push(v.object({ msg: v.string() }).parse(payload, "ping event").msg);
        },
        idempotent: false,
        name: "",
      },
    ]);

    engine.startEventBus();

    await bus.simulateExternalEvents([
      { name: "ping", payload: { msg: "hello" }, payloadJson: '{"msg":"hello"}' },
    ]);

    expect(dispatched).toEqual(["hello"]);

    engine.stopEventBus();
  });

  test("multiple events across different topics dispatch to correct subscriptions", async () => {
    class TestEventBus implements ChimpbaseEventBus {
      private callback: ChimpbaseEventBusCallback | null = null;

      async publish(_events: ChimpbaseEventRecord[]): Promise<void> {}

      start(callback: ChimpbaseEventBusCallback): void {
        this.callback = callback;
      }

      stop(): void {
        this.callback = null;
      }

      async simulateExternalEvents(events: ChimpbaseEventRecord[]): Promise<void> {
        if (!(this.callback !== null)) throw new Error("not started");
        await this.callback(events);
      }
    }

    const bus = new TestEventBus();
    const orderEvents: unknown[] = [];
    const userEvents: unknown[] = [];
    const { engine, registry } = await createTestEngine(bus);

    registry.subscriptions.set("order.created", [
      {
        handler: async (_ctx, payload) => { orderEvents.push(payload); },
        idempotent: false,
        name: "",
      },
    ]);
    registry.subscriptions.set("user.registered", [
      {
        handler: async (_ctx, payload) => { userEvents.push(payload); },
        idempotent: false,
        name: "",
      },
    ]);

    engine.startEventBus();

    await bus.simulateExternalEvents([
      { name: "order.created", payload: { id: 1 }, payloadJson: '{"id":1}' },
      { name: "user.registered", payload: { name: "alice" }, payloadJson: '{"name":"alice"}' },
      { name: "order.created", payload: { id: 2 }, payloadJson: '{"id":2}' },
      { name: "unknown.topic", payload: {}, payloadJson: '{}' },
    ]);

    expect(orderEvents).toEqual([{ id: 1 }, { id: 2 }]);
    expect(userEvents).toEqual([{ name: "alice" }]);

    engine.stopEventBus();
  });

  test("idempotent handler skips duplicate event with same id", async () => {
    class TestEventBus implements ChimpbaseEventBus {
      private callback: ChimpbaseEventBusCallback | null = null;

      async publish(_events: ChimpbaseEventRecord[]): Promise<void> {}

      start(callback: ChimpbaseEventBusCallback): void {
        this.callback = callback;
      }

      stop(): void {
        this.callback = null;
      }

      async simulateExternalEvents(events: ChimpbaseEventRecord[]): Promise<void> {
        if (!(this.callback !== null)) throw new Error("not started");
        await this.callback(events);
      }
    }

    const bus = new TestEventBus();
    const calls: unknown[] = [];
    const { engine, registry } = await createTestEngine(bus);

    registry.subscriptions.set("order.created", [
      {
        handler: async (_ctx, payload) => { calls.push(payload); },
        idempotent: true,
        name: "onOrderCreated",
      },
    ]);

    engine.startEventBus();

    await bus.simulateExternalEvents([
      { id: 42, name: "order.created", payload: { orderId: "a" }, payloadJson: '{"orderId":"a"}' },
    ]);
    await bus.simulateExternalEvents([
      { id: 42, name: "order.created", payload: { orderId: "a" }, payloadJson: '{"orderId":"a"}' },
    ]);

    expect(calls).toEqual([{ orderId: "a" }]);

    engine.stopEventBus();
  });

  test("idempotent handler processes event when id differs", async () => {
    class TestEventBus implements ChimpbaseEventBus {
      private callback: ChimpbaseEventBusCallback | null = null;

      async publish(_events: ChimpbaseEventRecord[]): Promise<void> {}

      start(callback: ChimpbaseEventBusCallback): void {
        this.callback = callback;
      }

      stop(): void {
        this.callback = null;
      }

      async simulateExternalEvents(events: ChimpbaseEventRecord[]): Promise<void> {
        if (!(this.callback !== null)) throw new Error("not started");
        await this.callback(events);
      }
    }

    const bus = new TestEventBus();
    const calls: unknown[] = [];
    const { engine, registry } = await createTestEngine(bus);

    registry.subscriptions.set("order.created", [
      {
        handler: async (_ctx, payload) => { calls.push(payload); },
        idempotent: true,
        name: "onOrderCreated",
      },
    ]);

    engine.startEventBus();

    await bus.simulateExternalEvents([
      { id: 1, name: "order.created", payload: { orderId: "a" }, payloadJson: '{"orderId":"a"}' },
    ]);
    await bus.simulateExternalEvents([
      { id: 2, name: "order.created", payload: { orderId: "b" }, payloadJson: '{"orderId":"b"}' },
    ]);

    expect(calls).toEqual([{ orderId: "a" }, { orderId: "b" }]);

    engine.stopEventBus();
  });

  test("non-idempotent handler processes duplicate events", async () => {
    class TestEventBus implements ChimpbaseEventBus {
      private callback: ChimpbaseEventBusCallback | null = null;

      async publish(_events: ChimpbaseEventRecord[]): Promise<void> {}

      start(callback: ChimpbaseEventBusCallback): void {
        this.callback = callback;
      }

      stop(): void {
        this.callback = null;
      }

      async simulateExternalEvents(events: ChimpbaseEventRecord[]): Promise<void> {
        if (!(this.callback !== null)) throw new Error("not started");
        await this.callback(events);
      }
    }

    const bus = new TestEventBus();
    const calls: unknown[] = [];
    const { engine, registry } = await createTestEngine(bus);

    registry.subscriptions.set("order.created", [
      {
        handler: async (_ctx, payload) => { calls.push(payload); },
        idempotent: false,
        name: "",
      },
    ]);

    engine.startEventBus();

    await bus.simulateExternalEvents([
      { id: 42, name: "order.created", payload: { orderId: "a" }, payloadJson: '{"orderId":"a"}' },
    ]);
    await bus.simulateExternalEvents([
      { id: 42, name: "order.created", payload: { orderId: "a" }, payloadJson: '{"orderId":"a"}' },
    ]);

    expect(calls).toEqual([{ orderId: "a" }, { orderId: "a" }]);

    engine.stopEventBus();
  });

  test("event without id bypasses idempotency check", async () => {
    class TestEventBus implements ChimpbaseEventBus {
      private callback: ChimpbaseEventBusCallback | null = null;

      async publish(_events: ChimpbaseEventRecord[]): Promise<void> {}

      start(callback: ChimpbaseEventBusCallback): void {
        this.callback = callback;
      }

      stop(): void {
        this.callback = null;
      }

      async simulateExternalEvents(events: ChimpbaseEventRecord[]): Promise<void> {
        if (!(this.callback !== null)) throw new Error("not started");
        await this.callback(events);
      }
    }

    const bus = new TestEventBus();
    const calls: unknown[] = [];
    const { engine, registry } = await createTestEngine(bus);

    registry.subscriptions.set("order.created", [
      {
        handler: async (_ctx, payload) => { calls.push(payload); },
        idempotent: true,
        name: "onOrderCreated",
      },
    ]);

    engine.startEventBus();

    await bus.simulateExternalEvents([
      { name: "order.created", payload: { orderId: "a" }, payloadJson: '{"orderId":"a"}' },
    ]);
    await bus.simulateExternalEvents([
      { name: "order.created", payload: { orderId: "a" }, payloadJson: '{"orderId":"a"}' },
    ]);

    // Without id, idempotency is skipped — handler runs both times
    expect(calls).toEqual([{ orderId: "a" }, { orderId: "a" }]);

    engine.stopEventBus();
  });
});
