import { describe, expect, test } from "bun:test";

import {
  createChimpbaseRegistry,
  createDefaultChimpbasePlatformShim,
  NoopEventBus,
  type ChimpbaseEventBus,
  type ChimpbaseEventBusCallback,
  type ChimpbaseEventRecord,
} from "../packages/core/index.ts";
import { ChimpbaseEngine } from "../packages/core/engine.ts";
import { Database } from "bun:sqlite";
import {
  createSqliteEngineAdapter,
  ensureSqliteInternalTables,
} from "../packages/bun/src/sqlite_adapter.ts";
import { action, v } from "../packages/runtime/index.ts";

async function createTestEngine(
  eventBus: ChimpbaseEventBus,
  dispatch: "async" | "sync" = "sync",
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
    telemetry: { minLevel: "debug", persist: { log: false, metric: false, trace: false } },
    worker: { leaseMs: 30_000, maxAttempts: 5, retryDelayMs: 0 },
  });

  return { db, engine, registry };
}

describe("event bus", () => {
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
