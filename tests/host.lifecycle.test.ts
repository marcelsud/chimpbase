import { describe, expect, spyOn, test } from "bun:test";
import { Database } from "bun:sqlite";

import { defineChimpbaseApp, type ChimpbaseEventBusCallback, type ChimpbaseEventRecord } from "../packages/core/index.ts";
import { bunRuntimeShim } from "../packages/bun/src/runtime.ts";
import { createChimpbaseRuntimeLibrary } from "../packages/host/src/library.ts";
import { ChimpbaseHost, type ChimpbaseRuntimeShim } from "../packages/host/src/runtime.ts";
import { action, onStart, onStop, plugin, route, subscription, worker } from "../packages/runtime/index.ts";

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
    calls, library: createChimpbaseRuntimeLibrary(TestHost, runtime),
    async deliverEvents(events: ChimpbaseEventRecord[]) {
      if (!deliver) throw new Error("event bus did not start");
      await deliver(events);
    },
  };
}

describe("host lifecycle cleanup", () => {
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

  test("close waits for active operations before closing storage and sinks", async () => {
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
      })],
    });
    const operation = host.executeAction("hold");
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
