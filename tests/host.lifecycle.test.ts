import { describe, expect, test } from "bun:test";

import { defineChimpbaseApp } from "../packages/core/index.ts";
import { bunRuntimeShim } from "../packages/bun/src/runtime.ts";
import { createChimpbaseRuntimeLibrary } from "../packages/host/src/library.ts";
import { ChimpbaseHost, type ChimpbaseRuntimeShim } from "../packages/host/src/runtime.ts";
import { action, onStart, onStop, plugin, worker } from "../packages/runtime/index.ts";

class TestHost extends ChimpbaseHost<{ port: number }> {}

function cleanupSink(shutdown: () => void) {
  const span = { setAttribute() {}, end() {} };
  return {
    onLog() {}, onMetric() {}, startSpan: () => span, startHandlerSpan: () => span,
    async shutdown() { shutdown(); },
  };
}

function createTrackedRuntime(errors: {
  serverStart?: Error; serverStop?: Error; busStart?: Error; storageClose?: Error; secrets?: Error;
} = {}) {
  const calls = { serverStart: 0, serverStop: 0, busStart: 0, busStop: 0, storageClose: 0 };
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
          eventBus: {
            async publish() {},
            start() {
              calls.busStart += 1;
              if (errors.busStart) throw errors.busStart;
            },
            stop() { calls.busStop += 1; },
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
  return { calls, library: createChimpbaseRuntimeLibrary(TestHost, runtime) };
}

describe("host lifecycle cleanup", () => {
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
      storage: { engine: "memory" }, sinks: [cleanupSink(() => { sinkClosed += 1; throw new Error("sink failed"); })],
    })).rejects.toThrow("missing");
    expect(calls.storageClose).toBe(1);
    expect(sinkClosed).toBe(1);
  });

  test("secret-loading failure closes storage before an engine exists", async () => {
    const failure = new Error("secrets unavailable");
    const { calls, library } = createTrackedRuntime({ secrets: failure, storageClose: new Error("cleanup failed") });
    await expect(library.createChimpbase({ storage: { engine: "memory" } })).rejects.toBe(failure);
    expect(calls.storageClose).toBe(1);
  });
});
