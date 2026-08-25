import { afterEach, describe, expect, test } from "bun:test";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";

import { createChimpbase } from "../packages/bun/src/library.ts";
import { defineChimpbaseApp, normalizeProjectConfig } from "../packages/core/index.ts";
import { action, cron, v, worker } from "../packages/runtime/index.ts";
import { ChimpbaseBunHost, bunRuntimeShim } from "../packages/bun/src/runtime.ts";
import { createRuntimeHost } from "../packages/host/src/runtime.ts";
import type {
  ChimpbaseSinkSpan,
  ChimpbaseTelemetrySink,
  ChimpbaseValidator,
} from "../packages/runtime/index.ts";

interface SinkCall {
  args: unknown[];
  method: string;
}
const scopeValidator = v.object({ kind: v.string(), name: v.string() });

function parseCallArg<T>(
  call: SinkCall,
  index: number,
  validator: ChimpbaseValidator<T>,
  label: string,
): T {
  return validator.parse(call.args[index], label);
}


function createMockSink() {
  const calls: SinkCall[] = [];
  const spanEnds: Array<{ status: string; errorMessage?: string }> = [];
  const handlerSpanEnds: Array<{ status: string; errorMessage?: string }> = [];
  let runInContextCalled = false;

  const sink: ChimpbaseTelemetrySink = {
    onLog(scope, level, message, attributes) {
      calls.push({ method: "onLog", args: [scope, level, message, attributes] });
    },
    onMetric(scope, name, value, labels) {
      calls.push({ method: "onMetric", args: [scope, name, value, labels] });
    },
    startSpan(scope, name, attributes): ChimpbaseSinkSpan {
      calls.push({ method: "startSpan", args: [scope, name, attributes] });
      return {
        setAttribute(key, value) {
          calls.push({ method: "span.setAttribute", args: [key, value] });
        },
        end(status, errorMessage) {
          spanEnds.push({ status, errorMessage });
        },
      };
    },
    startHandlerSpan(scope): ChimpbaseSinkSpan {
      calls.push({ method: "startHandlerSpan", args: [scope] });
      const handlerSpan = {
        active: true,
        setAttribute(key: string, value: string | number | boolean) {
          calls.push({ method: "handlerSpan.setAttribute", args: [key, value] });
        },
        end(status: "error" | "ok", errorMessage?: string) {
          handlerSpanEnds.push({ status, errorMessage });
        },
        runInContext<T>(fn: () => T | Promise<T>): T | Promise<T> {
          if (!this.active) throw new Error("handler span receiver missing");
          runInContextCalled = true;
          return fn();
        },
      };
      return handlerSpan;
    },
  };

  return { calls, handlerSpanEnds, runInContextCalled: () => runInContextCalled, sink, spanEnds };
}

const cleanupHosts: ChimpbaseBunHost[] = [];
const cleanupDirs: string[] = [];

afterEach(async () => {
  while (cleanupHosts.length > 0) {
    await cleanupHosts.pop()?.close();
  }
  while (cleanupDirs.length > 0) {
    const dir = cleanupDirs.pop();
    if ((dir !== undefined && dir.length > 0)) {
      await rm(dir, { recursive: true, force: true });
    }
  }
});

async function createHostWithSink(sink: ChimpbaseTelemetrySink) {
  const dir = await mkdtemp(join(tmpdir(), "chimpbase-sink-test-"));
  cleanupDirs.push(dir);
  await writeFile(join(dir, "package.json"), "{}");

  const host = await createChimpbase({
    app: defineChimpbaseApp({
      project: { name: "sink-test" },
      registrations: [],
    }),
    projectDir: dir,
    sinks: [sink],
    storage: { engine: "memory" },
  });
  cleanupHosts.push(host);
  return host;
}

async function createHostWithCleanup(
  sink: ChimpbaseTelemetrySink,
  storageClose: () => void | Promise<void>,
) {
  const dir = await mkdtemp(join(tmpdir(), "chimpbase-shutdown-test-"));
  cleanupDirs.push(dir);
  await writeFile(join(dir, "package.json"), "{}");

  const runtime = {
    ...bunRuntimeShim,
    storage: {
      async open(...args: Parameters<typeof bunRuntimeShim.storage.open>) {
        const resources = await bunRuntimeShim.storage.open(...args);
        return {
          ...resources,
          storage: {
            async close() {
              try {
                await storageClose();
              } finally {
                await resources.storage.close();
              }
            },
          },
        };
      },
    },
  };

  return await createRuntimeHost(ChimpbaseBunHost, runtime, {
    config: normalizeProjectConfig({
      project: { name: "shutdown-test" },
      storage: { engine: "memory" },
    }),
    projectDir: dir,
    secrets: { get: () => null },
    sinks: [sink],
  });
}

describe("telemetry sink interface", () => {
  test("onLog is called when ctx.log is used", async () => {
    const mock = createMockSink();
    const host = await createHostWithSink(mock.sink);

    host.register(
      action("logAction", async (ctx) => {
        ctx.log.info("hello sink", { key: "value" });
      }),
    );

    await host.executeAction("logAction");

    const logCalls = mock.calls.filter((c) => c.method === "onLog");
    expect(logCalls.length).toBe(1);
    const scope = parseCallArg(logCalls[0], 0, scopeValidator, "log scope");
    const level = parseCallArg(logCalls[0], 1, v.string(), "log level");
    const message = parseCallArg(logCalls[0], 2, v.string(), "log message");
    const attributes = parseCallArg(logCalls[0], 3, v.record(v.unknown()), "log attributes");
    expect(scope).toEqual({ kind: "action", name: "logAction" });
    expect(level).toBe("info");
    expect(message).toBe("hello sink");
    expect(attributes).toEqual({ key: "value" });
  });

  test("onMetric is called when ctx.metric is used", async () => {
    const mock = createMockSink();
    const host = await createHostWithSink(mock.sink);

    host.register(
      action("metricAction", async (ctx) => {
        ctx.metric("requests", 42, { endpoint: "/api" });
      }),
    );

    await host.executeAction("metricAction");

    const metricCalls = mock.calls.filter((c) => c.method === "onMetric");
    expect(metricCalls.length).toBe(1);
    const scope = parseCallArg(metricCalls[0], 0, scopeValidator, "metric scope");
    const name = parseCallArg(metricCalls[0], 1, v.string(), "metric name");
    const value = parseCallArg(metricCalls[0], 2, v.number(), "metric value");
    const labels = parseCallArg(metricCalls[0], 3, v.record(v.unknown()), "metric labels");
    expect(scope).toEqual({ kind: "action", name: "metricAction" });
    expect(name).toBe("requests");
    expect(value).toBe(42);
    expect(labels).toEqual({ endpoint: "/api" });
  });

  test("startSpan is called and ended with ok on successful ctx.trace", async () => {
    const mock = createMockSink();
    const host = await createHostWithSink(mock.sink);

    host.register(
      action("traceAction", async (ctx) => {
        return await ctx.trace("doWork", async () => "result");
      }),
    );

    await host.executeAction("traceAction");

    const spanCalls = mock.calls.filter((c) => c.method === "startSpan");
    expect(spanCalls.length).toBe(1);
    const scope = parseCallArg(spanCalls[0], 0, scopeValidator, "span scope");
    const name = parseCallArg(spanCalls[0], 1, v.string(), "span name");
    expect(scope).toEqual({ kind: "action", name: "traceAction" });
    expect(name).toBe("doWork");

    expect(mock.spanEnds.length).toBe(1);
    expect(mock.spanEnds[0].status).toBe("ok");
  });

  test("startSpan is ended with error when ctx.trace throws", async () => {
    const mock = createMockSink();
    const host = await createHostWithSink(mock.sink);

    host.register(
      action("traceError", async (ctx) => {
        await ctx.trace("failingWork", async () => {
          throw new Error("boom");
        });
      }),
    );

    try {
      await host.executeAction("traceError");
    } catch {
      // expected
    }

    expect(mock.spanEnds.length).toBe(1);
    expect(mock.spanEnds[0].status).toBe("error");
    expect(mock.spanEnds[0].errorMessage).toBe("boom");
  });

  test("startHandlerSpan is called for action execution", async () => {
    const mock = createMockSink();
    const host = await createHostWithSink(mock.sink);

    host.register(
      action("spanAction", async () => "ok"),
    );

    await host.executeAction("spanAction");

    const handlerSpanCalls = mock.calls.filter((c) => c.method === "startHandlerSpan");
    expect(handlerSpanCalls.length).toBe(1);
    const scope = parseCallArg(handlerSpanCalls[0], 0, scopeValidator, "handler span scope");
    expect(scope).toEqual({ kind: "action", name: "spanAction" });

    expect(mock.handlerSpanEnds.length).toBe(1);
    expect(mock.handlerSpanEnds[0].status).toBe("ok");
  });

  test("handler span ends with error when action throws", async () => {
    const mock = createMockSink();
    const host = await createHostWithSink(mock.sink);

    host.register(
      action("failAction", async () => {
        throw new Error("action failed");
      }),
    );

    try {
      await host.executeAction("failAction");
    } catch {
      // expected
    }

    expect(mock.handlerSpanEnds.length).toBe(1);
    expect(mock.handlerSpanEnds[0].status).toBe("error");
    expect(mock.handlerSpanEnds[0].errorMessage).toBe("action failed");
  });

  test("runInContext is called to wrap handler execution", async () => {
    const mock = createMockSink();
    const host = await createHostWithSink(mock.sink);

    host.register(
      action("contextAction", async () => "wrapped"),
    );

    await host.executeAction("contextAction");

    expect(mock.runInContextCalled()).toBe(true);
  });

  test("drainTelemetryRecords still works alongside sinks", async () => {
    const mock = createMockSink();
    const host = await createHostWithSink(mock.sink);

    host.register(
      action("dualAction", async (ctx) => {
        ctx.log.info("dual message");
        ctx.metric("dual_metric", 1);
        return await ctx.trace("dual_trace", async () => "ok");
      }),
    );

    await host.executeAction("dualAction");

    // Sink received calls
    expect(mock.calls.filter((c) => c.method === "onLog").length).toBe(1);
    expect(mock.calls.filter((c) => c.method === "onMetric").length).toBe(1);
    expect(mock.calls.filter((c) => c.method === "startSpan").length).toBe(1);

    // Buffer still works
    const records = host.drainTelemetryRecords();
    expect(records.some((r) => r.kind === "log")).toBe(true);
    expect(records.some((r) => r.kind === "metric")).toBe(true);
    expect(records.some((r) => r.kind === "trace")).toBe(true);
  });

  test("stream persistence still works alongside sinks", async () => {
    const dir = await mkdtemp(join(tmpdir(), "chimpbase-sink-persist-test-"));
    cleanupDirs.push(dir);
    await writeFile(join(dir, "package.json"), "{}");

    const mock = createMockSink();
    const host = await createChimpbase({
      app: defineChimpbaseApp({
        project: { name: "sink-persist-test" },
        registrations: [],
        telemetry: { persist: { log: true } },
      }),
      projectDir: dir,
      sinks: [mock.sink],
      storage: { engine: "memory" },
    });
    cleanupHosts.push(host);

    host.register(
      action("persistAction", async (ctx) => {
        ctx.log.info("persisted and sinked");
      }),
    );

    await host.executeAction("persistAction");

    // Sink received log
    expect(mock.calls.filter((c) => c.method === "onLog").length).toBe(1);

    // Stream persistence also happened
    host.register(
      action("__readLogs", async (ctx) => {
        return await ctx.stream.read("_chimpbase.logs");
      }),
    );
    const result = await host.executeAction("__readLogs");
    const streamLogs = v.unknown().array().parse(result.result, "persisted log stream");
    expect(streamLogs.length).toBeGreaterThan(0);
  });

  test("startHandlerSpan is called for queue worker execution", async () => {
    const mock = createMockSink();
    const host = await createHostWithSink(mock.sink);

    host.register(
      action("enqueueAction", async (ctx) => {
        // Legacy alias remains supported during the ctx.enqueue migration window.
        await ctx.queue.enqueue("test.worker", { data: "hello" });
      }),
      worker("test.worker", async (ctx, payload) => {
        ctx.log.info("processing", { data: (payload as { data: string }).data });
      }),
    );

    await host.executeAction("enqueueAction");
    await host.drain({ maxRuns: 5 });

    // Should have handler spans for both action and queue worker
    const handlerSpanCalls = mock.calls.filter((c) => c.method === "startHandlerSpan");
    const scopes = handlerSpanCalls.map((call) =>
      parseCallArg(call, 0, scopeValidator, "worker handler span scope")
    );

    expect(scopes.some((s) => s.kind === "action" && s.name === "enqueueAction")).toBe(true);
    expect(scopes.some((s) => s.kind === "queue" && s.name === "test.worker")).toBe(true);
  });

  test("startHandlerSpan is called for cron execution", async () => {
    const mock = createMockSink();
    let now = Date.now();

    const dir = await mkdtemp(join(tmpdir(), "chimpbase-sink-cron-test-"));
    cleanupDirs.push(dir);
    await writeFile(join(dir, "package.json"), "{}");

    const host = await createRuntimeHost(ChimpbaseBunHost, bunRuntimeShim, {
      app: defineChimpbaseApp({
        project: { name: "sink-cron-test" },
        registrations: [],
        worker: { retryDelayMs: 0 },
      }),
      config: normalizeProjectConfig({
        project: { name: "sink-cron-test" },
        storage: { engine: "memory" },
        worker: { retryDelayMs: 0 },
      }),
      platform: {
        hashString: (input: string) => `hash:${input}`,
        now: () => now,
        randomUUID: () => crypto.randomUUID(),
      },
      projectDir: dir,
      secrets: { get: () => null },
      sinks: [mock.sink],
    });
    cleanupHosts.push(host);

    host.register(
      cron("test.cleanup", "*/5 * * * *", async (ctx) => {
        ctx.log.info("running cron");
      }),
      action("__listSchedules", async (ctx) =>
        await ctx.db.query(
          "SELECT next_fire_at_ms FROM _chimpbase_cron_schedules ORDER BY schedule_name ASC",
          undefined,
          v.object({ next_fire_at_ms: v.number() }),
        ),
      ),
    );

    await host.syncCronSchedules();

    // Advance time past the next fire time
    const schedules = await host.executeAction("__listSchedules");
    const rows = v.object({ next_fire_at_ms: v.number() }).array().parse(
      schedules.result,
      "cron schedule rows",
    );
    if (rows.length > 0) {
      now = rows[0].next_fire_at_ms;
    }

    await host.drain({ maxRuns: 5 });

    const handlerSpanCalls = mock.calls.filter((c) => c.method === "startHandlerSpan");
    const scopes = handlerSpanCalls.map((call) =>
      parseCallArg(call, 0, scopeValidator, "cron handler span scope")
    );

    // Cron goes through the queue path first, then processCronQueuePayload creates a cron-scoped span
    expect(scopes.some((s) => s.kind === "cron" && s.name === "test.cleanup")).toBe(true);
  });

  test("startHandlerSpan is called for route execution", async () => {
    const mock = createMockSink();
    const dir = await mkdtemp(join(tmpdir(), "chimpbase-sink-route-test-"));
    cleanupDirs.push(dir);
    await writeFile(join(dir, "package.json"), "{}");

    const host = await createChimpbase({
      app: defineChimpbaseApp({
        project: { name: "sink-route-test" },
        httpHandler: async (req) => new Response("ok"),
        registrations: [],
      }),
      projectDir: dir,
      sinks: [mock.sink],
      storage: { engine: "memory" },
    });
    cleanupHosts.push(host);

    await host.executeRoute(new Request("http://test.local/api/hello"));

    const handlerSpanCalls = mock.calls.filter((c) => c.method === "startHandlerSpan");
    expect(handlerSpanCalls.length).toBeGreaterThanOrEqual(1);

    const scopes = handlerSpanCalls.map((call) =>
      parseCallArg(call, 0, scopeValidator, "route handler span scope")
    );
    expect(scopes.some((s) => s.kind === "action" && s.name.includes("route:"))).toBe(true);
  });
});

describe("runtime shutdown", () => {
  test("waits for sink and storage cleanup", async () => {
    let releaseSink!: () => void;
    let releaseStorage!: () => void;
    let sinkStarted = false;
    let storageStarted = false;
    const sinkCleanup = new Promise<void>((resolve) => {
      releaseSink = resolve;
    });
    const storageCleanup = new Promise<void>((resolve) => {
      releaseStorage = resolve;
    });
    const sink = createMockSink().sink;
    sink.shutdown = () => {
      sinkStarted = true;
      return sinkCleanup;
    };
    const host = await createHostWithCleanup(sink, () => {
      storageStarted = true;
      return storageCleanup;
    });

    let closed = false;
    const closing = host.close().then(() => {
      closed = true;
    });
    await Promise.resolve();
    expect(sinkStarted).toBe(true);
    expect(storageStarted).toBe(true);
    expect(closed).toBe(false);

    releaseSink();
    await Promise.resolve();
    expect(closed).toBe(false);

    releaseStorage();
    await closing;
    expect(closed).toBe(true);
  });

  test("reports sink cleanup rejection after storage settles", async () => {
    let releaseStorage!: () => void;
    const storageCleanup = new Promise<void>((resolve) => {
      releaseStorage = resolve;
    });
    const sink = createMockSink().sink;
    sink.shutdown = () => Promise.reject(new Error("sink cleanup failed"));
    const host = await createHostWithCleanup(sink, () => storageCleanup);

    let rejected = false;
    const closing = host.close().catch((error: unknown) => {
      rejected = true;
      throw error;
    });
    for (let i = 0; i < 5; i++) await Promise.resolve();
    expect(rejected).toBe(false);

    releaseStorage();
    await expect(closing).rejects.toThrow("sink cleanup failed");
  });

  test("reports storage cleanup rejection", async () => {
    const host = await createHostWithCleanup(createMockSink().sink, () => {
      return Promise.reject(new Error("storage cleanup failed"));
    });

    await expect(host.close()).rejects.toThrow("storage cleanup failed");
  });
});
