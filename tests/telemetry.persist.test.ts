import { afterEach, describe, expect, test } from "bun:test";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";

import { createChimpbase } from "../packages/bun/src/library.ts";
import { createDefaultChimpbasePlatformShim, defineChimpbaseApp, normalizeProjectConfig } from "../packages/core/index.ts";
import { action, v, worker } from "../packages/runtime/index.ts";
import { ChimpbaseBunHost } from "../packages/bun/src/runtime.ts";

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

async function createMemoryHost(
  options: {
    telemetry?: {
      minLevel?: "debug" | "info" | "warn" | "error";
      persist?: { log?: boolean; metric?: boolean; trace?: boolean };
      retention?: { enabled?: boolean; maxAgeDays?: number; schedule?: string };
    };
  } = {},
) {
  const dir = await mkdtemp(join(tmpdir(), "chimpbase-telemetry-test-"));
  cleanupDirs.push(dir);
  await writeFile(join(dir, "package.json"), "{}");

  const host = await createChimpbase({
    app: defineChimpbaseApp({
      project: { name: "telemetry-test" },
      registrations: [],
      telemetry: options.telemetry,
    }),
    projectDir: dir,
    storage: { engine: "memory" },
  });
  cleanupHosts.push(host);
  return host;
}

describe("telemetry stream persistence", () => {
  for (const engine of ["memory", "sqlite"] as const) {
    test(`retention compares SQLite and ISO dates at the cutoff without deleting recent logs (${engine})`, async () => {
      const dir = await mkdtemp(join(tmpdir(), "chimpbase-telemetry-retention-"));
      cleanupDirs.push(dir);
      const now = Date.parse("2026-10-09T02:00:00.000Z");
      const host = await ChimpbaseBunHost.create({
        projectDir: dir,
        config: normalizeProjectConfig({
          storage: { engine, path: "telemetry.db" },
          telemetry: { retention: { enabled: true, maxAgeDays: 1 } },
        }),
        platform: { ...createDefaultChimpbasePlatformShim(), now: () => now },
      });
      cleanupHosts.push(host);
      const timestamps = [
        ["expired.sqlite", "2026-10-08 01:59:59.999"],
        ["expired.iso", "2026-10-08T01:59:59.999Z"],
        ["boundary.sqlite", "2026-10-08 02:00:00"],
        ["boundary.iso", "2026-10-08T02:00:00.000Z"],
        ["recent.sqlite", "2026-10-08 23:00:00"],
        ["recent.iso", "2026-10-08T23:00:00.000Z"],
      ];
      host.register(
        action("seedRetention", async (ctx) => {
          for (const stream of ["_chimpbase.logs", "_chimpbase.metrics", "_chimpbase.traces"]) {
            for (const [event, createdAt] of timestamps) {
              await ctx.db.query(
                "INSERT INTO _chimpbase_stream_events (stream_name, event_name, payload_json, created_at) VALUES (?1, ?2, '{}', ?3)",
                [stream, event, createdAt],
              );
            }
          }
          await ctx.db.query(
            "INSERT INTO _chimpbase_stream_events (stream_name, event_name, payload_json, created_at) VALUES ('app.activity', 'keep', '{}', '2000-01-01 00:00:00')",
          );
          await ctx.db.query(
            "UPDATE _chimpbase_cron_schedules SET next_fire_at_ms = ?1 WHERE schedule_name = '__chimpbase.telemetry.cleanup'",
            [now - 1],
          );
        }),
      );
      await host.syncCronSchedules();
      await host.executeAction("seedRetention");
      expect((await host.processNextCronSchedule())?.scheduleName).toBe("__chimpbase.telemetry.cleanup");
      expect(await host.processNextQueueJob()).not.toBeNull();
      for (const stream of ["_chimpbase.logs", "_chimpbase.metrics", "_chimpbase.traces"]) {
        expect((await readStream(host, stream)).map((event) => event.event)).toEqual([
          "boundary.sqlite", "boundary.iso", "recent.sqlite", "recent.iso",
        ]);
      }
      expect((await readStream(host, "app.activity")).map((event) => event.event)).toEqual(["keep"]);
    });
  }

  test("does not persist telemetry when disabled (default)", async () => {
    const host = await createMemoryHost();
    host.register(
      action("hello", async (ctx) => {
        ctx.log.info("hello world");
        ctx.metric("calls", 1);
        return await ctx.trace("greet", async () => "hi");
      }),
    );

    await host.executeAction("hello");

    const logs = await host.engine.createRouteEnv().action("__readStream", "_chimpbase.logs").catch(() => []);
    expect(logs).toEqual([]);

    // Verify in-memory telemetry still works
    const telemetry = host.drainTelemetryRecords();
    expect(telemetry.some((r) => r.kind === "log")).toBe(true);
    expect(telemetry.some((r) => r.kind === "metric")).toBe(true);
    expect(telemetry.some((r) => r.kind === "trace")).toBe(true);
  });

  test("persists logs to _chimpbase.logs stream", async () => {
    const host = await createMemoryHost({
      telemetry: { persist: { log: true } },
    });
    host.register(
      action("logTest", async (ctx) => {
        ctx.log.info("test message", { key: "val" });
        ctx.log.error("oops");
      }),
    );

    await host.executeAction("logTest");

    const events = await readStream(host, "_chimpbase.logs");
    expect(events).toHaveLength(2);
    expect(events[0].event).toBe("log.info");
    expect(events[0].payload).toMatchObject({
      message: "test message",
      attributes: { key: "val" },
      scope: { kind: "action", name: "logTest" },
    });
    expect(events[1].event).toBe("log.error");
    expect(events[1].payload).toMatchObject({ message: "oops" });

    // Metrics and traces should NOT be persisted
    const metrics = await readStream(host, "_chimpbase.metrics");
    expect(metrics).toHaveLength(0);
    const traces = await readStream(host, "_chimpbase.traces");
    expect(traces).toHaveLength(0);
  });

  test("persists metrics to _chimpbase.metrics stream", async () => {
    const host = await createMemoryHost({
      telemetry: { persist: { metric: true } },
    });
    host.register(
      action("metricTest", async (ctx) => {
        ctx.metric("req.count", 1, { endpoint: "/api" });
        ctx.metric("req.latency", 42.5);
      }),
    );

    await host.executeAction("metricTest");

    const events = await readStream(host, "_chimpbase.metrics");
    expect(events).toHaveLength(2);
    expect(events[0].event).toBe("metric");
    expect(events[0].payload).toMatchObject({
      name: "req.count",
      value: 1,
      labels: { endpoint: "/api" },
      scope: { kind: "action", name: "metricTest" },
    });
    expect(events[1].payload).toMatchObject({
      name: "req.latency",
      value: 42.5,
    });
  });

  test("persists traces to _chimpbase.traces stream", async () => {
    const host = await createMemoryHost({
      telemetry: { persist: { trace: true } },
    });
    host.register(
      action("traceTest", async (ctx) => {
        return await ctx.trace("my.span", async (span) => {
          span.setAttribute("foo", "bar");
          return 42;
        });
      }),
    );

    const result = await host.executeAction("traceTest");
    expect(result.result).toBe(42);

    const events = await readStream(host, "_chimpbase.traces");
    expect(events).toHaveLength(2);
    expect(events[0].event).toBe("trace.start");
    expect(events[0].payload).toMatchObject({
      name: "my.span",
      phase: "start",
      scope: { kind: "action", name: "traceTest" },
    });
    expect(events[1].event).toBe("trace.end");
    expect(events[1].payload).toMatchObject({
      name: "my.span",
      phase: "end",
      status: "ok",
    });
  });

  test("persists all three kinds when all enabled", async () => {
    const host = await createMemoryHost({
      telemetry: { persist: { log: true, metric: true, trace: true } },
    });
    host.register(
      action("allTelemetry", async (ctx) => {
        ctx.log.warn("warning!");
        ctx.metric("counter", 5);
        await ctx.trace("op", async () => {});
      }),
    );

    await host.executeAction("allTelemetry");

    const logs = await readStream(host, "_chimpbase.logs");
    const metrics = await readStream(host, "_chimpbase.metrics");
    const traces = await readStream(host, "_chimpbase.traces");

    expect(logs).toHaveLength(1);
    expect(metrics).toHaveLength(1);
    expect(traces).toHaveLength(2); // start + end
  });

  test("respects minLevel filter for logs", async () => {
    const host = await createMemoryHost({
      telemetry: { persist: { log: true }, minLevel: "warn" },
    });
    host.register(
      action("levelTest", async (ctx) => {
        ctx.log.debug("debug msg");
        ctx.log.info("info msg");
        ctx.log.warn("warn msg");
        ctx.log.error("error msg");
      }),
    );

    await host.executeAction("levelTest");

    const events = await readStream(host, "_chimpbase.logs");
    expect(events).toHaveLength(2);
    expect(events[0].event).toBe("log.warn");
    expect(events[1].event).toBe("log.error");
  });

  test("per-handler override opts out when global is enabled", async () => {
    const host = await createMemoryHost({
      telemetry: { persist: { log: true, metric: true, trace: true } },
    });
    host.register(
      action("silentAction", async (ctx) => {
        ctx.log.info("should not persist");
        ctx.metric("should.not.persist", 1);
        await ctx.trace("silent", async () => {});
      }, { telemetry: false }),
    );

    await host.executeAction("silentAction");

    const logs = await readStream(host, "_chimpbase.logs");
    const metrics = await readStream(host, "_chimpbase.metrics");
    const traces = await readStream(host, "_chimpbase.traces");

    expect(logs).toHaveLength(0);
    expect(metrics).toHaveLength(0);
    expect(traces).toHaveLength(0);

    // In-memory telemetry still captured
    const telemetry = host.drainTelemetryRecords();
    expect(telemetry.length).toBeGreaterThan(0);
  });

  test("per-handler override opts in when global is disabled", async () => {
    const host = await createMemoryHost({
      telemetry: { persist: { log: false, metric: false, trace: false } },
    });
    host.register(
      action("loudAction", async (ctx) => {
        ctx.log.info("should persist");
        ctx.metric("should.persist", 1);
      }, { telemetry: { log: true, metric: true } }),
    );

    await host.executeAction("loudAction");

    const logs = await readStream(host, "_chimpbase.logs");
    const metrics = await readStream(host, "_chimpbase.metrics");
    expect(logs).toHaveLength(1);
    expect(metrics).toHaveLength(1);
  });

  test("per-handler granular override merges with global", async () => {
    const host = await createMemoryHost({
      telemetry: { persist: { log: true, metric: true, trace: true } },
    });
    host.register(
      action("partialOverride", async (ctx) => {
        ctx.log.info("log");
        ctx.metric("m", 1);
        await ctx.trace("t", async () => {});
      }, { telemetry: { trace: false } }),
    );

    await host.executeAction("partialOverride");

    const logs = await readStream(host, "_chimpbase.logs");
    const metrics = await readStream(host, "_chimpbase.metrics");
    const traces = await readStream(host, "_chimpbase.traces");

    expect(logs).toHaveLength(1);
    expect(metrics).toHaveLength(1);
    expect(traces).toHaveLength(0); // overridden off
  });

  test("persists telemetry from queue workers", async () => {
    const host = await createMemoryHost({
      telemetry: { persist: { log: true, metric: true } },
    });
    host.register(
      action("enqueue", async (ctx) => {
        await ctx.enqueue("work", { x: 1 });
      }),
      worker("work", async (ctx, payload: { x: number }) => {
        ctx.log.info("processing", { x: payload.x });
        ctx.metric("work.done", 1);
      }),
    );

    await host.executeAction("enqueue");

    // Before processing, only action telemetry (no logs/metrics from worker yet)
    const logsBefore = await readStream(host, "_chimpbase.logs");
    expect(logsBefore).toHaveLength(0); // action didn't log

    await host.processNextQueueJob();

    const logs = await readStream(host, "_chimpbase.logs");
    const metrics = await readStream(host, "_chimpbase.metrics");
    expect(logs).toHaveLength(1);
    expect(logs[0].payload).toMatchObject({
      message: "processing",
      scope: { kind: "queue", name: "work" },
    });
    expect(metrics).toHaveLength(1);
  });

  test("drainTelemetryRecords still works alongside persistence", async () => {
    const host = await createMemoryHost({
      telemetry: { persist: { log: true } },
    });
    host.register(
      action("drainTest", async (ctx) => {
        ctx.log.info("persisted and drained");
      }),
    );

    await host.executeAction("drainTest");

    // Both drain and stream should have the data
    const telemetry = host.drainTelemetryRecords();
    expect(telemetry.some((r) => r.kind === "log" && r.message === "persisted and drained")).toBe(true);

    const events = await readStream(host, "_chimpbase.logs");
    expect(events).toHaveLength(1);
  });

  test("trace error status is persisted", async () => {
    const host = await createMemoryHost({
      telemetry: { persist: { trace: true } },
    });
    host.register(
      action("traceError", async (ctx) => {
        try {
          await ctx.trace("failing", async () => {
            throw new Error("boom");
          });
        } catch {
          // swallow
        }
      }),
    );

    await host.executeAction("traceError");

    const events = await readStream(host, "_chimpbase.traces");
    expect(events).toHaveLength(2);
    expect(events[1].event).toBe("trace.end");
    expect(events[1].payload).toMatchObject({
      status: "error",
      attributes: expect.objectContaining({ error: "boom" }) as unknown,
    });
  });
});

async function readStream(host: ChimpbaseBunHost, streamName: string) {
  // Use a simple action to read the stream
  const readActionName = `__test_read_${streamName.replace(/\./g, "_")}`;
  if (!host.registry.actions.has(readActionName)) {
    host.register(
      action(readActionName, async (ctx) => {
        return await ctx.stream.read(streamName);
      }),
    );
  }
  const result = await host.executeAction(readActionName);
  return v.object({
    createdAt: v.string(),
    event: v.string(),
    id: v.number(),
    payload: v.unknown(),
    stream: v.string(),
  }).array().parse(result.result, `telemetry stream ${streamName}`);
}
