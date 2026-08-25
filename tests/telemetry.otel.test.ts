import { afterEach, describe, expect, test } from "bun:test";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";

import { context, SpanStatusCode, trace } from "@opentelemetry/api";
import { AsyncLocalStorageContextManager } from "@opentelemetry/context-async-hooks";
import { resourceFromAttributes } from "@opentelemetry/resources";
import {
  BasicTracerProvider,
  InMemorySpanExporter,
  SimpleSpanProcessor,
} from "@opentelemetry/sdk-trace-base";

import { createChimpbase } from "../packages/bun/src/library.ts";
import { defineChimpbaseApp } from "../packages/core/index.ts";
import { action } from "../packages/runtime/index.ts";
import type { ChimpbaseBunHost } from "../packages/bun/src/runtime.ts";
import type {
  ChimpbaseSinkSpan,
  ChimpbaseTelemetryAttributes,
  ChimpbaseTelemetrySink,
} from "../packages/runtime/index.ts";

const cleanupHosts: ChimpbaseBunHost[] = [];
const cleanupDirs: string[] = [];
let tracerProvider: BasicTracerProvider | null = null;

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
  if ((tracerProvider !== null)) {
    await tracerProvider.shutdown();
    tracerProvider = null;
  }
  trace.disable();
  context.disable();
});

function toOtelAttributes(
  scope: { kind: string; name: string },
  attrs: ChimpbaseTelemetryAttributes = {},
): Record<string, string | number | boolean> {
  const result: Record<string, string | number | boolean> = {
    "chimpbase.scope.kind": scope.kind,
    "chimpbase.scope.name": scope.name,
  };
  for (const [key, value] of Object.entries(attrs)) {
    if (value !== null && value !== undefined) {
      result[key] = value;
    }
  }
  return result;
}

function createTestOtelSink(): {
  exporter: InMemorySpanExporter;
  sink: ChimpbaseTelemetrySink;
} {
  const exporter = new InMemorySpanExporter();
  const resource = resourceFromAttributes({ "service.name": "test-service" });

  const ctxManager = new AsyncLocalStorageContextManager();
  ctxManager.enable();
  context.setGlobalContextManager(ctxManager);

  const provider = new BasicTracerProvider({
    resource,
    spanProcessors: [new SimpleSpanProcessor(exporter)],
  });
  trace.setGlobalTracerProvider(provider);
  tracerProvider = provider;

  const tracer = provider.getTracer("chimpbase-test", "0.0.1");

  const sink: ChimpbaseTelemetrySink = {
    onLog() {},
    onMetric() {},

    startSpan(scope, name, attributes): ChimpbaseSinkSpan {
      const span = tracer.startSpan(
        name,
        { attributes: toOtelAttributes(scope, attributes) },
        context.active(),
      );
      const otelCtx = trace.setSpan(context.active(), span);
      return {
        setAttribute(key, value) {
          span.setAttribute(key, value);
        },
        end(status, errorMessage) {
          span.setStatus(
            status === "ok"
              ? { code: SpanStatusCode.OK }
              : { code: SpanStatusCode.ERROR, message: errorMessage },
          );
          span.end();
        },
        runInContext<T>(fn: () => T | Promise<T>): T | Promise<T> {
          return context.with(otelCtx, fn);
        },
      };
    },

    startHandlerSpan(scope): ChimpbaseSinkSpan {
      const spanName = `${scope.kind}:${scope.name}`;
      const span = tracer.startSpan(
        spanName,
        { attributes: toOtelAttributes(scope) },
        context.active(),
      );
      const otelCtx = trace.setSpan(context.active(), span);
      return {
        setAttribute(key, value) {
          span.setAttribute(key, value);
        },
        end(status, errorMessage) {
          span.setStatus(
            status === "ok"
              ? { code: SpanStatusCode.OK }
              : { code: SpanStatusCode.ERROR, message: errorMessage },
          );
          span.end();
        },
        runInContext<T>(fn: () => T | Promise<T>): T | Promise<T> {
          return context.with(otelCtx, fn);
        },
      };
    },

    async shutdown() {
      await provider.shutdown();
    },
  };

  return { exporter, sink };
}

async function createHostWithOtel(sink: ChimpbaseTelemetrySink) {
  const dir = await mkdtemp(join(tmpdir(), "chimpbase-otel-test-"));
  cleanupDirs.push(dir);
  await writeFile(join(dir, "package.json"), "{}");

  const host = await createChimpbase({
    app: defineChimpbaseApp({
      project: { name: "otel-test" },
      registrations: [],
    }),
    projectDir: dir,
    sinks: [sink],
    storage: { engine: "memory" },
  });
  cleanupHosts.push(host);
  return host;
}

describe("opentelemetry integration", () => {
  test("creates handler span for action execution", async () => {
    const { exporter, sink } = createTestOtelSink();
    const host = await createHostWithOtel(sink);

    host.register(action("myAction", async () => "done"));

    await host.executeAction("myAction");

    const spans = exporter.getFinishedSpans();
    const handlerSpan = spans.find((s) => s.name === "action:myAction");
    expect(handlerSpan).toBeDefined();
    expect(handlerSpan!.attributes["chimpbase.scope.kind"]).toBe("action");
    expect(handlerSpan!.attributes["chimpbase.scope.name"]).toBe("myAction");
    expect(handlerSpan!.status.code).toBe(SpanStatusCode.OK);
  });

  test("ctx.trace spans are children of handler span", async () => {
    const { exporter, sink } = createTestOtelSink();
    const host = await createHostWithOtel(sink);

    host.register(
      action("parentAction", async (ctx) => {
        return await ctx.trace("childWork", async () => "result");
      }),
    );

    await host.executeAction("parentAction");

    const spans = exporter.getFinishedSpans();
    const handlerSpan = spans.find((s) => s.name === "action:parentAction");
    const childSpan = spans.find((s) => s.name === "childWork");

    expect(handlerSpan).toBeDefined();
    expect(childSpan).toBeDefined();

    // In OTel SDK v2, parentSpanContext carries the parent relationship
    expect(childSpan!.parentSpanContext?.spanId).toBe(
      handlerSpan!.spanContext().spanId,
    );
  });

  test("handler span captures error status", async () => {
    const { exporter, sink } = createTestOtelSink();
    const host = await createHostWithOtel(sink);

    host.register(
      action("failingAction", async () => {
        throw new Error("otel error");
      }),
    );

    try {
      await host.executeAction("failingAction");
    } catch {
      // expected
    }

    const spans = exporter.getFinishedSpans();
    const handlerSpan = spans.find((s) => s.name === "action:failingAction");
    expect(handlerSpan).toBeDefined();
    expect(handlerSpan!.status.code).toBe(SpanStatusCode.ERROR);
    expect(handlerSpan!.status.message).toBe("otel error");
  });

  test("nested ctx.trace spans form proper hierarchy", async () => {
    const { exporter, sink } = createTestOtelSink();
    const host = await createHostWithOtel(sink);

    host.register(
      action("nestedAction", async (ctx) => {
        return await ctx.trace("outer", async () => {
          return await ctx.trace("inner", async () => "deep");
        });
      }),
    );

    await host.executeAction("nestedAction");

    const spans = exporter.getFinishedSpans();
    const handlerSpan = spans.find((s) => s.name === "action:nestedAction");
    const outerSpan = spans.find((s) => s.name === "outer");
    const innerSpan = spans.find((s) => s.name === "inner");

    expect(handlerSpan).toBeDefined();
    expect(outerSpan).toBeDefined();
    expect(innerSpan).toBeDefined();

    // outer is child of handler
    expect(outerSpan!.parentSpanContext?.spanId).toBe(
      handlerSpan!.spanContext().spanId,
    );
    // inner is child of outer
    expect(innerSpan!.parentSpanContext?.spanId).toBe(
      outerSpan!.spanContext().spanId,
    );
  });

  test("createOtelSink from @chimpbase/otel works end-to-end", async () => {
    // Clean up any prior global state
    trace.disable();
    context.disable();

    const { createOtelSink } = await import("../packages/otel/src/index.ts");
    const {
      InMemorySpanExporter: Exporter,
      SimpleSpanProcessor: SimpleSP,
    }: typeof import("@opentelemetry/sdk-trace-base") = await import("@opentelemetry/sdk-trace-base");

    const exporter = new Exporter();

    // Use SimpleSpanProcessor so spans export synchronously
    const sink = createOtelSink({
      serviceName: "otel-package-test",
      spanProcessor: new SimpleSP(exporter),
    });

    const dir = await mkdtemp(join(tmpdir(), "chimpbase-otel-pkg-test-"));
    cleanupDirs.push(dir);
    await writeFile(join(dir, "package.json"), "{}");

    const host = await createChimpbase({
      app: defineChimpbaseApp({
        project: { name: "otel-pkg-test" },
        registrations: [],
      }),
      projectDir: dir,
      sinks: [sink],
      storage: { engine: "memory" },
    });
    cleanupHosts.push(host);

    host.register(
      action("pkgTestAction", async (ctx) => {
        ctx.log.info("testing otel package");
        ctx.log.warn("a warning", { code: 42 });
        ctx.metric("test.counter", 1, { region: "us-east" });
        ctx.metric("test.gauge", 99.5);
        return await ctx.trace("pkgWork", async () => "done");
      }),
    );

    // All OTel sink methods (onLog, onMetric, startSpan, startHandlerSpan) must run without error
    await host.executeAction("pkgTestAction");

    const spans = exporter.getFinishedSpans();
    const handlerSpan = spans.find((s) => s.name === "action:pkgTestAction");
    const childSpan = spans.find((s) => s.name === "pkgWork");

    expect(handlerSpan).toBeDefined();
    expect(childSpan).toBeDefined();
    expect(childSpan!.parentSpanContext?.spanId).toBe(
      handlerSpan!.spanContext().spanId,
    );

    await sink.shutdown?.();
  });
});
