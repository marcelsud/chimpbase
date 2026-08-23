import { afterEach, describe, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";

import { createChimpbase } from "../packages/bun/src/library.ts";
import { action, middleware, onStart, onStop, plugin, route } from "../packages/runtime/index.ts";
import { readJsonResponse } from "./support/http.ts";

const cleanupDirs: string[] = [];

afterEach(async () => {
  while (cleanupDirs.length > 0) {
    const dir = cleanupDirs.pop();
    if (dir) {
      await rm(dir, { recursive: true, force: true });
    }
  }
});

async function createTestHost() {
  const projectDir = await mkdtemp(join(tmpdir(), "chimpbase-plugin-system-"));
  cleanupDirs.push(projectDir);

  return await createChimpbase({
    project: { name: "plugin-system-test" },
    projectDir,
    storage: { engine: "memory" },
  });
}

describe("plugin system", () => {
  // ── Request context ───────────────────────────────────────────────────

  test("route env supports get/set for request-scoped context", async () => {
    const host = await createTestHost();
    try {
      host.register({
        setter: route("test.setter", async (_request, env) => {
          env.set("user.id", 42);
          env.set("user.name", "Alice");
          return null; // pass through
        }),
        reader: route("test.reader", async (_request, env) => {
          const userId = env.get<number>("user.id");
          const userName = env.get<string>("user.name");
          return Response.json({ userId, userName });
        }),
      });

      const outcome = await host.executeRoute(new Request("http://test.local/any"));
      expect(outcome.response?.status).toBe(200);
      const body = await readJsonResponse<{ userId: number; userName: string }>(outcome.response);
      expect(body).toEqual({ userId: 42, userName: "Alice" });
    } finally {
      host.close();
    }
  });

  test("request context is isolated between requests", async () => {
    const host = await createTestHost();
    let callCount = 0;
    try {
      host.register({
        setter: route("test.setter", async (_request, env) => {
          callCount++;
          env.set("call", callCount);
          return null;
        }),
        reader: route("test.reader", async (_request, env) => {
          return Response.json({ call: env.get("call") });
        }),
      });

      const r1 = await host.executeRoute(new Request("http://test.local/a"));
      const r2 = await host.executeRoute(new Request("http://test.local/b"));

      expect(await readJsonResponse<{ call: number }>(r1.response)).toEqual({ call: 1 });
      expect(await readJsonResponse<{ call: number }>(r2.response)).toEqual({ call: 2 });
    } finally {
      host.close();
    }
  });

  test("get returns undefined for unset keys", async () => {
    const host = await createTestHost();
    try {
      host.register({
        reader: route("test.reader", async (_request, env) => {
          return Response.json({ value: env.get("nonexistent") ?? null });
        }),
      });

      const outcome = await host.executeRoute(new Request("http://test.local/any"));
      expect(await readJsonResponse<{ value: unknown }>(outcome.response)).toEqual({ value: null });
    } finally {
      host.close();
    }
  });

  // ── Lifecycle hooks ───────────────────────────────────────────────────

  test("onStart hook runs on start and has ctx access", async () => {
    const host = await createTestHost();
    let startRan = false;
    let kvValue: unknown = null;

    host.register({
      init: onStart("test.init", async (ctx) => {
        startRan = true;
        await ctx.kv.set("init.flag", "started");
      }),
      checkInit: action("checkInit", async (ctx) => {
        return await ctx.kv.get("init.flag");
      }),
    });

    const started = await host.start({ serve: false, runWorker: false });
    try {
      expect(startRan).toBe(true);

      const result = await host.executeAction("checkInit");
      expect(result.result).toBe("started");
    } finally {
      await started.stop();
    }
  });

  test("onStop hook runs on shutdown", async () => {
    const host = await createTestHost();
    let stopRan = false;

    host.register({
      cleanup: onStop("test.cleanup", async () => {
        stopRan = true;
      }),
    });

    const started = await host.start({ serve: false, runWorker: false });
    expect(stopRan).toBe(false);
    await started.stop();
    expect(stopRan).toBe(true);
  });

  test("multiple onStart hooks run in registration order", async () => {
    const host = await createTestHost();
    const order: string[] = [];

    host.register({
      first: onStart("first", async () => { order.push("first"); }),
      second: onStart("second", async () => { order.push("second"); }),
      third: onStart("third", async () => { order.push("third"); }),
    });

    const started = await host.start({ serve: false, runWorker: false });
    try {
      expect(order).toEqual(["first", "second", "third"]);
    } finally {
      await started.stop();
    }
  });

  test("lifecycle hooks work inside plugins", async () => {
    const host = await createTestHost();
    let pluginStarted = false;
    let pluginStopped = false;

    const myPlugin = plugin(
      { name: "lifecycle-plugin" },
      onStart("lifecycle-plugin.start", async (ctx) => {
        pluginStarted = true;
        ctx.log.info("plugin started");
      }),
      onStop("lifecycle-plugin.stop", async () => {
        pluginStopped = true;
      }),
      action("pluginAction", async () => "hello"),
    );

    host.register({ myPlugin });

    const started = await host.start({ serve: false, runWorker: false });
    expect(pluginStarted).toBe(true);

    await started.stop();
    expect(pluginStopped).toBe(true);
  });

  // ── Middleware alias ──────────────────────────────────────────────────

  test("middleware() is an alias for route()", async () => {
    const host = await createTestHost();
    try {
      host.register({
        cors: middleware("cors", async (request) => {
          if (request.method === "OPTIONS") {
            return new Response(null, {
              headers: { "access-control-allow-origin": "*" },
            });
          }
          return null;
        }),
        handler: route("test.handler", async () => {
          return Response.json({ ok: true });
        }),
      });

      // OPTIONS → handled by middleware
      const r1 = await host.executeRoute(
        new Request("http://test.local/any", { method: "OPTIONS" }),
      );
      expect(r1.response?.status).toBe(200);
      expect(r1.response?.headers.get("access-control-allow-origin")).toBe("*");

      // GET → passes through middleware to handler
      const r2 = await host.executeRoute(new Request("http://test.local/any"));
      expect(await readJsonResponse<{ ok: boolean }>(r2.response)).toEqual({ ok: true });
    } finally {
      host.close();
    }
  });
});
