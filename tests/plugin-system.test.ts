import { afterEach, describe, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";

import { createChimpbase } from "../packages/bun/src/library.ts";
import {
  action,
  middleware,
  onStart,
  onStop,
  parseJson,
  plugin,
  route,
  v,
  worker,
  type ChimpbaseValidator,
} from "../packages/runtime/index.ts";
import { readJsonResponse } from "./support/http.ts";

const cleanupDirs: string[] = [];

afterEach(async () => {
  while (cleanupDirs.length > 0) {
    const dir = cleanupDirs.pop();
    if ((dir !== undefined && dir.length > 0)) {
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
          const userId = env.get("user.id", v.number());
          const userName = env.get("user.name", v.string());
          return Response.json({ userId, userName });
        }),
      });

      const outcome = await host.executeRoute(new Request("http://test.local/any"));
      expect(outcome.response?.status).toBe(200);
      const body = await readJsonResponse<{ userId: number; userName: string }>(outcome.response);
      expect(body).toEqual({ userId: 42, userName: "Alice" });
    } finally {
      await host.close();
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
      await host.close();
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
      await host.close();
    }
  });

  // ── Lifecycle hooks ───────────────────────────────────────────────────

  test("HTTP and workers wait for asynchronous initialization", async () => {
    const host = await createTestHost();
    host.config.server.port = 0;
    let initialized = false;
    const readiness: boolean[] = [];
    const serve = host.serve.bind(host);
    const startWorker = host.startWorker.bind(host);
    host.serve = () => {
      readiness.push(initialized);
      return serve();
    };
    host.startWorker = () => {
      readiness.push(initialized);
      return startWorker();
    };
    host.register(
      worker("test.work", async () => { expect(initialized).toBe(true); }),
      onStart("test.init", async (ctx) => {
        await ctx.enqueue("test.work", {});
        await Bun.sleep(25);
        initialized = true;
      }),
    );
    const started = await host.start({ serve: true, runWorker: true });
    try {
      expect(readiness).toEqual([true, true]);
    } finally {
      await started.stop();
      await host.close();
    }
  });

  test("onStart can await host actions and routes", async () => {
    const host = await createTestHost();
    const completed: string[] = [];
    let timer: ReturnType<typeof setTimeout> | undefined;
    host.register(
      action("test.initialize", async (ctx) => {
        await ctx.kv.set("initialization", "ready");
        completed.push("action");
      }),
      route("test.initialization.route", async () => {
        completed.push("route");
        return new Response("ready");
      }),
      onStart("test.init", async () => {
        await host.executeAction("test.initialize");
        const outcome = await host.executeRoute(new Request("http://test.local/init"));
        expect(await outcome.response?.text()).toBe("ready");
      }),
    );
    try {
      const started = await Promise.race([
        host.start({ serve: false, runWorker: false }),
        new Promise<never>((_resolve, reject) => {
          timer = setTimeout(() => reject(new Error("initialization deadlocked")), 1000);
        }),
      ]);
      try {
        expect(completed).toEqual(["action", "route"]);
      } finally {
        await started.stop();
      }
    } finally {
      clearTimeout(timer);
      await host.close();
    }
  });

  test("failed initialization starts no HTTP listener or worker", async () => {
    const host = await createTestHost();
    host.config.server.port = 0;
    const servers: ReturnType<typeof host.serve>[] = [];
    const workers: ReturnType<typeof host.startWorker>[] = [];
    const serve = host.serve.bind(host);
    const startWorker = host.startWorker.bind(host);
    host.serve = () => {
      const server = serve();
      servers.push(server);
      return server;
    };
    host.startWorker = () => {
      const handle = startWorker();
      workers.push(handle);
      return handle;
    };
    host.register(onStart("test.init", () => { throw new Error("initialization failed"); }));
    try {
      await expect(host.start({ serve: true, runWorker: true })).rejects.toThrow("initialization failed");
      expect(servers).toHaveLength(0);
      expect(workers).toHaveLength(0);
    } finally {
      for (const server of servers) await server.stop(true);
      for (const handle of workers) await handle.stop();
      await host.close();
    }
  });

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

  test("method/path routes match exactly and pass unmatched requests to the next handler", async () => {
    const host = await createTestHost();
    let getCalls = 0;
    const getNotes = route("get", "/notes", (_request, env) => {
      getCalls++;
      expect(env.params).toEqual({});
      return Response.json({ requestId: env.get("requestId") });
    });
    expect(getNotes.name).toBe("GET /notes");
    try {
      host.register(
        middleware("requestId", (_request, env) => {
          env.set("requestId", "request-123");
          return null;
        }),
        getNotes,
        route("POST", "/notes", () => new Response("created", { status: 201 })),
        route("GET", "/café", () => new Response("unicode")),
        route("fallback", () => new Response("not found", { status: 404 })),
      );

      const get = await host.executeRoute(new Request("http://test.local/notes?limit=10"));
      expect(await readJsonResponse<{ requestId: string }>(get.response)).toEqual({ requestId: "request-123" });
      const post = await host.executeRoute(new Request("http://test.local/notes", { method: "POST" }));
      expect(post.response?.status).toBe(201);
      const unicode = await host.executeRoute(new Request("http://test.local/caf%C3%A9"));
      expect(await unicode.response?.text()).toBe("unicode");

      for (const [method, path] of [["GET", "/other"], ["GET", "/notes/"], ["HEAD", "/notes"]]) {
        const result = await host.executeRoute(new Request(`http://test.local${path}`, { method }));
        expect(result.response?.status).toBe(404);
      }
      expect(getCalls).toBe(1);
    } finally {
      await host.close();
    }
  });

  test("path parameters are decoded, isolated between requests, and preserve action access", async () => {
    const host = await createTestHost();
    const describeNote = action("describeNote", (_ctx, team: string, id: string) => ({ team, id }));
    try {
      host.register(
        describeNote,
        route("GET", "/teams/:team/notes/:id", async (_request, env) => {
          return Response.json(await env.action(describeNote, env.params.team, env.params.id));
        }),
      );
      const requests = await Promise.all([
        host.executeRoute(new Request("http://test.local/teams/acme/notes/caf%C3%A9?x=1")),
        host.executeRoute(new Request("http://test.local/teams/other/notes/a%2Fb")),
      ]);
      expect(await readJsonResponse<{ team: string; id: string }>(requests[0].response)).toEqual({ team: "acme", id: "café" });
      expect(await readJsonResponse<{ team: string; id: string }>(requests[1].response)).toEqual({ team: "other", id: "a/b" });

      for (const path of ["/teams/acme/notes", "/teams//notes/id", "/teams/acme/notes/", "/teams/acme/notes/%ZZ", "/teams/acme/other/id"]) {
        const result = await host.executeRoute(new Request(`http://test.local${path}`));
        expect(result.response).toBeNull();
      }
    } finally {
      await host.close();
    }
  });

  test("path parameters support object property names safely", async () => {
    const host = await createTestHost();
    try {
      host.register(route("GET", "/notes/:__proto__/:constructor", (_request, env) => Response.json(env.params)));
      const result = await host.executeRoute(new Request("http://test.local/notes/alice/bob"));
      expect(await readJsonResponse<Record<string, string>>(result.response)).toEqual({ ["__proto__"]: "alice", constructor: "bob" });
    } finally {
      await host.close();
    }
  });

  test("method/path routes reject invalid paths and parameter names", () => {
    for (const path of ["notes", "/notes?limit=1", "/notes#id", "/notes/:", "/notes/:id/:id", "/notes/:id.json"]) {
      expect(() => route("GET", path, () => new Response())).toThrow(TypeError);
    }
  });

  test("method/path routes propagate matched handler failures", async () => {
    const host = await createTestHost();
    try {
      host.register(route("GET", "/notes", () => { throw new Error("handler failed"); }));
      await expect(host.executeRoute(new Request("http://test.local/notes"))).rejects.toThrow("handler failed");
    } finally {
      await host.close();
    }
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
      await host.close();
    }
  });
  test("composed validators parse transforming members once", () => {
    const base = v.number();
    let parseCalls = 0;
    const normalizingNumber: ChimpbaseValidator<number> = {
      ...base,
      parse(value, path = "value") {
        parseCalls += 1;
        if (typeof value !== "string") {
          throw new Error(`${path} must be a numeric string`);
        }
        return base.parse(Number(value), path);
      },
    };

    expect(v.object({ value: normalizingNumber }).parse({ value: "42" })).toEqual({ value: 42 });
    expect(parseCalls).toBe(1);

    parseCalls = 0;
    expect(v.union(normalizingNumber, v.boolean()).parse("7")).toBe(7);
    expect(parseCalls).toBe(1);
  });

  test("record validators preserve special keys as data", () => {
    const source = parseJson('{"__proto__":{"polluted":true}}');
    const record = v.record(v.unknown()).parse(source);

    expect(Object.hasOwn(record, "__proto__")).toBe(true);
    expect(Reflect.getPrototypeOf(record)).toBe(Object.prototype);
    expect(() => v.integer().parse(Number.MAX_SAFE_INTEGER + 1)).toThrow("must be a safe integer");
  });

});
