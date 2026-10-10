import { describe, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { createChimpbase } from "../packages/bun/src/library.ts";
import { createCallDispatcher, type RemoteDispatcher } from "../packages/mesh/src/call.ts";
import { MeshPeerCache } from "../packages/mesh/src/discovery.ts";
import {
  chimpbaseMesh,
  DEFAULT_RPC_PATH,
  MESH_TOKEN_HEADER,
  MeshCallError,
  MeshTimeoutError,
  service,
  type ServiceDefinition,
} from "../packages/mesh/src/index.ts";
import { createHttpDispatcher } from "../packages/mesh/src/transport-http.ts";
import { action, onStart, v, type ChimpbaseContext } from "../packages/runtime/index.ts";
import { readJsonResponse } from "./support/http.ts";

type MeshHost = Awaited<ReturnType<typeof createChimpbase>>;
const pgUrl = process.env.CHIMPBASE_TEST_PG_URL;
const token = "mesh-transaction-token";

async function withHost(
  engine: "memory" | "postgres",
  actions: NonNullable<ServiceDefinition["actions"]>,
  run: (host: MeshHost, ctx: ChimpbaseContext, key: string) => Promise<void>,
): Promise<void> {
  const projectDir = await mkdtemp(join(tmpdir(), "chimpbase-mesh-transactions-"));
  const key = crypto.randomUUID();
  const host = await createChimpbase({
    projectDir,
    secrets: { get: () => token },
    storage: engine === "postgres" ? { engine, url: pgUrl } : { engine },
  });
  let context: ChimpbaseContext | undefined;
  host.register(
    chimpbaseMesh({
      advertisedUrl: "http://mesh-transaction.test",
      heartbeatMs: 0,
      meshToken: "MESH_TOKEN",
      services: [service({ name: "transaction", actions })],
      transport: "http",
    }),
    onStart("transaction-fixture", async (ctx) => {
      context = ctx;
      await ctx.db.query("CREATE TABLE IF NOT EXISTS mesh_transaction_regression (id TEXT PRIMARY KEY, value INTEGER NOT NULL)");
      await ctx.db.query("INSERT INTO mesh_transaction_regression (id, value) VALUES (?1, 0)", [key]);
    }),
    action("inspect", async (ctx) => ({
      kv: await ctx.kv.get(key),
      rows: await ctx.db.query("SELECT value FROM mesh_transaction_regression WHERE id = ?1", [key], v.object({ value: v.number() })),
    })),
    action("cleanup", async (ctx) => {
      await ctx.kv.delete(key);
      await ctx.db.query("DELETE FROM mesh_transaction_regression WHERE id = ?1", [key]);
    }),
  );
  let started: Awaited<ReturnType<MeshHost["start"]>> | undefined;
  try {
    started = await host.start({ serve: false, runWorker: false });
    if (context === undefined) throw new Error("mesh context missing");
    await host.executeAction("inspect");
    await run(host, context, key);
  } finally {
    if (started !== undefined) {
      await host.executeAction("cleanup");
      await started.stop();
    }
    await host.close();
    await rm(projectDir, { recursive: true, force: true });
  }
}

async function write(ctx: ChimpbaseContext, key: string): Promise<void> {
  await ctx.kv.set(key, true);
  await ctx.db.query("UPDATE mesh_transaction_regression SET value = value + 1 WHERE id = ?1", [key]);
  ctx.pubsub.publish("mesh.transaction.write", { key });
}

function remoteCall(remoteDispatcher: RemoteDispatcher) {
  const cache = new MeshPeerCache(30_000);
  cache.upsert({
    advertisedUrl: "http://127.0.0.1:9999",
    lastHeartbeatMs: Date.now(),
    metadata: {},
    nodeId: "remote",
    services: [{ name: "remote", version: 1, actions: ["v1.remote.run"], events: [] }],
    startedAtMs: Date.now(),
  });
  return createCallDispatcher({
    cache,
    defaultRetries: 2,
    defaultStrategy: "local-first",
    defaultTimeoutMs: 1_000,
    localActionNames: new Set(),
    localNodeId: "local",
    middleware: [],
    remoteDispatcher,
  });
}

for (const engine of ["memory", "postgres"] as const) {
  const describeEngine = engine === "postgres" && !pgUrl ? describe.skip : describe;
  describeEngine(`mesh transaction failures (${engine})`, () => {
    test("RPC errors roll back writes and events while preserving HTTP error responses", async () => {
      await withHost(engine, {
        fail: async (ctx, key: string) => {
          await write(ctx, key);
          throw new Error("target failed");
        },
        success: () => "ok",
      }, async (host, _ctx, key) => {
        await expect(host.executeAction("v1.transaction.fail", [key])).rejects.toThrow("target failed");
        const request = (actionName: string, providedToken = token) => new Request(`http://test.local${DEFAULT_RPC_PATH}`, {
          body: JSON.stringify({ actionName, args: key, callerNodeId: "caller", deadlineMs: Date.now() + 1_000 }),
          headers: { [MESH_TOKEN_HEADER]: providedToken },
          method: "POST",
        });
        const failed = await host.executeRoute(request("v1.transaction.fail"));
        expect(failed.response?.status).toBe(500);
        expect(await readJsonResponse<{ ok: boolean; error: string }>(failed.response)).toEqual({ ok: false, error: "target failed" });
        expect(failed.emittedEvents).toEqual([]);
        expect((await host.executeAction("inspect")).result).toEqual({ kv: null, rows: [{ value: 0 }] });
        const unauthorized = await host.executeRoute(request("v1.transaction.success", "incorrect"));
        expect(unauthorized.response?.status).toBe(401);
        const successful = await host.executeRoute(request("v1.transaction.success"));
        expect(successful.response?.status).toBe(200);
        expect(await readJsonResponse<{ ok: boolean; result: string }>(successful.response)).toEqual({ ok: true, result: "ok" });
      });
    });

    for (const lateFailure of [false, true]) {
      test(`local timeout waits for late ${lateFailure ? "failure" : "success"} before rolling back`, async () => {
        let release = () => {};
        let entered = () => {};
        const gate = new Promise<void>((resolve) => { release = resolve; });
        const active = new Promise<void>((resolve) => { entered = resolve; });
        let attempts = 0;
        let usedFallback = false;
        await withHost(engine, {
          slow: async (ctx, key: string) => {
            attempts += 1;
            entered();
            await gate;
            await write(ctx, key);
            if (lateFailure) throw new Error("late failure");
            return "ok";
          },
          run: async (ctx, key: string) => await ctx.mesh?.call("v1.transaction.slow", key, v.string(), {
            timeoutMs: 10,
            retry: { attempts: 2, delayMs: 0 },
            fallback: () => { usedFallback = true; return "fallback"; },
          }),
        }, async (host, _ctx, key) => {
          let settled = false;
          const running = host.executeAction("v1.transaction.run", [key]).then(
            (outcome) => { settled = true; return outcome; },
            (error: unknown) => { settled = true; return error; },
          );
          let outcome: unknown;
          try {
            await active;
            await Bun.sleep(30);
            expect(settled).toBe(false);
          } finally {
            release();
            outcome = await running;
          }
          expect(outcome).toBeInstanceOf(MeshTimeoutError);
          expect(attempts).toBe(1);
          expect(usedFallback).toBe(false);
          expect((await host.executeAction("inspect")).result).toEqual({ kv: null, rows: [{ value: 0 }] });
        });
      });
    }

    test("invalid local results are neither retried nor replaced by fallback", async () => {
      let attempts = 0;
      let usedFallback = false;
      await withHost(engine, {
        invalid: async (ctx, key: string) => {
          attempts += 1;
          await write(ctx, key);
          return 42;
        },
        run: async (ctx, key: string) => await ctx.mesh?.call("v1.transaction.invalid", key, v.string(), {
          retry: { attempts: 2, delayMs: 0 },
          fallback: () => { usedFallback = true; return "fallback"; },
        }),
      }, async (host, _ctx, key) => {
        await expect(host.executeAction("v1.transaction.run", [key])).rejects.toThrow("must be a string");
        expect(attempts).toBe(1);
        expect(usedFallback).toBe(false);
        expect((await host.executeAction("inspect")).result).toEqual({ kv: null, rows: [{ value: 0 }] });
      });
    });

    test("local application errors cannot retry or commit partial writes through fallback", async () => {
      let attempts = 0;
      let usedFallback = false;
      await withHost(engine, {
        fail: async (ctx, key: string) => {
          attempts += 1;
          await write(ctx, key);
          throw new Error("application failed");
        },
        run: async (ctx, key: string) => await ctx.mesh?.call("v1.transaction.fail", key, v.string(), {
          retry: { attempts: 2, delayMs: 0 },
          fallback: () => { usedFallback = true; return "fallback"; },
        }),
      }, async (host, _ctx, key) => {
        await expect(host.executeAction("v1.transaction.run", [key])).rejects.toThrow("application failed");
        expect(attempts).toBe(1);
        expect(usedFallback).toBe(false);
        expect((await host.executeAction("inspect")).result).toEqual({ kv: null, rows: [{ value: 0 }] });
      });
    });
  });
}

describe("mesh remote retries", () => {
  test("HTTP connection failures are explicitly retryable", async () => {
    const server = Bun.serve({ port: 0, fetch: () => new Response("ok") });
    const advertisedUrl = `http://127.0.0.1:${server.port}`;
    await server.stop(true);
    const dispatch = createHttpDispatcher({ callerNodeId: "caller", rpcPath: DEFAULT_RPC_PATH, tokenProvider: () => token });
    await expect(dispatch({
      actionName: "v1.remote.run",
      args: {},
      deadlineMs: Date.now() + 1_000,
      peer: { advertisedUrl, lastHeartbeatMs: Date.now(), metadata: {}, nodeId: "remote", services: [], startedAtMs: Date.now() },
    })).rejects.toMatchObject({ retryable: true });
  });

  test("retries transport failures until success", async () => {
    let attempts = 0;
    const call = remoteCall(async () => {
      attempts += 1;
      if (attempts < 3) throw new MeshCallError("v1.remote.run", "remote", "connection failed", undefined, true);
      return "ok";
    });
    await withHost("memory", { noop: () => "ok" }, async (_host, ctx) => {
      expect(await call(ctx, "v1.remote.run", {}, v.string(), { retry: { attempts: 3, delayMs: 0 }, nodeId: "remote" })).toBe("ok");
      expect(attempts).toBe(3);
    });
  });

  test("successful remote results are validated once outside retry and fallback", async () => {
    let attempts = 0;
    let usedFallback = false;
    const call = remoteCall(async () => { attempts += 1; return 42; });
    await withHost("memory", { noop: () => "ok" }, async (_host, ctx) => {
      await expect(call(ctx, "v1.remote.run", {}, v.string(), {
        retry: { attempts: 2, delayMs: 0 },
        fallback: () => { usedFallback = true; return "fallback"; },
      })).rejects.toThrow("must be a string");
      expect(attempts).toBe(1);
      expect(usedFallback).toBe(false);
    });
  });

  test("argument serialization errors are not retried as transport failures", async () => {
    let attempts = 0;
    const args: { self?: unknown } = {};
    args.self = args;
    const dispatch = createHttpDispatcher({ callerNodeId: "caller", rpcPath: DEFAULT_RPC_PATH, tokenProvider: () => token });
    const call = remoteCall(async (params) => { attempts += 1; return await dispatch(params); });
    await withHost("memory", { noop: () => "ok" }, async (_host, ctx) => {
      await expect(call(ctx, "v1.remote.run", args, v.string(), { retry: { attempts: 2, delayMs: 0 }, nodeId: "remote" })).rejects.toBeInstanceOf(TypeError);
      expect(attempts).toBe(1);
    });
  });

  for (const response of ["application error", "invalid JSON"] as const) {
    test(`does not retry HTTP ${response}`, async () => {
      let attempts = 0;
      const server = Bun.serve({
        port: 0,
        fetch() {
          attempts += 1;
          return new Response(response === "application error" ? '{"ok":false,"error":"application failed"}' : "{", {
            status: response === "application error" ? 500 : 200,
          });
        },
      });
      const dispatch = createHttpDispatcher({ callerNodeId: "caller", rpcPath: DEFAULT_RPC_PATH, tokenProvider: () => token });
      const call = remoteCall((params) => dispatch({
        ...params,
        peer: { ...params.peer, advertisedUrl: `http://127.0.0.1:${server.port}` },
      }));
      try {
        await withHost("memory", { noop: () => "ok" }, async (_host, ctx) => {
          await expect(call(ctx, "v1.remote.run", {}, v.string(), { retry: { attempts: 2, delayMs: 0 }, nodeId: "remote" })).rejects.toBeInstanceOf(MeshCallError);
          expect(attempts).toBe(1);
        });
      } finally {
        server.stop(true);
      }
    });
  }
});
