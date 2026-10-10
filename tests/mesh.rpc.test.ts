import { describe, expect, spyOn, test } from "bun:test";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { Pool } from "pg";

import { createChimpbase } from "../packages/bun/src/library.ts";
import {
  chimpbaseMesh,
  DEFAULT_RPC_PATH,
  MESH_TOKEN_HEADER,
  RPC_EXECUTE_ACTION,
  service,
  type ChimpbaseMeshOptions,
} from "../packages/mesh/src/index.ts";
import { createHttpDispatcher } from "../packages/mesh/src/transport-http.ts";
import { action, route } from "../packages/runtime/index.ts";
import { readJsonResponse } from "./support/http.ts";

const TOKEN = "mesh-rpc-test-token";
type MeshHost = Awaited<ReturnType<typeof createChimpbase>>;
type RpcFailure = { error: string; ok: false };

async function withRpcHost(
  run: (host: MeshHost) => Promise<void>,
  limits: Pick<ChimpbaseMeshOptions, "rpcBodyTimeoutMs" | "rpcMaxBodyBytes"> = {},
  engine: "memory" | "postgres" = "memory",
): Promise<void> {
  const projectDir = await mkdtemp(join(tmpdir(), "chimpbase-mesh-rpc-"));
  const host = await createChimpbase({
    projectDir,
    secrets: { get: (name) => name === "MESH_TOKEN" ? TOKEN : null },
    storage: engine === "postgres"
      ? { engine, url: process.env.CHIMPBASE_TEST_PG_URL! }
      : { engine },
  });
  try {
    host.register(
      chimpbaseMesh({
        advertisedUrl: "http://mesh.test",
        heartbeatMs: 0,
        meshToken: "MESH_TOKEN",
        ...limits,
        services: [service({ name: "payload", actions: {
          echo: (_ctx, payload: unknown) => ({ omitted: payload === undefined, payload }),
          write: async (ctx) => { await ctx.kv.set("rpc.write", true); return "written"; },
        } })],
      }),
      action("private.write", async (ctx) => { await ctx.kv.set("rpc.write", true); }),
      action("private.read", async (ctx) => await ctx.kv.get("rpc.write")),
      action("private.nodes", async (ctx) => await ctx.db.query("SELECT node_id FROM _chimpbase_mesh_nodes")),
    );
    const started = await host.start({ serve: false, runWorker: false });
    try {
      await run(host);
    } finally {
      await started.stop();
    }
  } finally {
    await host.close();
    await rm(projectDir, { recursive: true, force: true });
  }
}

function envelope(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    actionName: "v1.payload.write",
    args: {},
    callerNodeId: "caller",
    deadlineMs: Date.now() + 60_000,
    ...overrides,
  };
}

async function rpc(host: MeshHost, body: unknown, token: string | null = TOKEN): Promise<Response> {
  const outcome = await host.executeRoute(new Request(`http://mesh.test${DEFAULT_RPC_PATH}`, {
    body: JSON.stringify(body),
    headers: token === null ? {} : { [MESH_TOKEN_HEADER]: token },
    method: "POST",
  }));
  if (outcome.response === null) throw new Error("RPC response missing");
  return outcome.response;
}

describe("@chimpbase/mesh RPC boundary", () => {
  test("authenticates before inspecting the target or envelope", async () => {
    await withRpcHost(async (host) => {
      for (const token of [null, "wrong-token"]) {
        for (const body of [null, envelope(), envelope({ actionName: "private.write" })]) {
          const response = await rpc(host, body, token);
          expect(response.status).toBe(401);
          expect(await readJsonResponse<RpcFailure>(response)).toEqual({ error: "unauthorized mesh rpc", ok: false });
        }
      }
      expect((await host.executeAction("private.read")).result).toBeNull();
      expect((await host.executeAction("private.nodes")).result).toHaveLength(1);
    });
  });

  test("rejects private application and mesh administration actions", async () => {
    await withRpcHost(async (host) => {
      for (const actionName of ["private.write", "__chimpbase.mesh.deregister", RPC_EXECUTE_ACTION, "v1.missing.write"]) {
        const response = await rpc(host, envelope({ actionName }));
        expect(response.status).toBe(403);
        expect(await readJsonResponse<RpcFailure>(response)).toEqual({ error: "mesh rpc action is not registered", ok: false });
      }
      expect((await host.executeAction("private.read")).result).toBeNull();
      expect((await host.executeAction("private.nodes")).result).toHaveLength(1);
      expect((await rpc(host, envelope())).status).toBe(200);
      expect((await host.executeAction("private.read")).result).toBe(true);
    });
  });

  test("rejects malformed envelopes without invoking their actions", async () => {
    await withRpcHost(async (host) => {
      const malformed = [null, true, 1, "body", [], {},
        envelope({ actionName: undefined }), envelope({ actionName: "" }), envelope({ actionName: 1 }),
        envelope({ callerNodeId: undefined }), envelope({ callerNodeId: "" }), envelope({ callerNodeId: 1 }),
        envelope({ deadlineMs: undefined }), envelope({ deadlineMs: null }), envelope({ deadlineMs: "future" }),
        envelope({ deadlineMs: Infinity }), envelope({ deadlineMs: NaN }),
      ];
      for (const body of malformed) {
        const response = await rpc(host, body);
        expect(response.status).toBe(400);
        expect(await readJsonResponse<RpcFailure>(response)).toEqual({ error: "invalid rpc envelope", ok: false });
      }
      for (const deadlineMs of [NaN, Infinity, -Infinity]) {
        await expect(host.executeAction(RPC_EXECUTE_ACTION, [envelope({ deadlineMs }), TOKEN]))
          .rejects.toThrow("invalid rpc envelope");
      }
      expect((await host.executeAction("private.read")).result).toBeNull();
    });
  });

  test("rejects expired absolute deadlines before invocation", async () => {
    await withRpcHost(async (host) => {
      for (const deadlineMs of [0, Date.now() - 1_000, Date.now()]) {
        const response = await rpc(host, envelope({ deadlineMs }));
        expect(response.status).toBe(408);
        expect(await readJsonResponse<RpcFailure>(response)).toEqual({ error: "mesh rpc deadline exceeded", ok: false });
      }
      await expect(host.executeAction(RPC_EXECUTE_ACTION, [envelope({ deadlineMs: 0 }), TOKEN]))
        .rejects.toThrow("mesh rpc deadline exceeded");
      expect((await host.executeAction("private.read")).result).toBeNull();
      expect((await rpc(host, envelope())).status).toBe(200);
      expect((await host.executeAction("private.read")).result).toBe(true);
    });
  });

  test("keeps malformed JSON and wrong methods as HTTP client errors", async () => {
    await withRpcHost(async (host) => {
      const invalid = await host.executeRoute(new Request(`http://mesh.test${DEFAULT_RPC_PATH}`, {
        body: "{", headers: { [MESH_TOKEN_HEADER]: TOKEN }, method: "POST",
      }));
      expect(invalid.response?.status).toBe(400);
      expect(await readJsonResponse<RpcFailure>(invalid.response)).toEqual({ error: "invalid json body", ok: false });
      const wrongMethod = await host.executeRoute(new Request(`http://mesh.test${DEFAULT_RPC_PATH}`));
      expect(wrongMethod.response?.status).toBe(405);
      expect((await host.executeAction("private.read")).result).toBeNull();
    });
  });

  for (const engine of ["memory", "postgres"] as const) {
    const testEngine = engine === "postgres" && !process.env.CHIMPBASE_TEST_PG_URL ? test.skip : test;
    testEngine(`rejects ten incomplete unauthenticated bodies without a database checkout (${engine})`, async () => {
      await withRpcHost(async (host) => {
        let cancelled = 0;
        const checkout = spyOn(Pool.prototype, "connect");
        try {
          const outcomes = await Promise.all(Array.from({ length: 10 }, () => host.executeRoute(new Request(`http://mesh.test${DEFAULT_RPC_PATH}`, {
            body: new ReadableStream<Uint8Array>({ cancel() { cancelled += 1; } }),
            method: "POST",
          }))));
          expect(outcomes.map((outcome) => outcome.response?.status)).toEqual(Array.from({ length: 10 }, () => 401));
          expect(cancelled).toBe(10);
          expect(checkout.mock.calls).toHaveLength(0);
          expect((await rpc(host, envelope())).status).toBe(200);
        } finally {
          checkout.mockRestore();
        }
      }, {}, engine);
    });

    testEngine(`bounds streamed body size and read time before database checkout (${engine})`, async () => {
      await withRpcHost(async (host) => {
        const checkout = spyOn(Pool.prototype, "connect");
        try {
          for (const failure of ["timeout", "declared size", "streamed size"] as const) {
            let cancelled = false;
            const request = new Request(`http://mesh.test${DEFAULT_RPC_PATH}`, {
              body: new ReadableStream<Uint8Array>({
                start(controller) {
                  if (failure === "streamed size") controller.enqueue(new Uint8Array(65));
                },
                cancel() { cancelled = true; },
              }),
              headers: { [MESH_TOKEN_HEADER]: TOKEN, ...(failure === "declared size" ? { "content-length": "65" } : {}) },
              method: "POST",
            });
            const outcome = await host.executeRoute(request);
            expect(outcome.response?.status).toBe(failure === "timeout" ? 408 : 413);
            expect(cancelled).toBe(true);
            expect(request.body?.locked).toBe(false);
            expect(checkout.mock.calls).toHaveLength(0);
          }
        } finally {
          checkout.mockRestore();
        }
      }, { rpcBodyTimeoutMs: 25, rpcMaxBodyBytes: 64 }, engine);
    });
  }

  for (const rejected of [false, true]) {
    test(`prepared ${rejected ? "rejection" : "handler"} preserves earlier route guard ordering`, async () => {
      const host = await createChimpbase({ storage: { engine: "memory" } });
      let invoked = false;
      host.register(
        route("guard", () => new Response("denied by guard", { status: 403 })),
        {
          ...route("guard", () => { throw new Error("unprepared handler executed"); }),
          prepare: () => rejected ? new Response("preflight rejection", { status: 401 }) : async () => {
            invoked = true;
            return new Response("prepared target");
          },
          concurrencyGroup: "rpc",
        },
      );
      try {
        const outcome = await host.executeRoute(new Request("http://test/prepared"));
        expect(outcome.response?.status).toBe(403);
        expect(await outcome.response?.text()).toBe("denied by guard");
        expect(invoked).toBe(false);
      } finally {
        await host.close();
      }
    });
  }

  test("prepared fallthrough keeps route context and later handlers", async () => {
    const host = await createChimpbase({ storage: { engine: "memory" } });
    host.register(
      route("context", (_request, env) => { env.set("guard", "passed"); return null; }),
      { ...route("not-matched", () => { throw new Error("nonmatched handler executed"); }), prepare: () => null },
      { ...route("prepared", () => null), prepare: () => (_request, env) => {
        env.set("prepared", env.get("guard"));
        return null;
      } },
      route("fallback", (_request, env) => new Response(String(env.get("prepared")))),
    );
    try {
      expect(await (await host.executeRoute(new Request("http://test/fallthrough"))).response?.text()).toBe("passed");
    } finally {
      await host.close();
    }
  });

  test("preparers preserve bodies for later preparers and fallthrough in Node", async () => {
    const directory = await mkdtemp(join(tmpdir(), "chimpbase-rpc-node-"));
    try {
      const source = join(directory, "entry.ts");
      await writeFile(source, `
        import assert from "node:assert/strict";
        import { createChimpbase } from ${JSON.stringify(resolve(import.meta.dir, "../packages/node/src/library.ts"))};
        import { chimpbaseMesh, service, MESH_TOKEN_HEADER } from ${JSON.stringify(resolve(import.meta.dir, "../packages/mesh/src/index.ts"))};
        const host = await createChimpbase({ storage: { engine: "memory" }, secrets: { get: () => "token" } });
        const body = { actionName: "v1.echo.read", args: { value: "payload" }, callerNodeId: "caller", deadlineMs: Date.now() + 5000 };
        let laterPrepared = false;
        host.register(chimpbaseMesh({
          advertisedUrl: "http://mesh.test", meshToken: "TOKEN", heartbeatMs: 0,
          services: [service({ name: "echo", actions: { read: (_ctx, payload) => payload } })],
        }));
        host.registerRoute("later", () => null, { prepare: async (request) => {
          assert.deepEqual(await request.json(), body);
          laterPrepared = true;
          return null;
        } });
        try {
          const outcome = await host.executeRoute(new Request("http://mesh.test/__chimpbase/mesh/rpc", {
            method: "POST", headers: { [MESH_TOKEN_HEADER]: "token" }, body: JSON.stringify(body),
          }));
          assert.equal(outcome.response.status, 200);
          assert.equal(laterPrepared, true);
          assert.deepEqual((await outcome.response.json()).result, body.args);
        } finally { await host.close(); }
        const fallback = await createChimpbase({ storage: { engine: "memory" } });
        fallback.registerRoute("prepared", () => null, { prepare: async (request) => {
          assert.deepEqual(await request.json(), body.args);
          return () => null;
        } });
        fallback.registerRoute("fallback", async (request) => Response.json(await request.json()));
        try {
          const outcome = await fallback.executeRoute(new Request("http://test/fallback", {
            method: "POST", body: JSON.stringify(body.args),
          }));
          assert.deepEqual(await outcome.response.json(), body.args);
        } finally { await fallback.close(); }
        console.log("prepared bodies preserved");
      `);
      const build = Bun.spawnSync([process.execPath, "build", source, "--target=node", "--outdir", directory], {
        stdout: "pipe", stderr: "pipe",
      });
      if (build.exitCode !== 0) throw new Error(build.stderr.toString());
      const subprocess = Bun.spawn(["node", join(directory, "entry.js")], { stdout: "pipe", stderr: "pipe" });
      const [stdout, stderr, status] = await Promise.all([
        new Response(subprocess.stdout).text(), new Response(subprocess.stderr).text(), subprocess.exited,
      ]);
      if (status !== 0) throw new Error(`Node RPC probe failed (${status}): ${stderr}`);
      expect(stdout.trim()).toBe("prepared bodies preserved");
    } finally { await rm(directory, { recursive: true, force: true }); }
  });

  for (const authenticated of [false, true]) {
    test(`mesh preparation preserves an earlier body-reading guard (${authenticated ? "authenticated" : "rejected"})`, async () => {
      const host = await createChimpbase({ storage: { engine: "memory" }, secrets: { get: () => TOKEN } });
      let guarded = false;
      let invoked = false;
      const body = envelope();
      host.register(
        route("body-guard", async (request) => {
          expect(await request.clone().json() as unknown).toEqual(body);
          guarded = true;
          return new Response("guard rejection", { status: 403 });
        }),
        chimpbaseMesh({
          advertisedUrl: "http://mesh.test", heartbeatMs: 0, meshToken: "MESH_TOKEN",
          services: [service({ name: "payload", actions: { write: () => { invoked = true; } } })],
        }),
      );
      try {
        const outcome = await host.executeRoute(new Request(`http://mesh.test${DEFAULT_RPC_PATH}`, {
          body: JSON.stringify(body), method: "POST",
          headers: authenticated ? { [MESH_TOKEN_HEADER]: TOKEN } : {},
        }));
        expect(outcome.response?.status).toBe(403);
        expect(guarded).toBe(true);
        expect(invoked).toBe(false);
      } finally {
        await host.close();
      }
    });
  }

  test("preserves one payload through HTTP dispatch and RPC service invocation", async () => {
    await withRpcHost(async (host) => {
      const server = Bun.serve({
        port: 0,
        async fetch(request) {
          const outcome = await host.executeRoute(request);
          return outcome.response ?? new Response("missing route", { status: 404 });
        },
      });
      const dispatcher = createHttpDispatcher({
        callerNodeId: "caller", rpcPath: DEFAULT_RPC_PATH, tokenProvider: () => TOKEN,
      });
      try {
        for (const payload of [undefined, [], ["one"], ["one", "two"], { value: "one" }, null]) {
          const result = await dispatcher({
            actionName: "v1.payload.echo",
            args: payload,
            deadlineMs: Date.now() + 5_000,
            peer: {
              advertisedUrl: `http://127.0.0.1:${server.port}`,
              lastHeartbeatMs: Date.now(), metadata: {}, nodeId: "remote", services: [], startedAtMs: 0,
            },
          });
          expect(result).toEqual(payload === undefined
            ? { omitted: true }
            : { omitted: false, payload });
        }
      } finally {
        server.stop(true);
      }
    });
  });
});
