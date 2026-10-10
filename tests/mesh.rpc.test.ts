import { describe, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { createChimpbase } from "../packages/bun/src/library.ts";
import {
  chimpbaseMesh,
  DEFAULT_RPC_PATH,
  MESH_TOKEN_HEADER,
  RPC_EXECUTE_ACTION,
  service,
} from "../packages/mesh/src/index.ts";
import { createHttpDispatcher } from "../packages/mesh/src/transport-http.ts";
import { action } from "../packages/runtime/index.ts";
import { readJsonResponse } from "./support/http.ts";

const TOKEN = "mesh-rpc-test-token";
type MeshHost = Awaited<ReturnType<typeof createChimpbase>>;
type RpcFailure = { error: string; ok: false };

async function withRpcHost(run: (host: MeshHost) => Promise<void>): Promise<void> {
  const projectDir = await mkdtemp(join(tmpdir(), "chimpbase-mesh-rpc-"));
  const host = await createChimpbase({
    projectDir,
    secrets: { get: (name) => name === "MESH_TOKEN" ? TOKEN : null },
    storage: { engine: "memory" },
  });
  try {
    host.register(
      chimpbaseMesh({
        advertisedUrl: "http://mesh.test",
        heartbeatMs: 0,
        meshToken: "MESH_TOKEN",
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
        body: "{", method: "POST",
      }));
      expect(invalid.response?.status).toBe(400);
      expect(await readJsonResponse<RpcFailure>(invalid.response)).toEqual({ error: "invalid json body", ok: false });
      const wrongMethod = await host.executeRoute(new Request(`http://mesh.test${DEFAULT_RPC_PATH}`));
      expect(wrongMethod.response?.status).toBe(405);
      expect((await host.executeAction("private.read")).result).toBeNull();
    });
  });

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
