import { describe, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { createChimpbase } from "../packages/bun/src/library.ts";
import { chimpbaseMesh, DEFAULT_RPC_PATH, MESH_TOKEN_HEADER, service } from "../packages/mesh/src/index.ts";
import { action, v } from "../packages/runtime/index.ts";

const postgresUrl = process.env.CHIMPBASE_TEST_PG_URL;
const describeIfPg = postgresUrl === undefined || postgresUrl.length === 0 ? describe.skip : describe;

async function createHttpNodes() {
  if (postgresUrl === undefined || postgresUrl.length === 0) throw new Error("PostgreSQL test URL missing");
  const projectDir = await mkdtemp(join(tmpdir(), "chimpbase-mesh-http-"));
  const options = {
    projectDir,
    storage: { engine: "postgres" as const, url: postgresUrl },
    secrets: { get: () => "mesh-http-test-token" },
  };
  const caller = await createChimpbase(options);
  const target = await createChimpbase(options);
  const callerServer = Bun.serve({ port: 0, hostname: "127.0.0.1", fetch: async (request) =>
    (await caller.executeRoute(request)).response ?? new Response("not found", { status: 404 }) });
  const targetServer = Bun.serve({ port: 0, hostname: "127.0.0.1", fetch: async (request) =>
    (await target.executeRoute(request)).response ?? new Response("not found", { status: 404 }) });
  const callerUrl = `http://127.0.0.1:${callerServer.port}`;
  const targetUrl = `http://127.0.0.1:${targetServer.port}`;
  const writeKey = `${projectDir}:partial-write`;
  let touches = 0;
  let privateCalls = 0;

  caller.register(chimpbaseMesh({
    advertisedUrl: callerUrl, meshToken: "TOKEN", heartbeatMs: 20, defaultTimeoutMs: 1_000,
    services: [service({ name: "httpCaller", actions: {
      relay: async (ctx, args: { name: string; input?: unknown }) => {
        if (ctx.mesh === undefined) throw new Error("mesh missing");
        return await ctx.mesh.call(args.name, args.input, v.unknown());
      },
      leaf: () => 42,
      peers: (ctx) => ctx.mesh?.peers().flatMap((peer) => peer.services.flatMap((entry) => entry.actions)),
    } })],
  }));
  target.register(chimpbaseMesh({
    advertisedUrl: targetUrl, meshToken: "TOKEN", heartbeatMs: 20, defaultTimeoutMs: 1_000,
    services: [service({ name: "httpTarget", actions: {
      echo: (_ctx, input: unknown) => input,
      roundTrip: async (ctx) => {
        if (ctx.mesh === undefined) throw new Error("mesh missing");
        return await ctx.mesh.call("v1.httpCaller.leaf", undefined, v.number());
      },
      failWrite: async (ctx) => { await ctx.kv.set(writeKey, true); throw new Error("target failed"); },
      value: async (ctx) => await ctx.kv.get(writeKey),
      touch: () => ++touches,
    } })],
  }), action("private.mesh-http", () => ++privateCalls));

  let startedCaller: Awaited<ReturnType<typeof caller.start>> | undefined;
  let startedTarget: Awaited<ReturnType<typeof target.start>> | undefined;
  const close = async () => {
    callerServer.stop(true);
    targetServer.stop(true);
    await startedCaller?.stop();
    await startedTarget?.stop();
    await caller.close();
    await target.close();
    await rm(projectDir, { recursive: true, force: true });
  };
  try {
    startedCaller = await caller.start({ serve: false, runWorker: false });
    startedTarget = await target.start({ serve: false, runWorker: false });
    const deadline = Date.now() + 2_000;
    while (!v.array(v.string()).parse((await caller.executeAction("v1.httpCaller.peers")).result)
      .includes("v1.httpTarget.echo")) {
      if (Date.now() >= deadline) throw new Error("HTTP peer discovery timed out");
      await Bun.sleep(10);
    }
  } catch (error) {
    await close();
    throw error;
  }

  return {
    caller, target,
    touches: () => touches,
    privateCalls: () => privateCalls,
    rpc: async (name: string, deadlineMs = Date.now() + 1_000, token = "mesh-http-test-token") => await fetch(
      `${targetUrl}${DEFAULT_RPC_PATH}`, {
        method: "POST",
        headers: { "content-type": "application/json", [MESH_TOKEN_HEADER]: token },
        body: JSON.stringify({ actionName: name, args: {}, callerNodeId: "http-test", deadlineMs }),
      },
    ),
    close,
  };
}

describeIfPg("mesh HTTP across PostgreSQL nodes", () => {
  test("remote calls preserve empty and single-item array payloads", async () => {
    const nodes = await createHttpNodes();
    try {
      for (const input of [[], [1], [1, 2]]) {
        expect((await nodes.caller.executeAction("v1.httpCaller.relay", {
          name: "v1.httpTarget.echo", input,
        })).result).toEqual(input);
      }
    } finally { await nodes.close(); }
  });

  test("an HTTP action can call back to its caller without blocking behind the original action", async () => {
    const nodes = await createHttpNodes();
    try {
      expect((await nodes.caller.executeAction("v1.httpCaller.relay", {
        name: "v1.httpTarget.roundTrip",
      })).result).toBe(42);
    } finally { await nodes.close(); }
  });

  test("failed RPC writes roll back before the HTTP error reaches the caller", async () => {
    const nodes = await createHttpNodes();
    try {
      const response = await nodes.rpc("v1.httpTarget.failWrite");
      expect(response.status).toBe(500);
      expect((await nodes.target.executeAction("v1.httpTarget.value")).result).toBeNull();
    } finally { await nodes.close(); }
  });

  test("bad tokens, private actions, and expired deadlines cannot execute targets", async () => {
    const nodes = await createHttpNodes();
    try {
      expect((await nodes.rpc("v1.httpTarget.touch", Date.now() + 1_000, "wrong-token")).status).toBe(401);
      expect((await nodes.rpc("private.mesh-http")).ok).toBe(false);
      expect((await nodes.rpc("v1.httpTarget.touch", Date.now() - 1_000)).ok).toBe(false);
      expect(nodes.privateCalls()).toBe(0);
      expect(nodes.touches()).toBe(0);
    } finally { await nodes.close(); }
  });
});
