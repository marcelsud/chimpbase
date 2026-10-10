import { expect, spyOn, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { createChimpbase } from "../packages/bun/src/library.ts";
import { chimpbaseMesh, service } from "../packages/mesh/src/index.ts";
import { ensureRegistrySchema, listLiveNodes, upsertNode } from "../packages/mesh/src/registry.ts";
import { onStart, v } from "../packages/runtime/index.ts";

for (const fixture of [
  { metadata: { cpuLoad: 0.9 }, expected: "remote" },
  { metadata: { cpuLoad: 0.3 }, expected: "local" },
  { metadata: {}, expected: "local" },
  { metadata: { cpuLoad: "busy" }, expected: "local" },
]) {
  test(`CPU routing uses announced local metadata ${JSON.stringify(fixture.metadata)}`, async () => {
    const projectDir = await mkdtemp(join(tmpdir(), "chimpbase-mesh-cpu-"));
    const host = await createChimpbase({
      projectDir,
      secrets: { get: () => "token" },
      storage: { engine: "memory" },
    });
    let remoteCalls = 0;
    const fetchMock = spyOn(globalThis, "fetch").mockImplementation(Object.assign(async () => {
      remoteCalls += 1;
      return Response.json({ ok: true, result: "remote" });
    }, { preconnect: () => {} }));
    host.register(
      onStart("seed-cpu-peer", async (ctx) => {
        await ensureRegistrySchema(ctx);
        await upsertNode(ctx, {
          advertisedUrl: "http://mesh-peer.test",
          metadata: { cpuLoad: 0.6 },
          nodeId: "remote",
          services: [{ actions: ["v1.identity.get"], events: [], name: "identity", version: 1 }],
          startedAtMs: Date.now(),
        });
      }),
      chimpbaseMesh({
        advertisedUrl: "http://mesh-local.test",
        defaultStrategy: "cpu",
        heartbeatMs: 0,
        meshToken: "MESH_TOKEN",
        meta: fixture.metadata,
        services: [service({ name: "identity", actions: {
          get: () => "local",
          choose: async (ctx) => await ctx.mesh?.call("v1.identity.get", {}, v.string()),
          localFirst: async (ctx) => await ctx.mesh?.call("v1.identity.get", {}, v.string(), { strategy: "local-first" }),
          metadata: async (ctx) => (await listLiveNodes(ctx, 0)).find((node) => node.nodeId === ctx.mesh?.nodeId())?.metadata,
        } })],
      }),
    );
    let started: Awaited<ReturnType<typeof host.start>> | undefined;
    try {
      started = await host.start({ serve: false, runWorker: false });
      expect((await host.executeAction("v1.identity.metadata")).result).toEqual(fixture.metadata);
      expect((await host.executeAction("v1.identity.choose")).result).toBe(fixture.expected);
      expect((await host.executeAction("v1.identity.localFirst")).result).toBe("local");
      expect(remoteCalls).toBe(fixture.expected === "remote" ? 1 : 0);
    } finally {
      fetchMock.mockRestore();
      await started?.stop();
      await host.close();
      await rm(projectDir, { recursive: true, force: true });
    }
  });
}
