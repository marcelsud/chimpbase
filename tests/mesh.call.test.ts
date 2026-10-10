import { afterEach, describe, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import {
  chimpbaseMesh,
  service,
  MeshNoAvailableNodeError,
  MeshCallError,
  type MeshCallMiddleware,
} from "../packages/mesh/src/index.ts";
import { createChimpbase } from "../packages/bun/src/library.ts";
import { onStart, v, type ChimpbaseContext } from "../packages/runtime/index.ts";
import { createCallDispatcher } from "../packages/mesh/src/call.ts";
import { MeshPeerCache } from "../packages/mesh/src/discovery.ts";
import { createHttpDispatcher } from "../packages/mesh/src/transport-http.ts";



const cleanupDirs: string[] = [];

afterEach(async () => {
  while (cleanupDirs.length > 0) {
    const dir = cleanupDirs.pop();
    if ((dir !== undefined && dir.length > 0)) {
      await rm(dir, { recursive: true, force: true });
    }
  }
});

async function createMeshHost() {
  const projectDir = await mkdtemp(join(tmpdir(), "chimpbase-mesh-call-"));
  cleanupDirs.push(projectDir);
  return await createChimpbase({
    project: { name: "mesh-call-test" },
    projectDir,
    storage: { engine: "memory" },
  });

}
test("HTTP dispatcher wraps malformed JSON as MeshCallError", async () => {
  const server = Bun.serve({
    port: 0,
    fetch() {
      return new Response("{", {
        headers: { "content-type": "application/json" },
        status: 200,
      });
    },
  });
  const dispatcher = createHttpDispatcher({
    callerNodeId: "caller",
    rpcPath: "/rpc",
    tokenProvider: () => "token",
  });

  try {
    await expect(dispatcher({
      actionName: "v1.test.invalid",
      args: {},
      deadlineMs: Date.now() + 1_000,
      peer: {
        advertisedUrl: `http://127.0.0.1:${server.port}`,
        lastHeartbeatMs: Date.now(),
        metadata: {},
        nodeId: "peer",
        services: [],
        startedAtMs: Date.now(),
      },
    })).rejects.toBeInstanceOf(MeshCallError);
  } finally {
    server.stop(true);
  }
});

describe("@chimpbase/mesh ctx.mesh.call", () => {
  test("overlapping middleware calls keep their own action context", async () => {
    const hostA = await createMeshHost();
    const hostB = await createMeshHost();
    let ctxA: ChimpbaseContext | undefined;
    let ctxB: ChimpbaseContext | undefined;
    hostA.register(
      chimpbaseMesh({ services: [service({ name: "identity", actions: { get: () => "a" } })], transport: "local-only" }),
      onStart("capture-a", (ctx) => { ctxA = ctx; }),
    );
    hostB.register(
      chimpbaseMesh({ services: [service({ name: "identity", actions: { get: () => "b" } })], transport: "local-only" }),
      onStart("capture-b", (ctx) => { ctxB = ctx; }),
    );

    const startedA = await hostA.start({ serve: false, runWorker: false });
    const startedB = await hostB.start({ serve: false, runWorker: false });
    let releaseA = () => {};
    let releaseB = () => {};
    const gateA = new Promise<void>((resolve) => { releaseA = resolve; });
    const gateB = new Promise<void>((resolve) => { releaseB = resolve; });
    try {
      if (ctxA === undefined || ctxB === undefined) throw new Error("contexts missing");
      const dispatcher = createCallDispatcher({
        cache: new MeshPeerCache(30_000),
        defaultRetries: 0,
        defaultStrategy: "local-first",
        defaultTimeoutMs: 1_000,
        localActionNames: new Set(["v1.identity.get"]),
        localNodeId: "local",
        middleware: [(next) => async (name, args, validator, options) => {
          await (args === "a" ? gateA : gateB);
          return await next(name, args, validator, options);
        }],
        remoteDispatcher: null,
      });
      const first = dispatcher(ctxA, "v1.identity.get", "a", v.string(), {});
      const second = dispatcher(ctxB, "v1.identity.get", "b", v.string(), {});
      try {
        releaseA();
        expect(await first).toBe("a");
        releaseB();
        expect(await second).toBe("b");
      } finally {
        releaseA();
        releaseB();
        await Promise.allSettled([first, second]);
      }
    } finally {
      await startedA.stop();
      await startedB.stop();
      await hostA.close();
      await hostB.close();
    }
  });

  test("uses fallback when no node serves the action", async () => {
    const host = await createMeshHost();
    try {
      let captured: Error | null = null;
      const svc = service({
        name: "router",
        actions: {
          tryCall: async (ctx) => {
            if (!(ctx.mesh !== undefined)) throw new Error("mesh missing");
            return await ctx.mesh.call("v1.missing.thing", {}, v.string(), {
              fallback: (error) => {
                captured = error;
                return "fallback-result";
              },
            });
          },
        },
      });

      host.register(chimpbaseMesh({ services: [svc], transport: "local-only" }));
      const started = await host.start({ serve: false, runWorker: false });
      try {
        const outcome = await host.executeAction("v1.router.tryCall");
        expect(outcome.result).toBe("fallback-result");
        expect(captured).toBeInstanceOf(MeshNoAvailableNodeError);
      } finally {
        await started.stop();
      }
    } finally {
      await host.close();
    }
  });

  test("middleware wraps the dispatcher", async () => {
    const host = await createMeshHost();
    try {
      const trace: string[] = [];

      const logging: MeshCallMiddleware = (next) => async (name, args, resultValidator, opts) => {
        trace.push(`before:${name}`);
        const result = await next(name, args, resultValidator, opts);
        trace.push(`after:${name}`);
        return result;
      };

      const svc = service({
        name: "calc",
        actions: {
          add: async (_ctx, args: { a: number; b: number }) => args.a + args.b,
          run: async (ctx) => {
            if (!(ctx.mesh !== undefined)) throw new Error("mesh missing");
            return await ctx.mesh.call("v1.calc.add", { a: 2, b: 3 }, v.number());
          },
        },
      });

      host.register(chimpbaseMesh({
        middleware: [logging],
        services: [svc],
        transport: "local-only",
      }));

      const started = await host.start({ serve: false, runWorker: false });
      try {
        const outcome = await host.executeAction("v1.calc.run");
        expect(outcome.result).toBe(5);
        expect(trace).toEqual(["before:v1.calc.add", "after:v1.calc.add"]);
      } finally {
        await started.stop();
      }
    } finally {
      await host.close();
    }
  });

  test("retry attempts until success", async () => {
    const host = await createMeshHost();
    try {
      let attempts = 0;
      const svc = service({
        name: "flaky",
        actions: {
          flaky: async () => {
            attempts += 1;
            if (attempts < 3) {
              throw new Error("transient");
            }
            return "ok";
          },
          run: async (ctx) => {
            if (!(ctx.mesh !== undefined)) throw new Error("mesh missing");
            return await ctx.mesh.call("v1.flaky.flaky", {}, v.string(), {
              retry: { attempts: 3, delayMs: 1 },
            });
          },
        },
      });

      host.register(chimpbaseMesh({ services: [svc], transport: "local-only" }));
      const started = await host.start({ serve: false, runWorker: false });
      try {
        const outcome = await host.executeAction("v1.flaky.run");
        expect(outcome.result).toBe("ok");
        expect(attempts).toBe(3);
      } finally {
        await started.stop();
      }
    } finally {
      await host.close();
    }
  });
});
