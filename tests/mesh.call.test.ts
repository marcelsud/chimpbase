import { afterEach, describe, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import {
  chimpbaseMesh,
  service,
  MeshNoAvailableNodeError,
  type MeshCallMiddleware,
} from "../packages/mesh/src/index.ts";
import { createChimpbase } from "../packages/bun/src/library.ts";
import { v } from "../packages/runtime/index.ts";


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

describe("@chimpbase/mesh ctx.mesh.call", () => {
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
