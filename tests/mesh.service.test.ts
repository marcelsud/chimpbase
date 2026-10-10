import { afterEach, describe, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { chimpbaseMesh, service } from "../packages/mesh/src/index.ts";
import { resolveService } from "../packages/mesh/src/service.ts";
import type { AnyServiceDefinition } from "../packages/mesh/src/types.ts";
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
  const projectDir = await mkdtemp(join(tmpdir(), "chimpbase-mesh-test-"));
  cleanupDirs.push(projectDir);

  return await createChimpbase({
    project: { name: "mesh-test" },
    projectDir,
    storage: { engine: "memory" },
  });
}

describe("@chimpbase/mesh service()", () => {
  test("shared mixin ancestors merge without being treated as cycles", async () => {
    const host = await createMeshHost();
    try {
      const common = service({
        name: "common",
        settings: { source: "common", winner: "common" },
        methods: { tag: () => "common" },
        actions: { ping: () => "common" },
      });
      const left = service({ name: "left", mixins: [common], settings: { left: true, winner: "left" } });
      const right = service({ name: "right", mixins: [common], settings: { right: true, winner: "right" } });
      const combined = service({
        name: "combined",
        mixins: [left, right],
        settings: { winner: "combined" },
        actions: {
          inspect: (_ctx, _args: unknown, self) => ({ settings: self.settings, tag: self.methods.tag() }),
        },
      });
      host.register(chimpbaseMesh({ services: [combined], transport: "local-only" }));
      const started = await host.start({ serve: false, runWorker: false });
      try {
        expect((await host.executeAction("v1.combined.ping")).result).toBe("common");
        expect((await host.executeAction("v1.combined.inspect")).result).toEqual({
          settings: { source: "common", left: true, right: true, winner: "combined" },
          tag: "common",
        });
      } finally {
        await started.stop();
      }
    } finally {
      await host.close();
    }
  });

  test("mixin cycles still reject direct and indirect recursion", () => {
    const direct: AnyServiceDefinition = { name: "direct" };
    direct.mixins = [direct];
    expect(() => resolveService(direct)).toThrow('service "direct" has a circular mixin reference');

    const first: AnyServiceDefinition = { name: "first" };
    const second: AnyServiceDefinition = { name: "second", mixins: [first] };
    first.mixins = [second];
    expect(() => resolveService(first)).toThrow('service "first" has a circular mixin reference');
  });

  test("prefixes actions with version", async () => {
    const host = await createMeshHost();
    try {
      const users = service({
        name: "users",
        version: 1,
        actions: {
          create: async (_ctx, args: { email: string }) => ({ id: "u1", email: args.email }),
        },
      });

      host.register(chimpbaseMesh({ services: [users], transport: "local-only" }));

      const started = await host.start({ serve: false, runWorker: false });
      try {
        const outcome = await host.executeAction("v1.users.create", [{ email: "a@b.com" }]);
        expect(outcome.result).toEqual({ id: "u1", email: "a@b.com" });
      } finally {
        await started.stop();
      }
    } finally {
      await host.close();
    }
  });

  test("exposes settings and methods via self", async () => {
    const host = await createMeshHost();
    try {
      const svc = service({
        name: "greeter",
        settings: { greeting: "hola" },
        methods: {
          shout: (value: string) => value.toUpperCase(),
        },
        actions: {
          say: async (_ctx, args: { name: string }, self) =>
            `${self.settings.greeting} ${self.methods.shout(args.name)}`,
        },
      });

      host.register(chimpbaseMesh({ services: [svc], transport: "local-only" }));

      const started = await host.start({ serve: false, runWorker: false });
      try {
        const outcome = await host.executeAction("v1.greeter.say", [{ name: "bob" }]);
        expect(outcome.result).toBe("hola BOB");
      } finally {
        await started.stop();
      }
    } finally {
      await host.close();
    }
  });

  test("mixins deep-merge actions and methods", async () => {
    const host = await createMeshHost();
    try {
      const auditable = service({
        name: "auditable",
        methods: { tag: () => "audit" },
        actions: { ping: async () => "pong" },
      });

      const svc = service({
        name: "orders",
        mixins: [auditable],
        actions: {
          create: async (_ctx, _args: unknown, self) => ({ tag: self.methods.tag(), source: "orders" }),
        },
      });

      host.register(chimpbaseMesh({ services: [svc], transport: "local-only" }));

      const started = await host.start({ serve: false, runWorker: false });
      try {
        const ping = await host.executeAction("v1.orders.ping", []);
        expect(ping.result).toBe("pong");

        const create = await host.executeAction("v1.orders.create", [{}]);
        expect(create.result).toEqual({ tag: "audit", source: "orders" });
      } finally {
        await started.stop();
      }
    } finally {
      await host.close();
    }
  });

  test("ctx.mesh.call resolves local actions", async () => {
    const host = await createMeshHost();
    try {
      const svc = service({
        name: "calc",
        actions: {
          add: async (_ctx, args: { a: number; b: number }) => args.a + args.b,
          double: async (ctx, args: { n: number }) => {
            const mesh = ctx.mesh;
            if (!(mesh !== undefined)) throw new Error("mesh missing");
            const sum = await mesh.call("v1.calc.add", { a: args.n, b: args.n }, v.number());
            return sum;
          },
        },
      });

      host.register(chimpbaseMesh({ services: [svc], transport: "local-only" }));

      const started = await host.start({ serve: false, runWorker: false });
      try {
        const outcome = await host.executeAction("v1.calc.double", [{ n: 7 }]);
        expect(outcome.result).toBe(14);
      } finally {
        await started.stop();
      }
    } finally {
      await host.close();
    }
  });
});
