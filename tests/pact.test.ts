import { afterEach, describe, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";

import { defineChimpbaseApp } from "../packages/core/index.ts";
import { createChimpbase } from "../packages/bun/src/library.ts";
import { action, v } from "../packages/runtime/index.ts";
import {
  pact,
  interaction,
  verifyPact,
  serializePact,
  serializePactToJson,
  deserializePactJson,
} from "../packages/pact/src/index.ts";

const cleanupDirs: string[] = [];

afterEach(async () => {
  while (cleanupDirs.length > 0) {
    const dir = cleanupDirs.pop();
    if (dir) {
      await rm(dir, { recursive: true, force: true });
    }
  }
});

describe("@chimpbase/pact", () => {
  describe("contract definition", () => {
    test("creates a pact with action interactions", () => {
      const contract = pact({
        consumer: "order-service",
        provider: "inventory-service",
        interactions: [
          interaction.action("reserveStock", {
            states: ["product has stock"],
            args: v.object({ sku: v.string(), quantity: v.number() }),
            result: v.object({ reservationId: v.string() }),
            example: {
              args: [{ sku: "SKU-001", quantity: 2 }],
              result: { reservationId: "res-1" },
            },
          }),
        ],
      });

      expect(contract.consumer).toBe("order-service");
      expect(contract.provider).toBe("inventory-service");
      expect(contract.interactions).toHaveLength(1);
      expect(contract.interactions[0].kind).toBe("action");
    });

    test("creates a pact with event interactions", () => {
      const contract = pact({
        consumer: "order-service",
        provider: "inventory-service",
        interactions: [
          interaction.event("stock.reserved", {
            states: ["a reservation was just created"],
            payload: v.object({
              reservationId: v.string(),
              sku: v.string(),
            }),
          }),
        ],
      });

      expect(contract.interactions).toHaveLength(1);
      expect(contract.interactions[0].kind).toBe("event");
    });

    test("creates a pact with worker interactions", () => {
      const contract = pact({
        consumer: "notification-service",
        provider: "order-service",
        interactions: [
          interaction.worker("order.notify", {
            payload: v.object({ orderId: v.string(), email: v.string() }),
            example: { orderId: "ord-1", email: "test@test.com" },
          }),
        ],
      });

      expect(contract.interactions).toHaveLength(1);
      expect(contract.interactions[0].kind).toBe("worker");
    });
  });

  describe("serialization", () => {
    test("serializes a pact to JSON and back", () => {
      const contract = pact({
        consumer: "consumer-a",
        provider: "provider-b",
        interactions: [
          interaction.action("greet", {
            states: ["user exists"],
            args: v.object({ name: v.string() }),
            result: v.object({ message: v.string() }),
            example: {
              args: [{ name: "Alice" }],
              result: { message: "Hello, Alice!" },
            },
          }),
          interaction.event("user.greeted", {
            states: ["user was greeted"],
            payload: v.object({ name: v.string() }),
          }),
        ],
      });

      const serialized = serializePact(contract);

      expect(serialized.consumer).toBe("consumer-a");
      expect(serialized.provider).toBe("provider-b");
      expect(serialized.interactions).toHaveLength(2);

      const actionInteraction = serialized.interactions[0];
      expect(actionInteraction.kind).toBe("action");
      if (actionInteraction.kind === "action") {
        expect(actionInteraction.name).toBe("greet");
        expect(actionInteraction.argsSchema).toEqual({
          properties: { name: { type: "string" } },
          required: ["name"],
          type: "object",
        });
        expect(actionInteraction.resultSchema).toEqual({
          properties: { message: { type: "string" } },
          required: ["message"],
          type: "object",
        });
        expect(actionInteraction.example).toEqual({
          args: [{ name: "Alice" }],
          result: { message: "Hello, Alice!" },
        });
      }

      const json = serializePactToJson(contract);
      const deserialized = deserializePactJson(json);

      expect(deserialized.consumer).toBe("consumer-a");
      expect(deserialized.interactions).toHaveLength(2);
    });
  });

  describe("provider verification", () => {
    test("verifies action interactions against a real host", async () => {
      const projectDir = await mkdtemp(join(tmpdir(), "chimpbase-pact-action-"));
      cleanupDirs.push(projectDir);

      const greet = action({
        name: "greet",
        args: v.object({ name: v.string() }),
        async handler(_ctx, args) {
          return { message: `Hello, ${args.name}!` };
        },
      });

      const host = await createChimpbase({
        app: defineChimpbaseApp({
          project: { name: "pact-action-test" },
          registrations: [greet],
        }),
        projectDir,
        storage: { engine: "memory" },
      });

      try {
        const contract = pact({
          consumer: "client-app",
          provider: "pact-action-test",
          interactions: [
            interaction.action("greet", {
              args: v.object({ name: v.string() }),
              result: v.object({ message: v.string() }),
              example: {
                args: [{ name: "Alice" }],
                result: { message: "Hello, Alice!" },
              },
            }),
          ],
        });

        const result = await verifyPact({ host, pact: contract });

        expect(result.passed).toBe(1);
        expect(result.failed).toBe(0);
        expect(result.total).toBe(1);
        expect(result.results[0].status).toBe("passed");
      } finally {
        await host.close();
      }
    });

    test("reports failure when action result does not match contract", async () => {
      const projectDir = await mkdtemp(join(tmpdir(), "chimpbase-pact-mismatch-"));
      cleanupDirs.push(projectDir);

      // Action returns a number for "count", but contract expects a string
      const countItems = action("countItems", async () => {
        return { count: 42 };
      });

      const host = await createChimpbase({
        app: defineChimpbaseApp({
          project: { name: "pact-mismatch-test" },
          registrations: [countItems],
        }),
        projectDir,
        storage: { engine: "memory" },
      });

      try {
        const contract = pact({
          consumer: "client-app",
          provider: "pact-mismatch-test",
          interactions: [
            interaction.action("countItems", {
              result: v.object({ count: v.string() }), // expects string, handler returns number
              example: {
                args: [],
                result: { count: "42" },
              },
            }),
          ],
        });

        const result = await verifyPact({ host, pact: contract });

        expect(result.passed).toBe(0);
        expect(result.failed).toBe(1);
        if (result.results[0].status === "failed") {
          expect(result.results[0].failure.kind).toBe("result_mismatch");
        }
      } finally {
        await host.close();
      }
    });

    test("reports failure when action throws", async () => {
      const projectDir = await mkdtemp(join(tmpdir(), "chimpbase-pact-throw-"));
      cleanupDirs.push(projectDir);

      const failAction = action("failAction", async () => {
        throw new Error("something went wrong");
      });

      const host = await createChimpbase({
        app: defineChimpbaseApp({
          project: { name: "pact-throw-test" },
          registrations: [failAction],
        }),
        projectDir,
        storage: { engine: "memory" },
      });

      try {
        const contract = pact({
          consumer: "client-app",
          provider: "pact-throw-test",
          interactions: [
            interaction.action("failAction", {
              example: { args: [] },
            }),
          ],
        });

        const result = await verifyPact({ host, pact: contract });

        expect(result.failed).toBe(1);
        if (result.results[0].status === "failed") {
          expect(result.results[0].failure.kind).toBe("action_threw");
        }
      } finally {
        await host.close();
      }
    });

    test("verifies event interactions by capturing emitted events", async () => {
      const projectDir = await mkdtemp(join(tmpdir(), "chimpbase-pact-event-"));
      cleanupDirs.push(projectDir);

      const createOrder = action("createOrder", async (ctx) => {
        ctx.pubsub.publish("order.created", {
          orderId: "ord-123",
          total: 99.99,
        });
        return { orderId: "ord-123" };
      });

      const host = await createChimpbase({
        app: defineChimpbaseApp({
          project: { name: "pact-event-test" },
          registrations: [createOrder],
        }),
        projectDir,
        storage: { engine: "memory" },
      });

      try {
        const contract = pact({
          consumer: "notification-service",
          provider: "pact-event-test",
          interactions: [
            interaction.event("order.created", {
              states: ["an order is placed"],
              payload: v.object({
                orderId: v.string(),
                total: v.number(),
              }),
            }),
          ],
        });

        const result = await verifyPact({
          host,
          pact: contract,
          states: {
            "an order is placed": async (h) => {
              await h.executeAction("createOrder");
            },
          },
        });

        expect(result.passed).toBe(1);
        expect(result.failed).toBe(0);
      } finally {
        await host.close();
      }
    });

    test("reports failure when expected event is not emitted", async () => {
      const projectDir = await mkdtemp(join(tmpdir(), "chimpbase-pact-no-event-"));
      cleanupDirs.push(projectDir);

      const noopAction = action("noopAction", async () => {
        return { ok: true };
      });

      const host = await createChimpbase({
        app: defineChimpbaseApp({
          project: { name: "pact-no-event-test" },
          registrations: [noopAction],
        }),
        projectDir,
        storage: { engine: "memory" },
      });

      try {
        const contract = pact({
          consumer: "client",
          provider: "pact-no-event-test",
          interactions: [
            interaction.event("order.created", {
              states: ["an action runs"],
              payload: v.object({ orderId: v.string() }),
            }),
          ],
        });

        const result = await verifyPact({
          host,
          pact: contract,
          states: {
            "an action runs": async (h) => {
              await h.executeAction("noopAction");
            },
          },
        });

        expect(result.failed).toBe(1);
        if (result.results[0].status === "failed") {
          expect(result.results[0].failure.kind).toBe("event_not_emitted");
        }
      } finally {
        await host.close();
      }
    });

    test("reports failure for missing state handler", async () => {
      const projectDir = await mkdtemp(join(tmpdir(), "chimpbase-pact-missing-state-"));
      cleanupDirs.push(projectDir);

      const host = await createChimpbase({
        app: defineChimpbaseApp({
          project: { name: "pact-missing-state-test" },
          registrations: [
            action("greet", async () => ({ message: "hi" })),
          ],
        }),
        projectDir,
        storage: { engine: "memory" },
      });

      try {
        const contract = pact({
          consumer: "client",
          provider: "pact-missing-state-test",
          interactions: [
            interaction.action("greet", {
              states: ["user exists"],
              example: { args: [] },
            }),
          ],
        });

        // No states provided
        const result = await verifyPact({ host, pact: contract });

        expect(result.failed).toBe(1);
        if (result.results[0].status === "failed") {
          expect(result.results[0].failure.kind).toBe("missing_state");
        }
      } finally {
        await host.close();
      }
    });

    test("verifies multiple interactions in a single pact", async () => {
      const projectDir = await mkdtemp(join(tmpdir(), "chimpbase-pact-multi-"));
      cleanupDirs.push(projectDir);

      const createUser = action({
        name: "createUser",
        args: v.object({ name: v.string() }),
        async handler(ctx, args) {
          ctx.pubsub.publish("user.created", { name: args.name });
          return { userId: "user-1", name: args.name };
        },
      });

      const getUser = action("getUser", async () => {
        return { userId: "user-1", name: "Alice" };
      });

      const host = await createChimpbase({
        app: defineChimpbaseApp({
          project: { name: "pact-multi-test" },
          registrations: [createUser, getUser],
        }),
        projectDir,
        storage: { engine: "memory" },
      });

      try {
        const contract = pact({
          consumer: "frontend",
          provider: "pact-multi-test",
          interactions: [
            interaction.action("createUser", {
              args: v.object({ name: v.string() }),
              result: v.object({ userId: v.string(), name: v.string() }),
              example: {
                args: [{ name: "Alice" }],
                result: { userId: "user-1", name: "Alice" },
              },
            }),
            interaction.action("getUser", {
              result: v.object({ userId: v.string(), name: v.string() }),
              example: {
                args: [],
                result: { userId: "user-1", name: "Alice" },
              },
            }),
            interaction.event("user.created", {
              states: ["a user is created"],
              payload: v.object({ name: v.string() }),
            }),
          ],
        });

        const result = await verifyPact({
          host,
          pact: contract,
          states: {
            "a user is created": async (h) => {
              await h.executeAction("createUser", [{ name: "Bob" }]);
            },
          },
        });

        expect(result.passed).toBe(3);
        expect(result.failed).toBe(0);
        expect(result.total).toBe(3);
      } finally {
        await host.close();
      }
    });

    test("calls lifecycle hooks during verification", async () => {
      const projectDir = await mkdtemp(join(tmpdir(), "chimpbase-pact-hooks-"));
      cleanupDirs.push(projectDir);

      const host = await createChimpbase({
        app: defineChimpbaseApp({
          project: { name: "pact-hooks-test" },
          registrations: [
            action("ping", async () => ({ pong: true })),
          ],
        }),
        projectDir,
        storage: { engine: "memory" },
      });

      try {
        const contract = pact({
          consumer: "client",
          provider: "pact-hooks-test",
          interactions: [
            interaction.action("ping", {
              result: v.object({ pong: v.boolean() }),
              example: { args: [] },
            }),
          ],
        });

        const started: string[] = [];
        const passed: string[] = [];

        const result = await verifyPact({
          host,
          pact: contract,
          onInteractionStart: (i) => started.push(i.kind),
          onInteractionPass: (i) => passed.push(i.kind),
        });

        expect(result.passed).toBe(1);
        expect(started).toEqual(["action"]);
        expect(passed).toEqual(["action"]);
      } finally {
        await host.close();
      }
    });
  });
});
