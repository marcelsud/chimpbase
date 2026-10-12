import { expect, test } from "bun:test";

import { createChimpbase } from "../packages/bun/src/library.ts";
import { defineChimpbaseModuleImplementation, defineChimpbaseModuleInterface } from "../packages/core/index.ts";
import { interaction, pact, verifyPact } from "../packages/pact/src/index.ts";
import { readJsonResponse } from "./support/http.ts";
import {
  action,
  plugin,
  route,
  v,
  workflow,
  workflowActionStep,
  type ChimpbaseContext,
  type ChimpbaseRouteEnv,
  type InferActionsFromModules,
  type InferActionsFromRecord,
} from "../packages/runtime/index.ts";

const batch = action({
  name: "batch",
  args: v.array(v.string()),
  handler: (_ctx, input) => input.join(","),
});
const tuple = action("tuple", (_ctx, label: string, count: number) => label.repeat(count));
const nullableBatch = action({
  name: "nullableBatch",
  args: v.union(v.array(v.string()), v.null()),
  handler: (_ctx, input) => input?.join(",") ?? "none",
});
type ActionContracts = InferActionsFromRecord<{ batch: typeof batch; tuple: typeof tuple }>;

// Checked by `bun run typecheck`; these calls must never execute.
function checkActionTypes(
  ctx: ChimpbaseContext<ActionContracts>,
  env: ChimpbaseRouteEnv<ActionContracts>,
  host: Awaited<ReturnType<typeof createChimpbase>>,
  modules: ChimpbaseContext<InferActionsFromModules<[{ batch: typeof batch }]>>,
  untyped: ChimpbaseContext,
) {
  const reference: Promise<string> = batch(["a", "b"]);
  const context: Promise<string> = ctx.action(batch, ["a", "b"]);
  const named: Promise<string> = ctx.action("batch", ["a", "b"]);
  const routeResult: Promise<string> = env.action("batch", ["a", "b"]);
  const hostResult: Promise<{ result: string; emittedEvents: unknown[] }> = host.executeAction(batch, ["a", "b"]);
  void [reference, context, named, routeResult, hostResult];
  void tuple("a", 2);
  void ctx.action("tuple", "a", 2);
  void env.action(tuple, "a", 2);
  void modules.action("batch", ["a"]);
  void nullableBatch(["a"]);
  void nullableBatch(null);
  void untyped.action("dynamic", 1, "two");
  // @ts-expect-error Validated array input is one argument, never a variadic tuple.
  void batch("a", "b");
  // @ts-expect-error Reference calls preserve the validated array signature.
  void ctx.action(batch, "a", "b");
  // @ts-expect-error Host reference overloads must not fall back to an erased signature.
  void host.executeAction(batch, "a", "b");
  // @ts-expect-error Typed string calls require the registered input shape.
  void ctx.action("batch", "a", "b");
  // @ts-expect-error Typed route calls require the registered input shape.
  void env.action("batch", 123);
  // @ts-expect-error Unknown names cannot escape through the untyped overload.
  void ctx.action("missing", []);
  // @ts-expect-error Unknown route names cannot escape through the untyped overload.
  void env.action("missing", []);
  // @ts-expect-error Tuple argument types remain checked.
  void ctx.action("tuple", "a", "two");
  // @ts-expect-error Module inference preserves validated array input.
  void modules.action("batch", "a");
  void workflowActionStep("batch", batch, { args: () => ["a", "b"] });
  // @ts-expect-error Workflow reference input is the validated array payload.
  void workflowActionStep("batch", batch, { args: () => "a" });
}
void checkActionTypes;

test("object validators retain declared prototype-named properties as ordinary data", () => {
  const stringShape = v.object({ ["__proto__"]: v.string(), constructor: v.string() });
  const input: unknown = JSON.parse('{"__proto__":"ordinary data","constructor":"constructor data"}');
  const parsed = stringShape.parse(input);
  expect(Reflect.getPrototypeOf(parsed)).toBe(Object.prototype);
  expect(Object.hasOwn(parsed, "__proto__")).toBe(true);
  expect(parsed.__proto__).toBe("ordinary data");
  expect(JSON.stringify(parsed)).toBe(JSON.stringify(input));
  expect(() => stringShape.parse({ constructor: "missing prototype field" })).toThrow("__proto__ is required");
  const invalidInput: unknown = JSON.parse('{"__proto__":123,"constructor":"data"}');
  expect(() => stringShape.parse(invalidInput)).toThrow("must be a string");
  const objectShape = v.object({ ["__proto__"]: v.object({ label: v.string() }) });
  const nestedInput: unknown = JSON.parse('{"__proto__":{"label":"nested data"}}');
  const nested = objectShape.parse(nestedInput);
  expect(Reflect.getPrototypeOf(nested)).toBe(Object.prototype);
  expect(nested.__proto__).toEqual({ label: "nested data" });
});

test("shared action references use the current host and require an explicit host outside handlers", async () => {
  const read = action("read", async (ctx) => await ctx.kv.get("identity"));
  const seed = action("seed", async (ctx, identity: string) => await ctx.kv.set("identity", identity));
  const nested = action("nested", async (ctx) => [await read(), await ctx.action(read)]);
  const registrations = [read, seed, nested, route("read", async (_request, env) =>
    Response.json([await read(), await env.action(read)]))];
  await expect(read()).rejects.toThrow("use host.executeAction(reference, ...)");
  const first = await createChimpbase({ storage: { engine: "memory" }, registrations });
  const second = await createChimpbase({ storage: { engine: "memory" }, registrations });
  try {
    await first.executeAction(seed, "first");
    await second.executeAction(seed, "second");
    expect((await first.executeAction(read)).result).toBe("first");
    expect((await second.executeAction(read)).result).toBe("second");
    const nestedResults = await Promise.all([first.executeAction(nested), second.executeAction(nested)]);
    expect(nestedResults.map((outcome) => outcome.result)).toEqual([["first", "first"], ["second", "second"]]);
    const routed = await first.executeRoute(new Request("http://localhost/read"));
    expect(await readJsonResponse<string[]>(routed.response)).toEqual(["first", "first"]);
    await expect(read()).rejects.toThrow("requires an active chimpbase runtime context");
  } finally {
    await first.close();
    await second.close();
  }
  await expect(read()).rejects.toThrow("requires an active chimpbase runtime context");
});

test("validated array inputs work through references, routes and workflow steps", async () => {
  const caller = action("caller", async (ctx) => [await batch(["a", "b"]), await ctx.action(batch, [])]);
  const inspect = action("inspect", async (ctx) => await ctx.workflow.get("array-instance"));
  const start = action("start", async (ctx) => await ctx.workflow.start("array", {}, { workflowId: "array-instance" }));
  const host = await createChimpbase({
    storage: { engine: "memory" },
    registrations: [
      batch, nullableBatch, tuple, caller, inspect, start,
      route("batch", async (_request, env) => Response.json(await env.action(batch, ["a", "b"]))),
      workflow({
        name: "array", version: 1, initialState: () => ({ output: "" }),
        steps: [workflowActionStep<{}, { output: string }, typeof batch>("join", batch, {
          args: () => ["a", "b"], onResult: ({ result }) => ({ output: result }),
        })],
      }),
    ],
  });
  try {
    expect((await host.executeAction(batch, ["a", "b"])).result).toBe("a,b");
    expect((await host.executeAction(batch, [])).result).toBe("");
    expect((await host.executeAction(nullableBatch, null)).result).toBe("none");
    expect((await host.executeAction(tuple, "a", 2)).result).toBe("aa");
    expect((await host.executeAction(caller)).result).toEqual(["a,b", ""]);
    const routed = await host.executeRoute(new Request("http://localhost/batch"));
    expect(await readJsonResponse<string>(routed.response)).toBe("a,b");
    await host.executeAction(start);
    await host.drain();
    expect((await host.executeAction(inspect)).result).toMatchObject({ state: { output: "a,b" }, status: "completed" });
  } finally { await host.close(); }
});

test("duplicate app and plugin action names fail without replacing the existing handler", async () => {
  const original = action("createOrder", () => "original");
  const replacement = action("createOrder", () => "replacement");
  await expect(createChimpbase({ storage: { engine: "memory" }, registrations: [original, replacement] }))
    .rejects.toThrow("duplicate action registration: createOrder");
  const host = await createChimpbase({ storage: { engine: "memory" } });
  try {
    host.register(plugin({ name: "first" }, original));
    expect(() => host.register(plugin({ name: "second" }, replacement))).toThrow("duplicate action registration: createOrder");
    expect(() => host.registerAction("createOrder", () => "replacement")).toThrow("duplicate action registration: createOrder");
    expect((await host.executeAction(original)).result).toBe("original");
  } finally { await host.close(); }
});

test("validated array inputs remain one payload through module contracts and pact verification", async () => {
  const catalog = defineChimpbaseModuleInterface({
    name: "catalog", version: 1, events: {},
    calls: { join: { input: v.array(v.string()), output: v.string() } },
  });
  const caller = action("joinCatalog", async (ctx) => await ctx.call(catalog.calls.join, ["a", "b"]));
  const host = await createChimpbase({
    storage: { engine: "memory" },
    modules: [defineChimpbaseModuleImplementation({
      interface: catalog, calls: { join: (_ctx, input) => input.join(",") },
    })],
    registrations: [batch, caller],
  });
  try {
    expect((await host.executeAction(caller)).result).toBe("a,b");
    const result = await verifyPact({ host, pact: pact({
      consumer: "array-consumer", provider: "array-provider",
      interactions: [interaction.action(batch.name, {
        args: v.array(v.string()), result: v.string(), example: { args: [["a", "b"]], result: "a,b" },
      })],
    }) });
    expect(result.passed).toBe(1);
    expect(result.failed).toBe(0);
  } finally { await host.close(); }
});

test("string workflow steps preserve argument lists and omitted inputs alongside validated references", async () => {
  const calls: string[] = [];
  const createAccount = action({
    name: "createAccount", args: v.object({ email: v.string() }),
    handler: (_ctx, input) => { calls.push(input.email); },
  });
  const optional = action({
    name: "optional", args: v.optional(v.object({ email: v.string() })),
    handler: (_ctx, input) => { calls.push(input?.email ?? "missing"); },
  });
  const zero = action({ name: "zero", handler: () => { calls.push("zero"); } });
  const start = action("start", async (ctx, name: string) =>
    await ctx.workflow.start(name, {}, { workflowId: name }));
  const inspect = action("inspect", async (ctx) => await ctx.workflow.get("named"));
  const host = await createChimpbase({
    storage: { engine: "memory" },
    registrations: [createAccount, optional, zero, batch, nullableBatch, start, inspect,
      workflow<{}, { output: string }>({
        name: "named", version: 1, initialState: () => ({ output: "" }),
        steps: [
          workflowActionStep("object", createAccount.name, { args: () => [{ email: "named" }] }),
          workflowActionStep("empty-tuple", zero.name),
          workflowActionStep("explicit-empty-tuple", zero.name, { args: () => [] }),
          workflowActionStep("optional-string", optional.name),
          workflowActionStep("optional-reference", optional),
          workflowActionStep("object-reference", createAccount, { args: () => ({ email: "reference" }) }),
          workflowActionStep<{}, { output: string }, string>("array-string", batch.name, {
            args: () => [["a", "b"]], onResult: ({ result }) => ({ output: result }),
          }),
          workflowActionStep<{}, { output: string }, typeof nullableBatch>("nullable-reference", nullableBatch, {
            args: () => null, onResult: ({ state, result }) => ({ output: `${state.output}/${result}` }),
          }),
        ],
      }),
      workflow({
        name: "missing", version: 1, initialState: () => ({}),
        steps: [workflowActionStep("required-input", createAccount.name)],
      }),
    ],
  });
  try {
    await host.executeAction(start, "named");
    await host.drain();
    expect(calls).toEqual(["named", "zero", "zero", "missing", "missing", "reference"]);
    expect((await host.executeAction(inspect)).result).toMatchObject({ status: "completed", state: { output: "a,b/none" } });
    await host.executeAction(start, "missing");
    await expect(host.processNextQueueJob()).rejects.toThrow("args must be an object");
    expect(calls).toEqual(["named", "zero", "zero", "missing", "missing", "reference"]);
  } finally { await host.close(); }
});
