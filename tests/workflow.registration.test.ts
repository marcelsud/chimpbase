import { expect, test } from "bun:test";

import { createChimpbase } from "../packages/bun/src/library.ts";
import { chimpbaseModuleResourceName, defineChimpbaseModuleImplementation, defineChimpbaseModuleInterface } from "../packages/core/index.ts";
import {
  action,
  plugin,
  workflow,
  workflowWaitForSignalStep,
  type ChimpbaseWorkflowRunContext,
} from "../packages/runtime/index.ts";

type Input = { customerId: string };
type State = { completed: boolean; note: string };
type ReadyPayload = { note: string };

const stepsWorkflow = workflow<Input, State>({
  initialState: () => ({ completed: false, note: "" }),
  name: "typed.steps",
  version: 1,
  steps: [workflowWaitForSignalStep<Input, State, ReadyPayload>("ready", "ready", {
    onSignal: ({ payload }) => ({ completed: true, note: payload.note }),
  })],
});

const runWorkflow = workflow<Input, State>({
  initialState: () => ({ completed: false, note: "" }),
  name: "typed.run",
  version: 1,
  run(wf) {
    if (wf.state.completed) return wf.complete();
    return wf.waitForSignal<ReadyPayload>("ready", {
      onSignal: ({ payload }) => ({ completed: true, note: payload.note }),
    });
  },
});

// Checked by `bun run typecheck`; invalid callbacks must never execute.
function checkSignalTypes(wf: ChimpbaseWorkflowRunContext<Input, State>) {
  workflowWaitForSignalStep<Input, State, ReadyPayload>("ready", "ready", {
    onSignal({ payload, state }) {
      const note: string = payload.note;
      // @ts-expect-error Signal payload types are preserved within a callback.
      const invalid: number = payload.note;
      void invalid;
      return { ...state, note };
    },
  });
  workflowWaitForSignalStep<Input, State, ReadyPayload>("ready", "ready", {
    // @ts-expect-error Step callbacks must return the declared workflow state.
    onSignal: () => ({ completed: "yes", note: "" }),
  });
  wf.waitForSignal<ReadyPayload>("ready", {
    // @ts-expect-error Run callbacks must return the declared workflow state.
    onSignal: () => ({ completed: true, note: 42 }),
  });
  wf.waitForSignal<ReadyPayload>("ready", {
    onSignal({ payload, state }) {
      // @ts-expect-error Run payload callbacks preserve declared field types.
      const invalid: number = payload.note;
      void invalid;
      return { ...state, note: payload.note };
    },
  });
}
void checkSignalTypes;

for (const definition of [stepsWorkflow, runWorkflow]) {
  test(`typed signal payloads update state in ${definition.definition.name}`, async () => {
    const launch = action("launch", async (ctx) =>
      await ctx.workflow.start(definition, { customerId: "customer-1" }, { workflowId: "flow" }));
    const signal = action("signal", async (ctx) =>
      await ctx.workflow.signal("flow", "ready", { note: "approved" }));
    const inspect = action("inspect", async (ctx) => await ctx.workflow.get("flow"));
    const host = await createChimpbase({
      storage: { engine: "memory" },
      registrations: [definition, launch, signal, inspect],
    });
    try {
      await host.executeAction(launch);
      await host.drain();
      expect((await host.executeAction(inspect)).result).toMatchObject({ status: "waiting_signal" });
      await host.executeAction(signal);
      await host.drain();
      expect((await host.executeAction(inspect)).result).toMatchObject({
        state: { completed: true, note: "approved" }, status: "completed",
      });
    } finally { await host.close(); }
  });
}

test("duplicate workflow identities fail in app, plugin and direct registration without replacement", async () => {
  const original = workflow({
    initialState: () => ({ source: "original" }), name: "orders", version: 1,
    run: (wf) => wf.complete(),
  });
  const replacement = workflow({
    initialState: () => ({ source: "replacement" }), name: "orders", version: 1,
    run: (wf) => wf.complete(),
  });
  const nextVersion = workflow({ ...replacement.definition, version: 2 });
  await expect(createChimpbase({ storage: { engine: "memory" }, registrations: [original, replacement] }))
    .rejects.toThrow("duplicate workflow registration: orders v1");
  const launch = action("launch", async (ctx) =>
    await ctx.workflow.start(original, {}, { workflowId: "original" }));
  const inspect = action("inspect", async (ctx) => await ctx.workflow.get("original"));
  const host = await createChimpbase({ storage: { engine: "memory" }, registrations: [launch, inspect] });
  try {
    host.register(plugin({ name: "first" }, original));
    expect(() => host.register(plugin({ name: "second" }, replacement)))
      .toThrow("duplicate workflow registration: orders v1");
    expect(() => host.registerWorkflow(replacement.definition))
      .toThrow("duplicate workflow registration: orders v1");
    host.register(nextVersion);
    expect(host.registry.workflows.get("orders")?.size).toBe(2);
    await host.executeAction(launch);
    await host.drain();
    expect((await host.executeAction(inspect)).result).toMatchObject({
      state: { source: "original" }, status: "completed", workflowVersion: 1,
    });
  } finally { await host.close(); }
});

test("module workflow versions coexist while duplicate and app-global registrations fail", async () => {
  const moduleInterface = defineChimpbaseModuleInterface({ name: "orders", version: 1, calls: {}, events: {} });
  const first = workflow({
    initialState: () => ({}), name: "fulfill", version: 1, run: (wf) => wf.complete(),
  });
  const second = workflow({ ...first.definition, version: 2 });
  const implementation = defineChimpbaseModuleImplementation({
    interface: moduleInterface, calls: {}, registrations: [first, second],
  });
  const registeredName = chimpbaseModuleResourceName("orders", "workflow", "fulfill");
  const host = await createChimpbase({ storage: { engine: "memory" }, modules: [implementation] });
  try {
    expect([...host.registry.workflows.get(registeredName)!.keys()]).toEqual([1, 2]);
    expect(host.registry.workflowOwnership.get(registeredName)).toBe("orders");
    expect(() => host.registerWorkflow({ ...first.definition, name: registeredName }))
      .toThrow(`workflow ${registeredName} is owned by module orders and cannot be replaced`);
    expect(() => host.registerWorkflow({ ...first.definition, name: registeredName, version: 3 }))
      .toThrow(`workflow ${registeredName} is owned by module orders and cannot be replaced`);
    expect([...host.registry.workflows.get(registeredName)!.keys()]).toEqual([1, 2]);
  } finally { await host.close(); }
  await expect(createChimpbase({
    storage: { engine: "memory" },
    modules: [defineChimpbaseModuleImplementation({
      interface: moduleInterface, calls: {}, registrations: [first, first],
    })],
  })).rejects.toThrow(`duplicate workflow registration: ${registeredName} v1`);
  await expect(createChimpbase({
    storage: { engine: "memory" },
    registrations: [workflow({ ...first.definition, name: registeredName, version: 3 })],
    modules: [implementation],
  })).rejects.toThrow(`module orders cannot register workflow ${registeredName}; already owned by app-global infrastructure`);
});
