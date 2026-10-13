import { describe, expect, test } from "bun:test";

import {
  compareWorkflowContracts,
  describeWorkflow,
  versionWorkflow,
  workflowActionStep,
  workflowSleepStep,
  workflowWaitForSignalStep,
} from "../packages/runtime/index.ts";

const onboardingInputSchema = {
  properties: {
    customerId: { type: "string" },
  },
  required: ["customerId"],
  type: "object",
} as const;

const onboardingStateSchema = {
  properties: {
    customerId: { type: "string" },
    kickoffCompletedAt: { type: "string" },
  },
  required: ["customerId"],
  type: "object",
} as const;

function createWorkflowDraft() {
  return {
    initialState(input: { customerId: string }) {
      return {
        customerId: input.customerId,
        kickoffCompletedAt: null as string | null,
      };
    },
    inputSchema: onboardingInputSchema,
    name: "customer.onboarding",
    signalSchemas: {
      "kickoff.completed": {
        properties: {
          completedAt: { type: "string" },
        },
        required: ["completedAt"],
        type: "object",
      },
    },
    stateSchema: onboardingStateSchema,
    steps: [
      workflowActionStep("create-account", "createCustomerRecord"),
      workflowSleepStep("wait-7-days", 7 * 24 * 60 * 60 * 1000),
      workflowWaitForSignalStep("wait-kickoff", "kickoff.completed", {
        timeoutMs: 14 * 24 * 60 * 60 * 1000,
      }),
      workflowActionStep("activate-account", "activateCustomer"),
    ],
  };
}

describe("workflow contracts", () => {
  test("assigns version 1 for the first generated contract and reuses it when unchanged", () => {
    const first = versionWorkflow(createWorkflowDraft());
    expect(first.definition.version).toBe(1);
    expect(first.compatibility).toBe("additive");
    expect(first.changed).toBe(true);
    expect(first.contract.hash).toBe("7690bd81f283d424");
    expect(describeWorkflow(first.definition)).toEqual(first.contract);
    expect(first.contract.steps).toEqual([
      { action: "createCustomerRecord", id: "create-account", kind: "workflow_action" },
      { delayMs: 604_800_000, id: "wait-7-days", kind: "workflow_sleep" },
      { id: "wait-kickoff", kind: "workflow_wait_for_signal", onTimeout: "continue", signal: "kickoff.completed", timeoutMs: 1_209_600_000 },
      { action: "activateCustomer", id: "activate-account", kind: "workflow_action" },
    ]);

    const second = versionWorkflow(createWorkflowDraft(), first.contract);
    expect(second.definition.version).toBe(1);
    expect(second.compatibility).toBe("identical");
    expect(second.changed).toBe(false);
    expect(second.contract.hash).toBe(first.contract.hash);
  });

  test("keeps workflow hashes stable when object keys are reordered", () => {
    const first = versionWorkflow(createWorkflowDraft());
    const reordered = versionWorkflow({
      ...createWorkflowDraft(),
      inputSchema: {
        type: "object",
        required: ["customerId"],
        properties: {
          customerId: { type: "string" },
        },
      },
      signalSchemas: {
        "kickoff.completed": {
          type: "object",
          required: ["completedAt"],
          properties: {
            completedAt: { type: "string" },
          },
        },
      },
      stateSchema: {
        type: "object",
        required: ["customerId"],
        properties: {
          kickoffCompletedAt: { type: "string" },
          customerId: { type: "string" },
        },
      },
    });

    expect(reordered.contract.hash).toBe(first.contract.hash);
  });

  test("versions literal timing, dynamic timing and timeout policy changes", () => {
    const first = versionWorkflow(createWorkflowDraft());
    const steps = createWorkflowDraft().steps;
    const changes = [
      [steps[0]!, workflowSleepStep("wait-7-days", 86_400_000), ...steps.slice(2)],
      [steps[0]!, workflowSleepStep("wait-7-days", () => 604_800_000), ...steps.slice(2)],
      [...steps.slice(0, 2), workflowWaitForSignalStep("wait-kickoff", "kickoff.completed", { timeoutMs: 86_400_000 }), steps[3]!],
      [...steps.slice(0, 2), workflowWaitForSignalStep("wait-kickoff", "kickoff.completed"), steps[3]!],
      [...steps.slice(0, 2), workflowWaitForSignalStep("wait-kickoff", "kickoff.completed", { timeoutMs: () => 1_209_600_000 }), steps[3]!],
      [...steps.slice(0, 2), workflowWaitForSignalStep("wait-kickoff", "kickoff.completed", { timeoutMs: 1_209_600_000, onTimeout: "fail" }), steps[3]!],
      [...steps.slice(0, 2), workflowWaitForSignalStep("wait-kickoff", "kickoff.completed", {
        timeoutMs: 1_209_600_000, onTimeout: ({ state }) => state,
      }), steps[3]!],
    ];

    for (const changedSteps of changes) {
      const next = versionWorkflow({ ...createWorkflowDraft(), steps: changedSteps }, first.contract);
      expect(next.changed).toBe(true);
      expect(next.definition.version).toBe(2);
      expect(next.compatibility).toBe("breaking");
      expect(next.contract.hash).not.toBe(first.contract.hash);
    }
  });

  test("normalizes default timeout policy and leaves function bodies outside the contract", () => {
    const steps = createWorkflowDraft().steps;
    const first = versionWorkflow(createWorkflowDraft());
    const explicitDefault = versionWorkflow({
      ...createWorkflowDraft(),
      steps: [...steps.slice(0, 2), workflowWaitForSignalStep("wait-kickoff", "kickoff.completed", {
        timeoutMs: 1_209_600_000, onTimeout: "continue",
      }), steps[3]!],
    }, first.contract);
    expect(explicitDefault.changed).toBe(false);

    const dynamic = versionWorkflow({
      ...createWorkflowDraft(),
      steps: [workflowSleepStep("wait", () => 1_000)],
    });
    const changedFunction = versionWorkflow({
      ...createWorkflowDraft(),
      steps: [workflowSleepStep("wait", () => 86_400_000)],
    }, dynamic.contract);
    expect(dynamic.contract.steps[0]?.delayMs).toBe("dynamic");
    expect(changedFunction.changed).toBe(false);

    const imperative = versionWorkflow({
      initialState: () => ({}), name: "imperative", run: (wf) => wf.complete(),
    });
    const changedRun = versionWorkflow({
      initialState: () => ({}), name: "imperative", run: (wf) => wf.fail("changed behavior"),
    }, imperative.contract);
    expect(changedRun.changed).toBe(false);
  });

  test("does not silently reuse a legacy snapshot lacking timing metadata", () => {
    const legacy = versionWorkflow(createWorkflowDraft()).contract;
    legacy.hash = "f576e7d63f39541b";
    legacy.steps = legacy.steps.map(({ delayMs, timeoutMs, onTimeout, ...step }) => step);
    const enriched = versionWorkflow(createWorkflowDraft(), legacy);
    expect(enriched.changed).toBe(true);
    expect(enriched.definition.version).toBe(2);
    expect(enriched.compatibility).toBe("breaking");
  });

  test("bumps version for additive changes", () => {
    const first = versionWorkflow(createWorkflowDraft());
    const second = versionWorkflow(
      {
        ...createWorkflowDraft(),
        stateSchema: {
          ...onboardingStateSchema,
          properties: {
            ...onboardingStateSchema.properties,
            activationRate30d: { type: "number" },
          },
        },
        steps: [
          ...createWorkflowDraft().steps,
          workflowActionStep("evaluate-30-day-adoption", "evaluateActivation"),
        ],
      },
      first.contract,
    );

    expect(second.definition.version).toBe(2);
    expect(second.compatibility).toBe("additive");
    expect(second.changed).toBe(true);
  });

  test("flags breaking schema changes and inserted middle steps", () => {
    const first = versionWorkflow(createWorkflowDraft());

    const breaking = versionWorkflow(
      {
        ...createWorkflowDraft(),
        stateSchema: {
          ...onboardingStateSchema,
          properties: {
            ...onboardingStateSchema.properties,
            ownerEmail: { type: "string" },
          },
          required: ["customerId", "ownerEmail"],
        },
      },
      first.contract,
    );

    expect(breaking.definition.version).toBe(2);
    expect(breaking.compatibility).toBe("breaking");

    const requiresMigration = versionWorkflow(
      {
        ...createWorkflowDraft(),
        steps: [
          workflowActionStep("create-account", "createCustomerRecord"),
          workflowActionStep("send-welcome-email", "sendWelcomeEmail"),
          workflowSleepStep("wait-7-days", 7 * 24 * 60 * 60 * 1000),
          workflowWaitForSignalStep("wait-kickoff", "kickoff.completed", {
            timeoutMs: 14 * 24 * 60 * 60 * 1000,
          }),
          workflowActionStep("activate-account", "activateCustomer"),
        ],
      },
      first.contract,
    );

    expect(requiresMigration.compatibility).toBe("requires_migration");
    expect(
      compareWorkflowContracts(first.contract, requiresMigration.contract),
    ).toBe("requires_migration");
  });

  test("rejects changed object and array constraints instead of calling them identical", () => {
    const cases: readonly [unknown, unknown][] = [
      [{ type: "object", properties: { name: { type: "string" } } }, { type: "object", properties: { name: { type: "string" } }, additionalProperties: false }],
      [{ type: "object", properties: {} }, { type: "object", properties: {}, minProperties: 1 }],
      [{ type: "object", properties: {} }, { type: "object", properties: {}, dependentRequired: { name: ["email"] } }],
      [{ type: "object", properties: {} }, { type: "object", properties: {}, required: ["email"] }],
      [{ type: "object", properties: {} }, { type: "object", properties: {}, enum: [{}] }],
      [{ type: "array", items: { type: "string" } }, { type: "array", items: { type: "string" }, enum: [[]] }],
      [{ type: "array", items: { type: "string" } }, { type: "array", items: { type: "string" }, maxItems: 1 }],
      [{ type: "array", items: { type: "string" } }, { type: "array", items: { type: "string" }, uniqueItems: true }],
      [{ type: "array", items: { type: "string" }, maxItems: 2 }, { type: "array", items: { type: "string" }, maxItems: 1 }],
      [{ type: "object", properties: { name: { type: "array", items: { type: "string" } } } }, { type: "object", properties: { name: { type: "array", items: { type: "string" }, maxItems: 1 } } }],
    ];
    for (const [before, after] of cases) {
      const draft = { name: "constraints", stateSchema: before, initialState: () => ({}), run: () => ({ kind: "workflow_complete" as const, state: {} }) };
      const first = versionWorkflow(draft);
      const next = versionWorkflow({ ...draft, stateSchema: after }, first.contract);
      expect(next.changed).toBe(true);
      expect(next.definition.version).toBe(2);
      expect(next.compatibility).toBe("breaking");
    }
  });

  test("keeps optional properties and required-property relaxation additive", () => {
    const draft = { name: "constraints", stateSchema: { type: "object", properties: { name: { type: "string" } }, required: ["name"] }, initialState: () => ({}), run: () => ({ kind: "workflow_complete" as const, state: {} }) };
    const first = versionWorkflow(draft);
    const next = versionWorkflow({ ...draft, stateSchema: { type: "object", properties: { name: { type: "string" }, email: { type: "string" } }, required: [] } }, first.contract);
    expect(next.compatibility).toBe("additive");
  });

  test("supports imperative workflow definitions without step arrays", () => {
    const first = versionWorkflow({
      initialState(input: { customerId: string }): { customerId: string; phase: "done" | "provision" } {
        return {
          customerId: input.customerId,
          phase: "provision" as const,
        };
      },
      inputSchema: onboardingInputSchema,
      name: "customer.lifecycle",
      stateSchema: {
        properties: {
          customerId: { type: "string" },
          phase: { enum: ["provision", "done"] },
        },
        required: ["customerId", "phase"],
        type: "object",
      },
      run(wf) {
        switch (wf.state.phase) {
          case "provision":
            return wf.transition({
              customerId: wf.state.customerId,
              phase: "done",
            });
          default:
            return wf.complete(wf.state);
        }
      },
    });

    expect(first.contract.mode).toBe("run");
    expect(first.contract.steps).toEqual([]);

    const second = versionWorkflow({
      ...first.definition,
      stateSchema: {
        properties: {
          customerId: { type: "string" },
          phase: { enum: ["provision", "done"] },
          syncedAt: { type: "string" },
        },
        required: ["customerId", "phase"],
        type: "object",
      },
    }, first.contract);

    expect(second.definition.version).toBe(2);
    expect(second.compatibility).toBe("additive");
  });
});
