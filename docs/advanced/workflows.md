# Workflows

Workflows model long-running business processes that survive time, restarts, and retries. They maintain durable state, support sleeping and waiting for external signals, and can span minutes to months.

## Two Patterns

### Steps-based (declarative)

Define a sequence of typed steps:

```ts chimpbase-check:workflow-registration
import { workflow, workflowActionStep, workflowWaitForSignalStep, v } from "chimpbase";

type OnboardingInput = { email: string };
type OnboardingState = { email: string; step: string; accountId?: string };

const onboarding = workflow<OnboardingInput, OnboardingState>({
  name: "customer.onboarding",
  version: 1,
  initialState: (input) => ({ email: input.email, step: "started" }),

  steps: [
    workflowActionStep<OnboardingInput, OnboardingState>("create-account", "createAccount", {
      args: ({ input }) => [{ email: input.email }],
      onResult: ({ state, result }) => ({
        ...state,
        accountId: v.object({ id: v.string() }).parse(result).id,
        step: "account-created",
      }),
    }),

    workflowWaitForSignalStep<OnboardingInput, OnboardingState>("wait-verification", "email.verified", {
      timeoutMs: 86_400_000, // 24 hours
      onSignal: ({ state }) => ({ ...state, step: "verified" }),
      onTimeout: "fail",
    }),

    workflowActionStep<OnboardingInput, OnboardingState>("send-welcome", "sendWelcomeEmail", {
      args: ({ state }) => [{ accountId: state.accountId }],
    }),
  ],
});

export default { registrations: [onboarding] };
```

Register the workflow directly alongside the actions it calls (`createAccount` and `sendWelcomeEmail`). `workflow()` already returns a registration.

### Run-based (imperative)

Full control with an imperative `run` function:

```ts
const orderFollowUp = workflow<
  { orderId: string },
  { orderId: string; phase: "waiting" | "ready" }
>({
  name: "order.follow-up",
  version: 1,
  initialState: (input) => ({ orderId: input.orderId, phase: "waiting" }),

  async run(wf) {
    if (wf.state.phase === "waiting") {
      return wf.sleep(2 * 24 * 60 * 60 * 1000, {
        stepId: "wait-2-days",
        state: { ...wf.state, phase: "ready" },
      });
    }

    await wf.action("sendFollowUpEmail", { orderId: wf.state.orderId });
    return wf.complete(wf.state);
  },
});
```

## Step Types

### Action Step

Calls a registered action:

```ts
{
  id: "charge-card",
  kind: "workflow_action",
  action: "chargePayment",
  args: ({ state }) => [{ amount: state.total }],
  onResult: ({ state, result }) => ({ ...state, chargeId: result.id }),
}
```

### Sleep Step

Pauses the workflow for a duration:

```ts
{
  id: "cooldown",
  kind: "workflow_sleep",
  delayMs: 60_000, // 1 minute
}
```

The delay can also be dynamic:

```ts
{
  id: "backoff",
  kind: "workflow_sleep",
  delayMs: ({ state }) => state.retryCount * 5000,
}
```

### Wait For Signal Step

Pauses until an external signal arrives (or times out):

```ts
{
  id: "wait-approval",
  kind: "workflow_wait_for_signal",
  signal: "manager.approved",
  timeoutMs: 72 * 60 * 60 * 1000, // 3 days
  onSignal: ({ state, payload }) => ({ ...state, approvedBy: payload.manager }),
  onTimeout: "fail", // or "continue", or a function
}
```

## Starting Workflows

```ts
const result = await ctx.workflow.start(onboarding, { email: "user@example.com" }, {
  workflowId: "onboarding-user-42", // optional custom ID
});
// result.workflowId, result.status
```

## Sending Signals

```ts
await ctx.workflow.signal("onboarding-user-42", "email.verified", {
  verifiedAt: new Date().toISOString(),
});
```

## Querying Workflow State

```ts
const instance = await ctx.workflow.get("onboarding-user-42");
// instance.status, instance.state, instance.currentStepId
```

## Run Context Methods

In run-based workflows, the `wf` context provides:

| Method | Description |
|--------|-------------|
| `wf.action(name, ...args)` | Call an action |
| `wf.sleep(delayMs, options?)` | Pause for a duration |
| `wf.waitForSignal(signal, options?)` | Wait for an external signal |
| `wf.transition(state, options?)` | Update state and continue |
| `wf.complete(state?)` | Mark workflow as completed |
| `wf.fail(error, options?)` | Mark workflow as failed |

## Configuration

```ts
export default {
  workflows: {
    contractsDir: "./workflow-contracts", // optional, for contract sync
  },
} satisfies ChimpbaseAppDefinitionInput;
```
