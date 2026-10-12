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

Register the workflow directly alongside the actions it calls (`createAccount` and `sendWelcomeEmail`). `workflow()` already returns a registration. A name/version pair must be unique within the app; register older versions alongside newer ones while their instances still need to execute.

### Run-based (imperative)

Full control with an imperative `run` function. Return the instruction from `sleep()` or `waitForSignal()`; these methods create plain values, so `await wf.sleep(...)` does not pause execution. When the workflow resumes, `run()` starts from its beginning with the saved state. Save the next phase in the instruction so completed work is skipped:

```ts chimpbase-check:workflow-run
import { workflow } from "chimpbase";

export const orderFollowUp = workflow<
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

Register `orderFollowUp` and `sendFollowUpEmail` in the app. In a signal wait, keep returning the same wait instruction for the waiting phase; change that phase in `onSignal` or a timeout callback. Immediate signals, zero delays, and `transition()` can re-enter `run()` within the same execution.

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

`onTimeout: "fail"` makes the instance terminal in both authoring styles. A late signal cannot resume it. A payload type on `workflowWaitForSignalStep<Input, State, Payload>()` or `wf.waitForSignal<Payload>()` checks the callback at compile time; validate external payloads in `onSignal` with a validator such as `v.object(...)`.

## Starting Workflows

```ts
const result = await ctx.workflow.start(onboarding, { email: "user@example.com" }, {
  workflowId: "onboarding-user-42", // optional custom ID
});
// result.workflowId, result.status
```

The workflow's name and version must be registered locally. If another host sharing the database runs it, start with an imported workflow and explicitly allow that destination:

```ts
await ctx.workflow.start(onboarding, { email: "user@example.com" }, {
  allowUnregistered: true,
});
```

That host must register the same name and version. Passing a string name still requires a local definition to choose the version and create initial state.

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

`get()` returns `null` for an unknown ID. Persisted input, state, status, and errors remain readable after the workflow's definition is removed; historical instances do not require old code just for inspection.

## Retrying Failed Workflows

After correcting the cause of a failure, retry from a handler:

```ts
const result = await ctx.workflow.retry("onboarding-user-42");
// Same workflowId and version, status: "running"
```

Retry accepts only failed instances and requires the original name/version registration. It resumes from the saved state and step with a fresh attempt budget, clears the error and deadline, and supersedes old pending wake jobs in the same transaction. It does not restart from the initial state. A retry of a timed-out signal wait starts a new wait deadline. Use this API for workflows; `host.retryJob()` rejects their internal coordination jobs.

## Transaction Checkpoints and External Effects

Both authoring styles execute all work up to a suspension or completion in one database transaction. Finishing an action step, calling `wf.action()`, or returning `wf.transition()` does not commit a checkpoint. A positive sleep or a signal wait that actually suspends commits the progress so far. Zero delays and signals already queued continue in the same transaction.

If a later action throws before that commit, earlier database writes, workflow state changes, events, and enqueues roll back. A retry repeats those earlier actions. External effects such as charging a card or sending email cannot roll back, so use the external service's supported idempotency mechanism with a stable business-operation key. Keep that key across attempts and replacement workflows for the same operation; a queue job ID, workflow ID, or timestamp is insufficient.

For example, this action asks your application's receipt delivery service to send one receipt per order:

```ts chimpbase-check:workflow-receipt
import { action } from "chimpbase";

export const sendReceipt = action("sendReceipt", async (
  ctx,
  input: { orderId: string; to: string },
) => {
  const deliveryUrl = ctx.secret("RECEIPT_DELIVERY_URL");
  if (!deliveryUrl) throw new Error("RECEIPT_DELIVERY_URL is required");

  const response = await fetch(deliveryUrl, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Idempotency-Key": `order:${input.orderId}:receipt:v1`,
    },
    body: JSON.stringify(input),
    signal: AbortSignal.timeout(5_000),
  });
  if (!response.ok) throw new Error(`receipt delivery returned ${response.status}`);

  await ctx.kv.set(`order:${input.orderId}:receipt-requested`, true);
});
```

The delivery service must atomically deduplicate that key, return the original outcome on repeats, and reject a reused key with different request data. A header alone provides no guarantee; use the documented mechanism of your actual payment or email provider. The KV write above records application progress but cannot deduplicate delivery: it rolls back if a later workflow action fails. Keep the request data stable across retries and retain deduplication records for the full recovery window.

## Run Context Methods

In run-based workflows, the `wf` context provides:

| Method | Description |
|--------|-------------|
| `wf.action(name, ...args)` | Call an action |
| `return wf.sleep(delayMs, options?)` | Save state and suspend for a positive duration |
| `return wf.waitForSignal(signal, options?)` | Save state and suspend until a signal or deadline |
| `return wf.transition(state, options?)` | Update state and re-enter `run()` in the current transaction |
| `return wf.complete(state?)` | Mark workflow as completed |
| `return wf.fail(error, options?)` | Persist terminal failure |

## Versioning

New instances use the selected registered version; existing instances keep their persisted version. Bump `version` explicitly when changing `run()`, action logic, callbacks, or values captured by functions. Keep the old definition available while its unfinished instances need to execute or retry.

`describeWorkflow()` and `versionWorkflow()` compare structural contracts: schemas, step identities, action and signal names, literal sleep durations, literal signal timeouts, and timeout policies. Changing a literal duration or policy changes the contract and is classified as breaking. Function bodies and closed-over values are not compared; dynamic durations and callbacks are described as dynamic behavior. A run-based workflow's contract does not inspect instructions returned by its `run()` function, so a behavior change still needs an explicit version bump even when `changed` is `false`.

With `workflows.contractsDir` configured, run `bunx chimpbase contracts` to save contract snapshots and `bunx chimpbase contracts --check` in CI. For Node, use `npx chimpbase-node`. Review breaking changes before using `--allow-breaking`; that flag does not allow a changed contract at the same version.

Snapshots generated before durations and timeout policies were included have different hashes even when the workflow source is unchanged. Back up and review those snapshots against the unchanged definitions, remove the reviewed snapshots, and regenerate them with all recorded versions still registered. For a regenerated history with intentional breaking changes, use `contracts --allow-breaking`, then run `contracts --check --allow-breaking`. Actual behavior edits still require a new version.

## Configuration

```ts
export default {
  workflows: {
    contractsDir: "./workflow-contracts", // optional, for contract sync
  },
} satisfies ChimpbaseAppDefinitionInput;
```
