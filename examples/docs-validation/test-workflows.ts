import { createChimpbase } from "@chimpbase/bun";
import { action, workflow, v, workflowActionStep, workflowSleepStep, workflowWaitForSignalStep, describeWorkflow, compareWorkflowContracts } from "@chimpbase/runtime";
interface OrderFollowUpInput { orderId: number }
interface OrderFollowUpState { orderId: number; phase: "waiting" | "ready" }
interface OnboardingInput { email: string }
interface OnboardingState { step: string; accountId?: number; verified?: boolean }
const chimpbase = await createChimpbase({ storage: { engine: "memory" }, server: { port: 0 } });
const createAccount = action({ name: "createAccount", args: v.object({ email: v.string() }), async handler(_ctx, input) { return { id: 1, email: input.email }; } });
const sendWelcomeEmail = action({ name: "sendWelcomeEmail", args: v.object({ accountId: v.number() }), async handler(ctx, input) { ctx.log.info("welcome", { accountId: input.accountId }); return { sent: true }; } });
const sendFollowUpEmail = action({ name: "sendFollowUpEmail", args: v.object({ orderId: v.number() }), async handler(ctx, input) { ctx.log.info("follow-up", { orderId: input.orderId }); } });
const orderFollowUp = workflow<OrderFollowUpInput, OrderFollowUpState>({ name: "order.follow-up", version: 1, initialState: (input) => ({ orderId: input.orderId, phase: "waiting" }), async run(wf) { if (wf.state.phase === "waiting") { return wf.sleep(2*24*60*60*1000, { stepId: "wait-2-days", state: { ...wf.state, phase: "ready" } }); } await wf.action("sendFollowUpEmail", { orderId: wf.state.orderId }); return wf.complete(wf.state); } });
const onboardingWorkflow = workflow<OnboardingInput, OnboardingState>({ name: "customer.onboarding", version: 1, initialState: () => ({ step: "started" }), steps: [{ id: "create-account", kind: "workflow_action" as const, action: "createAccount", args: ({ input }) => [{ email: input.email }], onResult: ({ state, result }) => ({ ...state, accountId: (result as { id: number }).id, step: "account-created" }) }, { id: "wait-for-verification", kind: "workflow_wait_for_signal" as const, signal: "email.verified", timeoutMs: 86_400_000, onSignal: ({ state }) => ({ ...state, step: "verified" }), onTimeout: "fail" as const }, { id: "send-welcome", kind: "workflow_action" as const, action: "sendWelcomeEmail", args: ({ state }) => [{ accountId: state.accountId }] }] });
const onboardingWithHelpers = workflow<OnboardingInput, OnboardingState>({ name: "customer.onboarding-helpers", version: 1, initialState: () => ({ step: "started" }), steps: [workflowActionStep<OnboardingInput, OnboardingState>("create-account", "createAccount", { args: ({ input }) => [{ email: input.email }], onResult: ({ state, result }) => ({ ...state, accountId: (result as { id: number }).id }) }), workflowSleepStep("cooldown", 60_000), workflowWaitForSignalStep<OnboardingInput, OnboardingState>("wait-verification", "email.verified", { timeoutMs: 86_400_000, onSignal: ({ state }) => ({ ...state, verified: true }), onTimeout: "fail" })] });
const startOnboarding = action({ name: "startOnboarding", args: v.object({ email: v.string(), customerId: v.string() }), async handler(ctx, input) { return await ctx.workflow.start(onboardingWorkflow, { email: input.email }, { workflowId: `onboarding-${input.customerId}` }); } });
const signalWorkflow = action({ name: "signalWorkflow", args: v.object({ customerId: v.string() }), async handler(ctx, input) { await ctx.workflow.signal(`onboarding-${input.customerId}`, "email.verified", { verifiedAt: new Date().toISOString() }); return { signaled: true }; } });
const queryWorkflow = action({ name: "queryWorkflow", args: v.object({ customerId: v.string() }), async handler(ctx, input) { return await ctx.workflow.get(`onboarding-${input.customerId}`); } });
const contract = describeWorkflow(onboardingWorkflow.definition); console.log("workflows (contract):", contract.name, "mode:", contract.mode);
const contract2 = describeWorkflow(onboardingWithHelpers.definition); const compat = compareWorkflowContracts(contract, contract2); console.log("workflows (compat):", compat);
chimpbase.register({ createAccount, sendWelcomeEmail, sendFollowUpEmail, orderFollowUp, onboardingWorkflow, onboardingWithHelpers, startOnboarding, signalWorkflow, queryWorkflow });
await chimpbase.start();
const r1 = await chimpbase.executeAction("startOnboarding", { email: "test@example.com", customerId: "123" }); console.log("workflows (start):", JSON.stringify(r1.result));
await new Promise((r) => setTimeout(r, 500));
const r2 = await chimpbase.executeAction("queryWorkflow", { customerId: "123" }); console.log("workflows (query):", r2.result != null ? "found" : "not found");
const r3 = await chimpbase.executeAction("signalWorkflow", { customerId: "123" }); console.log("workflows (signal):", JSON.stringify(r3.result));
await new Promise((r) => setTimeout(r, 1000)); console.log("workflows: OK"); chimpbase.close(); process.exit(0);
