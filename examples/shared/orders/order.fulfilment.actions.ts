import { action, v } from "@chimpbase/runtime";

import { orderFulfilmentWorkflow } from "./order.workflow.ts";

export const startOrderFulfilment = action({
  name: "startOrderFulfilment",
  args: v.object({
    orderId: v.number(),
    assignee: v.string(),
  }),
  async handler(ctx, input) {
    return await ctx.workflow.start(orderFulfilmentWorkflow, input);
  },
});

export const getFulfilmentStatus = action({
  name: "getFulfilmentStatus",
  args: v.object({ workflowId: v.string() }),
  async handler(ctx, input) {
    const instance = await ctx.workflow.get(input.workflowId);
    if (!(instance !== null)) throw new Error(`workflow ${input.workflowId} not found`);
    return instance;
  },
});

export const signalQualityDecision = action({
  name: "signalQualityDecision",
  args: v.object({
    workflowId: v.string(),
    approved: v.boolean(),
    reason: v.optional(v.string()),
  }),
  async handler(ctx, input) {
    await ctx.workflow.signal(input.workflowId, "quality.decision", {
      approved: input.approved,
      reason: input.reason,
    });
    return { ok: true };
  },
});
