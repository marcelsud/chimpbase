import { v, workflow } from "@chimpbase/runtime";

import { assignOrder, completeOrder, rejectOrder, startOrder } from "./order.actions.ts";

export interface OrderFulfilmentInput {
  orderId: number;
  assignee: string;
}

export interface OrderFulfilmentState {
  orderId: number;
  assignee: string;
  phase:
    | "starting"
    | "awaiting-quality"
    | "completing"
    | "rejecting"
    | "done";
  qualityApproved: boolean | null;
  rejectionReason: string | null;
  lastStepAt: string;
}

const qualityDecisionValidator = v.object({
  approved: v.boolean(),
  reason: v.string().optional(),
});

const QUALITY_SIGNAL = "quality.decision";

export const orderFulfilmentWorkflow = workflow<OrderFulfilmentInput, OrderFulfilmentState>({
  name: "order.fulfilment",
  version: 1,
  initialState(input) {
    return {
      orderId: input.orderId,
      assignee: input.assignee,
      phase: "starting",
      qualityApproved: null,
      rejectionReason: null,
      lastStepAt: new Date().toISOString(),
    };
  },
  async run(ctx) {
    const state = ctx.state;

    switch (state.phase) {
      case "starting": {
        await ctx.action(assignOrder, { id: state.orderId, assignee: state.assignee });
        await ctx.action(startOrder, { id: state.orderId });
        return ctx.waitForSignal(QUALITY_SIGNAL, {
          state: { ...state, phase: "awaiting-quality", lastStepAt: new Date().toISOString() },
          timeoutMs: 5 * 60_000,
          onTimeout: "fail",
          onSignal: ({ state: pending, payload }) => {
            const decision = qualityDecisionValidator.parse(payload, QUALITY_SIGNAL);
            return {
              ...pending,
              qualityApproved: decision.approved,
              rejectionReason: decision.approved ? null : decision.reason ?? "quality rejection",
              phase: decision.approved ? "completing" : "rejecting",
              lastStepAt: new Date().toISOString(),
            };
          },
        });
      }

      case "awaiting-quality": {
        return ctx.waitForSignal(QUALITY_SIGNAL, {
          state,
          timeoutMs: 5 * 60_000,
          onTimeout: "fail",
          onSignal: ({ state: pending, payload }) => {
            const decision = qualityDecisionValidator.parse(payload, QUALITY_SIGNAL);
            return {
              ...pending,
              qualityApproved: decision.approved,
              rejectionReason: decision.approved ? null : decision.reason ?? "quality rejection",
              phase: decision.approved ? "completing" : "rejecting",
              lastStepAt: new Date().toISOString(),
            };
          },
        });
      }

      case "completing": {
        await ctx.action(completeOrder, { id: state.orderId });
        return ctx.complete({ ...state, phase: "done", lastStepAt: new Date().toISOString() });
      }

      case "rejecting": {
        await ctx.action(rejectOrder, {
          id: state.orderId,
          reason: state.rejectionReason ?? "quality rejection",
        });
        return ctx.complete({ ...state, phase: "done", lastStepAt: new Date().toISOString() });
      }

      case "done":
        return ctx.complete(state);
    }
  },
});
