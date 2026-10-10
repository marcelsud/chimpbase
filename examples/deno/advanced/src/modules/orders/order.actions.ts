import { action, v } from "@chimpbase/runtime";

import { assertTransition, normalizeAmount, normalizeCustomer } from "../../../../../shared/orders/order.domain.ts";
import {
  getOrder,
  insertOrder,
  listOrders as listOrdersQuery,
  updateOrderStatus,
} from "../../../../../shared/orders/order.repository.ts";
import type { OrderRecord } from "../../../../../shared/orders/order.types.ts";

export const createOrder = action({
  name: "createOrder",
  args: v.object({
    customer: v.string(),
    amount: v.number(),
  }),
  async handler(ctx, input) {
    const order = await insertOrder(ctx, {
      customer: normalizeCustomer(input.customer),
      amount: normalizeAmount(input.amount),
    });
    ctx.pubsub.publish("order.created", order);
    return order;
  },
});

export const listOrders = action({
  name: "listOrders",
  async handler(ctx) {
    return await listOrdersQuery(ctx);
  },
});

async function moveOrder(
  ctx: Parameters<typeof createOrder.handler>[0],
  id: number,
  target: OrderRecord["status"],
  assignee: string | null,
  event: string,
  extra: Record<string, unknown> = {},
): Promise<OrderRecord> {
  const current = await getOrder(ctx, id);
  if (!current) throw new Error(`order ${id} not found`);
  assertTransition(current.status, target);
  const next = await updateOrderStatus(ctx, id, target, assignee);
  ctx.pubsub.publish(event, { ...next, ...extra });
  return next;
}

export const assignOrder = action({
  name: "assignOrder",
  args: v.object({
    id: v.number(),
    assignee: v.string(),
  }),
  async handler(ctx, input) {
    return await moveOrder(ctx, input.id, "assigned", input.assignee.trim(), "order.assigned");
  },
});

export const startOrder = action({
  name: "startOrder",
  args: v.object({ id: v.number() }),
  async handler(ctx, input) {
    return await moveOrder(ctx, input.id, "in_progress", null, "order.started");
  },
});

export const completeOrder = action({
  name: "completeOrder",
  args: v.object({ id: v.number() }),
  async handler(ctx, input) {
    return await moveOrder(ctx, input.id, "completed", null, "order.completed");
  },
});

export const rejectOrder = action({
  name: "rejectOrder",
  args: v.object({
    id: v.number(),
    reason: v.string(),
  }),
  async handler(ctx, input) {
    return await moveOrder(ctx, input.id, "rejected", null, "order.rejected", {
      reason: input.reason,
    });
  },
});
