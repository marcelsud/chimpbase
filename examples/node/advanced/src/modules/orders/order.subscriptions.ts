import type { ChimpbaseContext } from "@chimpbase/runtime";

import { insertAuditEntry } from "../../../../../shared/orders/order.repository.ts";
import type { OrderRecord } from "../../../../../shared/orders/order.types.ts";

export async function auditOrderCreated(ctx: ChimpbaseContext, order: OrderRecord): Promise<void> {
  await insertAuditEntry(ctx, { orderId: order.id, event: "order.created", payload: order });
}

export async function auditOrderAssigned(ctx: ChimpbaseContext, order: OrderRecord): Promise<void> {
  await insertAuditEntry(ctx, { orderId: order.id, event: "order.assigned", payload: order });
}

export async function auditOrderStarted(ctx: ChimpbaseContext, order: OrderRecord): Promise<void> {
  await insertAuditEntry(ctx, { orderId: order.id, event: "order.started", payload: order });
}

export async function auditOrderCompleted(ctx: ChimpbaseContext, order: OrderRecord): Promise<void> {
  await insertAuditEntry(ctx, { orderId: order.id, event: "order.completed", payload: order });
}

export async function auditOrderRejected(
  ctx: ChimpbaseContext,
  order: OrderRecord & { reason: string },
): Promise<void> {
  await insertAuditEntry(ctx, { orderId: order.id, event: "order.rejected", payload: order });
}

export async function enqueueOrderCompletedNotification(
  ctx: ChimpbaseContext,
  order: OrderRecord,
): Promise<void> {
  await ctx.enqueue("order.completed.notify", {
    orderId: order.id,
    customer: order.customer,
    amount: order.amount,
  });
}
