import type { ChimpbaseContext, ChimpbaseDlqEnvelope } from "@chimpbase/runtime";

import { insertNotification } from "./order.repository.ts";

export interface OrderCompletedNotificationPayload {
  orderId: number;
  customer: string;
  amount: number;
}

export async function notifyOrderCompleted(
  ctx: ChimpbaseContext,
  payload: OrderCompletedNotificationPayload,
): Promise<void> {
  await ctx.trace("order.notify", async (span) => {
    span.setAttribute("order.id", payload.orderId);
    span.setAttribute("order.customer", payload.customer);

    ctx.log.info("delivering order completion notice", {
      orderId: payload.orderId,
      customer: payload.customer,
    });
    ctx.metric("order.notifications.delivered", 1);

    await insertNotification(ctx, {
      orderId: payload.orderId,
      channel: "email",
      status: "sent",
      detail: `order ${payload.orderId} for ${payload.customer} settled at ${payload.amount} cents`,
    });
  });
}

export async function captureOrderCompletedDlq(
  ctx: ChimpbaseContext,
  envelope: ChimpbaseDlqEnvelope<OrderCompletedNotificationPayload>,
): Promise<void> {
  ctx.log.warn("order completion notify moved to DLQ", {
    orderId: envelope.payload.orderId,
    attempts: envelope.attempts,
    error: envelope.error,
  });
  await insertNotification(ctx, {
    orderId: envelope.payload.orderId,
    channel: "email",
    status: "failed",
    detail: envelope.error ?? "unknown error",
  });
}
