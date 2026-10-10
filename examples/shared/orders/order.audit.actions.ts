import { action, v } from "@chimpbase/runtime";

import { listNotifications, listOrderAudit } from "./order.repository.ts";

export const listOrderEvents = action({
  name: "listOrderEvents",
  args: v.object({ orderId: v.number() }),
  async handler(ctx, input) {
    return await listOrderAudit(ctx, input.orderId);
  },
});

export const listOrderNotifications = action({
  name: "listOrderNotifications",
  async handler(ctx) {
    return await listNotifications(ctx);
  },
});
