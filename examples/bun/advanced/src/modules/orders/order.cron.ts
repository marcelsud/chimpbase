import { action, v } from "@chimpbase/runtime";
import type { ChimpbaseContext } from "@chimpbase/runtime";

import { countByStatus, insertBacklogSnapshot, listBacklogSnapshots } from "../../../../../shared/orders/order.repository.ts";

export async function captureOrderBacklogSnapshot(ctx: ChimpbaseContext): Promise<void> {
  const counts = await countByStatus(ctx);
  await insertBacklogSnapshot(ctx, counts);
  ctx.metric("orders.pending", counts.pending);
  ctx.metric("orders.in_progress", counts.in_progress);
  ctx.log.info("captured order backlog snapshot", counts);
}

export const listOrderBacklogSnapshots = action({
  name: "listOrderBacklogSnapshots",
  async handler(ctx) {
    return await listBacklogSnapshots(ctx);
  },
});
