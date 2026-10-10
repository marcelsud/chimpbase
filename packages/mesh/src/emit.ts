import type { ChimpbaseContext } from "@chimpbase/runtime";

import type { EmitOptions } from "./types.ts";

export function balancedWorkerName(event: string): string {
  return `__chimpbase.mesh.balanced.${event}`;
}

export interface BalancedEnvelope<TPayload = unknown> {
  event: string;
  payload: TPayload;
}

export async function meshEmit(
  ctx: ChimpbaseContext,
  event: string,
  payload: unknown,
  options: EmitOptions,
): Promise<void> {
  if ((options.balanced === true)) {
    const envelope: BalancedEnvelope = { event, payload };
    await ctx.enqueue(balancedWorkerName(event), envelope);
    return;
  }

  ctx.pubsub.publish(event, payload);
}
