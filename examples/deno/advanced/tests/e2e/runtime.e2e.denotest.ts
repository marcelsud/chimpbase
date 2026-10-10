import { assert, assertEquals } from "jsr:@std/assert@1";

import { authedGet, authedPost, bootAdvanced } from "../support/harness.ts";
import type { OrderRecord } from "../../../../shared/orders/order.types.ts";

async function helper(t: () => Promise<void>): Promise<void> {
  await t();
}

Deno.test("deno/advanced — lifecycle", async () => {
  const booted = await bootAdvanced();
  try {
    const { baseUrl, host } = booted;

    const created = await authedPost(`${baseUrl}/orders`, {
      customer: "Alice@Example.com",
      amount: 4200,
    });
    assertEquals(created.status, 201);
    const order = (await created.json()) as OrderRecord;
    assertEquals(order.status, "pending");

    const assignRes = await authedPost(`${baseUrl}/orders/${order.id}/assign`, { assignee: "ops-1" });
    assertEquals(assignRes.status, 200);
    await assignRes.body?.cancel();

    const startRes = await authedPost(`${baseUrl}/orders/${order.id}/start`, {});
    assertEquals(startRes.status, 200);
    await startRes.body?.cancel();

    const completeRes = await authedPost(`${baseUrl}/orders/${order.id}/complete`, {});
    assertEquals(completeRes.status, 200);
    await completeRes.body?.cancel();

    const events = await (await authedGet(`${baseUrl}/orders/${order.id}/events`)).json() as Array<
      { event: string }
    >;
    assertEquals(
      events.map((e) => e.event),
      ["order.created", "order.assigned", "order.started", "order.completed"],
    );

    await host.drain({ maxDurationMs: 5_000 });
    const notifications = await (await authedGet(`${baseUrl}/notifications`)).json() as Array<{
      order_id: number;
      status: string;
    }>;
    assert(notifications.some((n) => n.order_id === order.id && n.status === "sent"));
  } finally {
    await booted.started.stop();
  }

  await helper(async () => {});
});

Deno.test("deno/advanced — rejects unauthenticated requests", async () => {
  const booted = await bootAdvanced();
  try {
    const res = await fetch(`${booted.baseUrl}/orders`);
    assertEquals(res.status, 401);
    await res.body?.cancel();
  } finally {
    await booted.started.stop();
  }
});
