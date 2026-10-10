import { afterEach, beforeEach, describe, expect, test } from "bun:test";

import { authedGet, authedPost, bootAdvanced } from "../support/harness.ts";
import type { OrderRecord } from "../../../../shared/orders/order.types.ts";

describe("bun/advanced example — lifecycle", () => {
  let booted: Awaited<ReturnType<typeof bootAdvanced>>;

  beforeEach(async () => {
    booted = await bootAdvanced();
  });

  afterEach(async () => {
    await booted.started.stop();
  });

  test("routes an order through the full lifecycle", async () => {
    const { baseUrl, host } = booted;

    const created = await authedPost(`${baseUrl}/orders`, {
      customer: "Alice@Example.com",
      amount: 4200,
    });
    expect(created.status).toBe(201);
    const order = (await created.json()) as OrderRecord;
    expect(order.status).toBe("pending");

    expect((await authedPost(`${baseUrl}/orders/${order.id}/assign`, { assignee: "ops-1" })).status).toBe(200);
    expect((await authedPost(`${baseUrl}/orders/${order.id}/start`, {})).status).toBe(200);
    expect((await authedPost(`${baseUrl}/orders/${order.id}/complete`, {})).status).toBe(200);

    const eventsRes = await authedGet(`${baseUrl}/orders/${order.id}/events`);
    const events = (await eventsRes.json()) as Array<{ event: string }>;
    expect(events.map((e) => e.event)).toEqual([
      "order.created",
      "order.assigned",
      "order.started",
      "order.completed",
    ]);

    await host.drain({ maxDurationMs: 5_000 });
    const notifications = (await (await authedGet(`${baseUrl}/notifications`)).json()) as Array<{
      order_id: number;
      status: string;
    }>;
    expect(notifications.some((n) => n.order_id === order.id && n.status === "sent")).toBe(true);
  });

  test("rejects unauthenticated requests", async () => {
    const res = await fetch(`${booted.baseUrl}/orders`);
    expect(res.status).toBe(401);
  });
});
