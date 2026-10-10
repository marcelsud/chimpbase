import { after, before, describe, test } from "node:test";
import { strict as assert } from "node:assert";

import { authedGet, authedPost, bootAdvanced } from "../support/harness.ts";
import type { OrderRecord } from "../../../../shared/orders/order.types.ts";

describe("node/advanced example — lifecycle", () => {
  let booted: Awaited<ReturnType<typeof bootAdvanced>>;

  before(async () => {
    booted = await bootAdvanced();
  });

  after(async () => {
    await booted.started.stop();
  });

  test("routes an order through the full lifecycle", async () => {
    const { baseUrl, host } = booted;

    const created = await authedPost(`${baseUrl}/orders`, {
      customer: "Alice@Example.com",
      amount: 4200,
    });
    assert.equal(created.status, 201);
    const order = (await created.json()) as OrderRecord;
    assert.equal(order.status, "pending");

    assert.equal(
      (await authedPost(`${baseUrl}/orders/${order.id}/assign`, { assignee: "ops-1" })).status,
      200,
    );
    assert.equal((await authedPost(`${baseUrl}/orders/${order.id}/start`, {})).status, 200);
    assert.equal((await authedPost(`${baseUrl}/orders/${order.id}/complete`, {})).status, 200);

    const eventsRes = await authedGet(`${baseUrl}/orders/${order.id}/events`);
    const events = (await eventsRes.json()) as Array<{ event: string }>;
    assert.deepEqual(
      events.map((e) => e.event),
      ["order.created", "order.assigned", "order.started", "order.completed"],
    );

    await host.drain({ maxDurationMs: 5_000 });
    const notifications = (await (await authedGet(`${baseUrl}/notifications`)).json()) as Array<{
      order_id: number;
      status: string;
    }>;
    assert.ok(notifications.some((n) => n.order_id === order.id && n.status === "sent"));
  });

  test("rejects unauthenticated requests", async () => {
    const res = await fetch(`${booted.baseUrl}/orders`);
    assert.equal(res.status, 401);
  });
});
