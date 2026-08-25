import { afterEach, beforeEach, describe, expect, test } from "bun:test";

import { createChimpbase } from "@chimpbase/bun";

import app from "../../chimpbase.app.ts";
import type { OrderRecord } from "../../src/modules/orders/order.types.ts";

type StartedHost = Awaited<ReturnType<Awaited<ReturnType<typeof createChimpbase>>["start"]>>;

describe("bun/intermediate example", () => {
  let started: StartedHost;
  let baseUrl: string;
  let host: Awaited<ReturnType<typeof createChimpbase>>;

  beforeEach(async () => {
    host = await createChimpbase({
      ...app,
      storage: { engine: "memory" },
      server: { port: 0 },
      subscriptions: { dispatch: "sync" },
    });
    started = await host.start();
    const port = started.server?.port;
    if (!(port !== undefined && port > 0)) throw new Error("server failed to bind a port");
    baseUrl = `http://127.0.0.1:${port}`;
  });

  afterEach(async () => {
    await started.stop();
  });

  test("routes an order through the full lifecycle", async () => {
    const created = await postJson(`${baseUrl}/orders`, {
      customer: "Alice@Example.com",
      amount: 4200,
    });
    expect(created.status).toBe(201);
    const order = (await created.json()) as OrderRecord;
    expect(order.status).toBe("pending");
    expect(order.customer).toBe("alice@example.com");
    expect(order.amount).toBe(4200);

    const assigned = await postJson(`${baseUrl}/orders/${order.id}/assign`, {
      assignee: "ops-1",
    });
    expect(assigned.status).toBe(200);
    expect(((await assigned.json()) as OrderRecord).status).toBe("assigned");

    const started200 = await postJson(`${baseUrl}/orders/${order.id}/start`, {});
    expect(((await started200.json()) as OrderRecord).status).toBe("in_progress");

    const completed = await postJson(`${baseUrl}/orders/${order.id}/complete`, {});
    expect(((await completed.json()) as OrderRecord).status).toBe("completed");

    const eventsRes = await fetch(`${baseUrl}/orders/${order.id}/events`);
    const events = (await eventsRes.json()) as Array<{ event: string }>;
    expect(events.map((e) => e.event)).toEqual([
      "order.created",
      "order.assigned",
      "order.started",
      "order.completed",
    ]);

    await host.drain({ maxDurationMs: 5_000 });

    const notificationsRes = await fetch(`${baseUrl}/notifications`);
    const notifications = (await notificationsRes.json()) as Array<{
      order_id: number;
      status: string;
    }>;
    expect(notifications.some((n) => n.order_id === order.id && n.status === "sent")).toBe(true);
  });

  test("rejects illegal transitions", async () => {
    const created = await postJson(`${baseUrl}/orders`, {
      customer: "bob@example.com",
      amount: 1000,
    });
    const order = (await created.json()) as OrderRecord;

    const illegal = await postJson(`${baseUrl}/orders/${order.id}/complete`, {});
    expect(illegal.status).toBeGreaterThanOrEqual(500);
  });
});

async function postJson(url: string, body: unknown): Promise<Response> {
  return await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
}
