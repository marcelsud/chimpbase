import { assert, assertEquals } from "jsr:@std/assert@1";

import { createChimpbase } from "@chimpbase/deno";

import app from "../../chimpbase.app.ts";
import type { OrderRecord } from "../../../../shared/orders/order.types.ts";

async function reservePort(): Promise<number> {
  const listener = Deno.listen({ port: 0, hostname: "127.0.0.1" });
  const { port } = listener.addr as Deno.NetAddr;
  listener.close();
  return port;
}

async function postJson(url: string, body: unknown): Promise<Response> {
  return await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
}

Deno.test("deno/intermediate — routes an order through the full lifecycle", async () => {
  const port = await reservePort();
  const host = await createChimpbase({
    ...app,
    storage: { engine: "memory" },
    server: { port },
    subscriptions: { dispatch: "sync" },
  });
  const started = await host.start();
  const baseUrl = `http://127.0.0.1:${port}`;

  try {
    const created = await postJson(`${baseUrl}/orders`, {
      customer: "Alice@Example.com",
      amount: 4200,
    });
    assertEquals(created.status, 201);
    const order = (await created.json()) as OrderRecord;
    assertEquals(order.status, "pending");
    assertEquals(order.customer, "alice@example.com");
    assertEquals(order.amount, 4200);

    const assigned = await postJson(`${baseUrl}/orders/${order.id}/assign`, {
      assignee: "ops-1",
    });
    assertEquals(assigned.status, 200);
    assertEquals(((await assigned.json()) as OrderRecord).status, "assigned");

    const startedRes = await postJson(`${baseUrl}/orders/${order.id}/start`, {});
    assertEquals(((await startedRes.json()) as OrderRecord).status, "in_progress");

    const completed = await postJson(`${baseUrl}/orders/${order.id}/complete`, {});
    assertEquals(((await completed.json()) as OrderRecord).status, "completed");

    const eventsRes = await fetch(`${baseUrl}/orders/${order.id}/events`);
    const events = (await eventsRes.json()) as Array<{ event: string }>;
    assertEquals(
      events.map((e) => e.event),
      ["order.created", "order.assigned", "order.started", "order.completed"],
    );

    await host.drain({ maxDurationMs: 5_000 });

    const notificationsRes = await fetch(`${baseUrl}/notifications`);
    const notifications = (await notificationsRes.json()) as Array<{
      order_id: number;
      status: string;
    }>;
    assert(notifications.some((n) => n.order_id === order.id && n.status === "sent"));
  } finally {
    await started.stop();
  }
});

Deno.test("deno/intermediate — rejects illegal transitions", async () => {
  const port = await reservePort();
  const host = await createChimpbase({
    ...app,
    storage: { engine: "memory" },
    server: { port },
    subscriptions: { dispatch: "sync" },
  });
  const started = await host.start();
  const baseUrl = `http://127.0.0.1:${port}`;

  try {
    const created = await postJson(`${baseUrl}/orders`, {
      customer: "bob@example.com",
      amount: 1000,
    });
    const order = (await created.json()) as OrderRecord;

    const illegal = await postJson(`${baseUrl}/orders/${order.id}/complete`, {});
    assert(illegal.status >= 500);
    await illegal.body?.cancel();
  } finally {
    await started.stop();
  }
});
