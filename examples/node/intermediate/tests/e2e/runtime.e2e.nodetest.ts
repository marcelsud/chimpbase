import { after, before, describe, test } from "node:test";
import { strict as assert } from "node:assert";
import { createServer } from "node:net";

import { createChimpbase } from "@chimpbase/node";

import app from "../../chimpbase.app.ts";
import type { OrderRecord } from "../../src/modules/orders/order.types.ts";

type Host = Awaited<ReturnType<typeof createChimpbase>>;
type StartedHost = Awaited<ReturnType<Host["start"]>>;

async function reservePort(): Promise<number> {
  return await new Promise((resolveFn, rejectFn) => {
    const server = createServer();
    server.once("error", rejectFn);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      if (address === null || typeof address === "string") {
        server.close(() => rejectFn(new Error("no port")));
        return;
      }
      const { port } = address;
      server.close((error) => ((error !== undefined) ? rejectFn(error) : resolveFn(port)));
    });
  });
}

async function postJson(url: string, body: unknown): Promise<Response> {
  return await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
}

describe("node/intermediate example", () => {
  let host: Host;
  let started: StartedHost;
  let baseUrl: string;

  before(async () => {
    const port = await reservePort();
    host = await createChimpbase({
      ...app,
      storage: { engine: "memory" },
      server: { port },
      subscriptions: { dispatch: "sync" },
    });
    started = await host.start();
    baseUrl = `http://127.0.0.1:${port}`;
  });

  after(async () => {
    await started.stop();
  });

  test("routes an order through the full lifecycle", async () => {
    const created = await postJson(`${baseUrl}/orders`, {
      customer: "Alice@Example.com",
      amount: 4200,
    });
    assert.equal(created.status, 201);
    const order = (await created.json()) as OrderRecord;
    assert.equal(order.status, "pending");
    assert.equal(order.customer, "alice@example.com");
    assert.equal(order.amount, 4200);

    const assigned = await postJson(`${baseUrl}/orders/${order.id}/assign`, {
      assignee: "ops-1",
    });
    assert.equal(assigned.status, 200);
    assert.equal(((await assigned.json()) as OrderRecord).status, "assigned");

    const startedRes = await postJson(`${baseUrl}/orders/${order.id}/start`, {});
    assert.equal(((await startedRes.json()) as OrderRecord).status, "in_progress");

    const completed = await postJson(`${baseUrl}/orders/${order.id}/complete`, {});
    assert.equal(((await completed.json()) as OrderRecord).status, "completed");

    const eventsRes = await fetch(`${baseUrl}/orders/${order.id}/events`);
    const events = (await eventsRes.json()) as Array<{ event: string }>;
    assert.deepEqual(
      events.map((e) => e.event),
      ["order.created", "order.assigned", "order.started", "order.completed"],
    );

    await host.drain({ maxDurationMs: 5_000 });

    const notificationsRes = await fetch(`${baseUrl}/notifications`);
    const notifications = (await notificationsRes.json()) as Array<{
      order_id: number;
      status: string;
    }>;
    assert.ok(notifications.some((n) => n.order_id === order.id && n.status === "sent"));
  });

  test("rejects illegal transitions", async () => {
    const created = await postJson(`${baseUrl}/orders`, {
      customer: "bob@example.com",
      amount: 1000,
    });
    const order = (await created.json()) as OrderRecord;

    const illegal = await postJson(`${baseUrl}/orders/${order.id}/complete`, {});
    assert.ok(illegal.status >= 500);
  });
});
