import { after, before, describe, test } from "node:test";
import { strict as assert } from "node:assert";

import { authedGet, authedPost, bootAdvanced } from "../support/harness.ts";
import type { OrderRecord } from "../../../../shared/orders/order.types.ts";

describe("node/advanced example — workflow", () => {
  let booted: Awaited<ReturnType<typeof bootAdvanced>>;

  before(async () => {
    booted = await bootAdvanced();
  });

  after(async () => {
    await booted.started.stop();
  });

  test("fulfilment workflow completes on quality approval", async () => {
    const { baseUrl, host } = booted;

    const order = (await (await authedPost(`${baseUrl}/orders`, {
      customer: "charlie@example.com",
      amount: 9900,
    })).json()) as OrderRecord;

    const launch = await authedPost(`${baseUrl}/orders/${order.id}/fulfilment`, {
      assignee: "ops-2",
    });
    assert.equal(launch.status, 201);
    const { workflowId } = (await launch.json()) as { workflowId: string };
    assert.ok(workflowId);

    await host.drain({ maxDurationMs: 5_000 });

    let instance = (await (await authedGet(`${baseUrl}/fulfilments/${workflowId}`)).json()) as {
      status: string;
      state: { phase: string };
    };
    assert.equal(instance.status, "waiting_signal");
    assert.equal(instance.state.phase, "awaiting-quality");

    await authedPost(`${baseUrl}/fulfilments/${workflowId}/quality`, { approved: true });
    await host.drain({ maxDurationMs: 5_000 });

    instance = (await (await authedGet(`${baseUrl}/fulfilments/${workflowId}`)).json()) as {
      status: string;
      state: { phase: string };
    };
    assert.equal(instance.status, "completed");
    assert.equal(instance.state.phase, "done");

    const finalOrders = (await (await authedGet(`${baseUrl}/orders`)).json()) as OrderRecord[];
    assert.equal(finalOrders.find((o) => o.id === order.id)?.status, "completed");
  });

  test("fulfilment workflow rejects on quality denial", async () => {
    const { baseUrl, host } = booted;

    const order = (await (await authedPost(`${baseUrl}/orders`, {
      customer: "dana@example.com",
      amount: 5500,
    })).json()) as OrderRecord;

    const launch = await authedPost(`${baseUrl}/orders/${order.id}/fulfilment`, {
      assignee: "ops-3",
    });
    const { workflowId } = (await launch.json()) as { workflowId: string };
    await host.drain({ maxDurationMs: 5_000 });

    await authedPost(`${baseUrl}/fulfilments/${workflowId}/quality`, {
      approved: false,
      reason: "torn package",
    });
    await host.drain({ maxDurationMs: 5_000 });

    const instance = (await (await authedGet(`${baseUrl}/fulfilments/${workflowId}`)).json()) as {
      status: string;
      state: { phase: string; rejectionReason: string };
    };
    assert.equal(instance.status, "completed");
    assert.equal(instance.state.phase, "done");
    assert.equal(instance.state.rejectionReason, "torn package");

    const orders = (await (await authedGet(`${baseUrl}/orders`)).json()) as OrderRecord[];
    assert.equal(orders.find((o) => o.id === order.id)?.status, "rejected");
  });
});
