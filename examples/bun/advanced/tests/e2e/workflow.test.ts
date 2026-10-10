import { afterEach, beforeEach, describe, expect, test } from "bun:test";

import { authedGet, authedPost, bootAdvanced } from "../support/harness.ts";
import type { OrderRecord } from "../../../../shared/orders/order.types.ts";

describe("bun/advanced example — workflow", () => {
  let booted: Awaited<ReturnType<typeof bootAdvanced>>;

  beforeEach(async () => {
    booted = await bootAdvanced();
  });

  afterEach(async () => {
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
    expect(launch.status).toBe(201);
    const { workflowId } = (await launch.json()) as { workflowId: string };
    expect(workflowId).toBeTruthy();

    await host.drain({ maxDurationMs: 5_000 });

    let instance = (await (await authedGet(`${baseUrl}/fulfilments/${workflowId}`)).json()) as {
      status: string;
      state: { phase: string };
    };
    expect(instance.status).toBe("waiting_signal");
    expect(instance.state.phase).toBe("awaiting-quality");

    await authedPost(`${baseUrl}/fulfilments/${workflowId}/quality`, { approved: true });
    await host.drain({ maxDurationMs: 5_000 });

    instance = (await (await authedGet(`${baseUrl}/fulfilments/${workflowId}`)).json()) as {
      status: string;
      state: { phase: string };
    };
    expect(instance.status).toBe("completed");
    expect(instance.state.phase).toBe("done");

    const finalOrder = (await (await authedGet(`${baseUrl}/orders`)).json()) as OrderRecord[];
    expect(finalOrder.find((o) => o.id === order.id)?.status).toBe("completed");
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
    expect(instance.status).toBe("completed");
    expect(instance.state.phase).toBe("done");
    expect(instance.state.rejectionReason).toBe("torn package");

    const orders = (await (await authedGet(`${baseUrl}/orders`)).json()) as OrderRecord[];
    expect(orders.find((o) => o.id === order.id)?.status).toBe("rejected");
  });
});
