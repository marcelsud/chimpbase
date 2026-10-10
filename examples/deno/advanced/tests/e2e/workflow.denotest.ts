import { assert, assertEquals } from "jsr:@std/assert@1";

import { authedGet, authedPost, bootAdvanced } from "../support/harness.ts";
import type { OrderRecord } from "../../../../shared/orders/order.types.ts";

Deno.test("deno/advanced — workflow completes on quality approval", async () => {
  const booted = await bootAdvanced();
  try {
    const { baseUrl, host } = booted;

    const order = await (await authedPost(`${baseUrl}/orders`, {
      customer: "charlie@example.com",
      amount: 9900,
    })).json() as OrderRecord;

    const launch = await authedPost(`${baseUrl}/orders/${order.id}/fulfilment`, {
      assignee: "ops-2",
    });
    assertEquals(launch.status, 201);
    const { workflowId } = await launch.json() as { workflowId: string };
    assert(workflowId);

    await host.drain({ maxDurationMs: 5_000 });

    let instance = await (await authedGet(`${baseUrl}/fulfilments/${workflowId}`)).json() as {
      status: string;
      state: { phase: string };
    };
    assertEquals(instance.status, "waiting_signal");
    assertEquals(instance.state.phase, "awaiting-quality");

    const signalRes = await authedPost(`${baseUrl}/fulfilments/${workflowId}/quality`, {
      approved: true,
    });
    await signalRes.body?.cancel();
    await host.drain({ maxDurationMs: 5_000 });

    instance = await (await authedGet(`${baseUrl}/fulfilments/${workflowId}`)).json() as {
      status: string;
      state: { phase: string };
    };
    assertEquals(instance.status, "completed");
    assertEquals(instance.state.phase, "done");

    const finalOrders = await (await authedGet(`${baseUrl}/orders`)).json() as OrderRecord[];
    assertEquals(finalOrders.find((o) => o.id === order.id)?.status, "completed");
  } finally {
    await booted.started.stop();
  }
});

Deno.test("deno/advanced — workflow rejects on quality denial", async () => {
  const booted = await bootAdvanced();
  try {
    const { baseUrl, host } = booted;

    const order = await (await authedPost(`${baseUrl}/orders`, {
      customer: "dana@example.com",
      amount: 5500,
    })).json() as OrderRecord;

    const launch = await authedPost(`${baseUrl}/orders/${order.id}/fulfilment`, {
      assignee: "ops-3",
    });
    const { workflowId } = await launch.json() as { workflowId: string };
    await host.drain({ maxDurationMs: 5_000 });

    const signalRes = await authedPost(`${baseUrl}/fulfilments/${workflowId}/quality`, {
      approved: false,
      reason: "torn package",
    });
    await signalRes.body?.cancel();
    await host.drain({ maxDurationMs: 5_000 });

    const instance = await (await authedGet(`${baseUrl}/fulfilments/${workflowId}`)).json() as {
      status: string;
      state: { phase: string; rejectionReason: string };
    };
    assertEquals(instance.status, "completed");
    assertEquals(instance.state.phase, "done");
    assertEquals(instance.state.rejectionReason, "torn package");

    const orders = await (await authedGet(`${baseUrl}/orders`)).json() as OrderRecord[];
    assertEquals(orders.find((o) => o.id === order.id)?.status, "rejected");
  } finally {
    await booted.started.stop();
  }
});
