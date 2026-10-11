import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { Pool } from "pg";

import type { ChimpbaseEventRecord } from "../packages/core/index.ts";
import {
  PayloadTooLargeError,
  PostgresListenEventBus,
} from "../packages/postgres/src/listen-event-bus.ts";

const PG_URL = process.env.CHIMPBASE_TEST_PG_URL;
const describeIfPg = (PG_URL !== undefined && PG_URL.length > 0) ? describe : describe.skip;

function uniqueChannel(suffix: string): string {
  return `chimpbase_test_${Date.now()}_${Math.floor(Math.random() * 1e6)}_${suffix}`;
}

function makeEvent(name: string, payload: unknown): ChimpbaseEventRecord {
  return { id: 1, name, payload, payloadJson: JSON.stringify(payload) };
}

async function waitFor(predicate: () => boolean | Promise<boolean>, timeoutMs = 3000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  throw new Error("waitFor timed out");
}

describeIfPg("PostgresListenEventBus (integration)", () => {
  let pool: Pool;

  beforeAll(() => {
    pool = new Pool({ connectionString: PG_URL });
  });

  afterAll(async () => {
    await pool.end();
  });

  test("publisher container reach subscriber container via LISTEN/NOTIFY", async () => {
    const channel = uniqueChannel("fanout");
    const publisher = new PostgresListenEventBus({ channel, originId: "pub-A", pool });
    const subscriber = new PostgresListenEventBus({ channel, originId: "sub-B", pool });

    const received: ChimpbaseEventRecord[] = [];
    subscriber.start(async (events) => {
      received.push(...events);
    });

    await new Promise((resolve) => setTimeout(resolve, 100));

    try {
      await publisher.publish([makeEvent("order.created", { orderId: "cross-container" })]);

      await waitFor(() => received.length > 0);
      expect(received[0].name).toBe("order.created");
      expect(received[0].payload).toEqual({ orderId: "cross-container" });
    } finally {
      subscriber.stop();
    }
  });

  test("self-origin notifications are suppressed (no loopback)", async () => {
    const channel = uniqueChannel("loop");
    const bus = new PostgresListenEventBus({ channel, originId: "same-origin", pool });

    const received: ChimpbaseEventRecord[] = [];
    bus.start(async (events) => {
      received.push(...events);
    });

    await new Promise((resolve) => setTimeout(resolve, 100));

    try {
      await bus.publish([makeEvent("self.event", { x: 1 })]);
      await new Promise((resolve) => setTimeout(resolve, 200));
      expect(received).toEqual([]);
    } finally {
      bus.stop();
    }
  });

  test("publish throws PayloadTooLargeError for oversized envelope", async () => {
    const channel = uniqueChannel("big");
    const bus = new PostgresListenEventBus({ channel, pool });
    const big = "x".repeat(8000);

    await expect(bus.publish([makeEvent("huge", { blob: big })])).rejects.toBeInstanceOf(
      PayloadTooLargeError,
    );
  });

  test("stop cleanly releases the listening connection", async () => {
    const channel = uniqueChannel("stop");
    const bus = new PostgresListenEventBus({ channel, pool });
    bus.start(async () => {});
    await new Promise((resolve) => setTimeout(resolve, 100));

    bus.stop();
    await new Promise((resolve) => setTimeout(resolve, 100));

    const result = await pool.query<{ count: string }>(
      "SELECT count(*)::text AS count FROM pg_stat_activity WHERE query LIKE $1",
      [`%LISTEN ${channel}%`],
    );
    expect(Number(result.rows[0]?.count ?? "0")).toBe(0);
  });

  test("reconnects after PostgreSQL terminates the listening backend", async () => {
    const channel = uniqueChannel("reconnect");
    const listenerPool = new Pool({ connectionString: PG_URL, max: 1 });
    listenerPool.on("error", () => {});
    const subscriber = new PostgresListenEventBus({ channel, originId: "subscriber", pool: listenerPool });
    const publisher = new PostgresListenEventBus({ channel, originId: "publisher", pool });
    const received: ChimpbaseEventRecord[] = [];
    const listenerPid = async () => (await pool.query<{ pid: number }>(
      "SELECT pid FROM pg_stat_activity WHERE datname = current_database() AND query = $1 AND state = 'idle'",
      [`LISTEN ${channel}`],
    )).rows[0]?.pid;
    subscriber.start(async (events) => { received.push(...events); });
    try {
      let initialPid: number | undefined;
      await waitFor(async () => {
        initialPid = await listenerPid();
        return initialPid !== undefined;
      });
      await pool.query("SELECT pg_terminate_backend($1)", [initialPid]);
      await waitFor(async () => {
        const pid = await listenerPid();
        return pid !== undefined && pid !== initialPid;
      });
      await publisher.publish([makeEvent("after-reconnect", { recovered: true })]);
      await waitFor(() => received.length > 0);
      expect(received).toEqual([makeEvent("after-reconnect", { recovered: true })]);
      expect(listenerPool.totalCount).toBe(1);
    } finally {
      subscriber.stop();
      await listenerPool.end();
    }
  });
});
