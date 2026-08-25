import { describe, expect, test } from "bun:test";
import { EventEmitter } from "node:events";

import type { ChimpbaseEventRecord } from "../packages/core/index.ts";
import { parseJson } from "../packages/runtime/index.ts";
import {
  PayloadTooLargeError,
  PostgresListenEventBus,
} from "../packages/postgres/src/listen-event-bus.ts";

type QueryCall = { params: unknown[]; sql: string };

class FakeClient extends EventEmitter {
  released = false;
  queries: QueryCall[] = [];

  async query(sql: string, params: unknown[] = []): Promise<{ rows: unknown[] }> {
    this.queries.push({ params, sql });
    return { rows: [] };
  }

  release(): void {
    this.released = true;
  }
}

class FakePool {
  queries: QueryCall[] = [];
  readonly client: FakeClient;
  connectCalls = 0;

  constructor() {
    this.client = new FakeClient();
  }

  async query(sql: string, params: unknown[] = []): Promise<{ rows: unknown[] }> {
    this.queries.push({ params, sql });
    return { rows: [] };
  }

  async connect(): Promise<FakeClient> {
    this.connectCalls += 1;
    return this.client;
  }
}

function event(name: string, payload: unknown): ChimpbaseEventRecord {
  const payloadJson = JSON.stringify(payload);
  return { id: 1, name, payload, payloadJson };
}

async function waitFor(predicate: () => boolean, timeoutMs = 500): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
  throw new Error("waitFor timed out");
}

describe("PostgresListenEventBus", () => {
  test("publish sends pg_notify with origin-tagged envelope", async () => {
    const pool = new FakePool();
    const bus = new PostgresListenEventBus({
      channel: "chimpbase_events",
      originId: "origin-A",
      pool: pool,
    });

    await bus.publish([event("order.created", { orderId: "123" })]);

    expect(pool.queries).toHaveLength(1);
    expect(pool.queries[0].sql).toBe("SELECT pg_notify($1, $2)");
    const [channel, payload] = pool.queries[0].params;
    expect(channel).toBe("chimpbase_events");
    if (typeof payload !== "string") throw new Error("pg_notify payload missing");
    expect(parseJson(payload)).toEqual({
      event: { id: 1, name: "order.created", payload: { orderId: "123" }, payloadJson: '{"orderId":"123"}' },
      origin: "origin-A",
    });
  });

  test("publish throws PayloadTooLargeError when envelope exceeds limit", async () => {
    const pool = new FakePool();
    const bus = new PostgresListenEventBus({ pool: pool });
    const big = "x".repeat(8000);

    await expect(bus.publish([event("huge", { blob: big })])).rejects.toBeInstanceOf(
      PayloadTooLargeError,
    );
    expect(pool.queries).toHaveLength(0);
  });

  test("invalid channel name rejected at construction", () => {
    const pool = new FakePool();
    expect(
      () =>
        new PostgresListenEventBus({
          channel: "bad channel; DROP TABLE",
          pool: pool,
        }),
    ).toThrow(/invalid channel name/);
  });

  test("start subscribes with LISTEN and forwards foreign-origin events", async () => {
    const pool = new FakePool();
    const bus = new PostgresListenEventBus({
      channel: "chimpbase_events",
      originId: "origin-A",
      pool: pool,
    });

    const received: ChimpbaseEventRecord[][] = [];
    bus.start(async (events) => {
      received.push(events);
    });

    await waitFor(() => pool.client.queries.some((q) => q.sql === "LISTEN chimpbase_events"));

    const foreignEnvelope = {
      event: event("order.created", { orderId: "xyz" }),
      origin: "origin-B",
    };
    pool.client.emit("notification", {
      channel: "chimpbase_events",
      payload: JSON.stringify(foreignEnvelope),
    });

    await waitFor(() => received.length > 0);
    expect(received[0][0].name).toBe("order.created");
    pool.client.emit("notification", {
      channel: "chimpbase_events",
      payload: JSON.stringify({
        event: { id: 2, name: "optional.payload", payloadJson: "null" },
        origin: "origin-B",
      }),
    });
    await waitFor(() => received.length > 1);
    expect(received[1][0].payload).toBeUndefined();


    bus.stop();
  });

  test("self-origin notifications are suppressed", async () => {
    const pool = new FakePool();
    const bus = new PostgresListenEventBus({
      channel: "chimpbase_events",
      originId: "origin-A",
      pool: pool,
    });

    const received: ChimpbaseEventRecord[][] = [];
    bus.start(async (events) => {
      received.push(events);
    });

    await waitFor(() => pool.client.queries.some((q) => q.sql === "LISTEN chimpbase_events"));

    const selfEnvelope = {
      event: event("order.created", { orderId: "self" }),
      origin: "origin-A",
    };
    pool.client.emit("notification", {
      channel: "chimpbase_events",
      payload: JSON.stringify(selfEnvelope),
    });

    await new Promise((resolve) => setTimeout(resolve, 30));
    expect(received).toEqual([]);

    bus.stop();
  });

  test("notifications on other channels are ignored", async () => {
    const pool = new FakePool();
    const bus = new PostgresListenEventBus({
      channel: "chimpbase_events",
      originId: "origin-A",
      pool: pool,
    });

    const received: ChimpbaseEventRecord[][] = [];
    bus.start(async (events) => {
      received.push(events);
    });

    await waitFor(() => pool.client.queries.some((q) => q.sql === "LISTEN chimpbase_events"));

    pool.client.emit("notification", {
      channel: "other_channel",
      payload: JSON.stringify({
        event: event("order.created", {}),
        origin: "origin-B",
      }),
    });

    await new Promise((resolve) => setTimeout(resolve, 30));
    expect(received).toEqual([]);

    bus.stop();
  });

  test("stop issues UNLISTEN and releases client", async () => {
    const pool = new FakePool();
    const bus = new PostgresListenEventBus({
      channel: "chimpbase_events",
      pool: pool,
    });

    bus.start(async () => {});
    await waitFor(() => pool.client.queries.some((q) => q.sql === "LISTEN chimpbase_events"));

    bus.stop();

    await waitFor(() => pool.client.released);
    expect(pool.client.queries.some((q) => q.sql === "UNLISTEN chimpbase_events")).toBe(true);
  });

  test("malformed payload does not throw and no callback fires", async () => {
    const pool = new FakePool();
    const bus = new PostgresListenEventBus({
      channel: "chimpbase_events",
      pool: pool,
    });

    const received: ChimpbaseEventRecord[][] = [];
    bus.start(async (events) => {
      received.push(events);
    });

    await waitFor(() => pool.client.queries.some((q) => q.sql === "LISTEN chimpbase_events"));

    pool.client.emit("notification", {
      channel: "chimpbase_events",
      payload: "{not-json",
    });

    await new Promise((resolve) => setTimeout(resolve, 30));
    expect(received).toEqual([]);

    bus.stop();
  });
});
