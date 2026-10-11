import { afterEach, describe, expect, mock, spyOn, test } from "bun:test";
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
  destroyed = false;
  releaseCalls = 0;
  queryFailure: Error | null = null;
  queries: QueryCall[] = [];

  async query(sql: string, params: unknown[] = []): Promise<{ rows: unknown[] }> {
    this.queries.push({ params, sql });
    if (this.queryFailure !== null) throw this.queryFailure;
    return { rows: [] };
  }

  release(destroy = false): void {
    this.released = true;
    this.destroyed = destroy;
    this.releaseCalls += 1;
  }
}

class FakePool {
  queries: QueryCall[] = [];
  readonly client: FakeClient;
  connectCalls = 0;
  connectFailures: Error[] = [];
  clients: FakeClient[];

  constructor() {
    this.client = new FakeClient();
    this.clients = [this.client];
  }

  async query(sql: string, params: unknown[] = []): Promise<{ rows: unknown[] }> {
    this.queries.push({ params, sql });
    return { rows: [] };
  }

  async connect(): Promise<FakeClient> {
    this.connectCalls += 1;
    const failure = this.connectFailures.shift();
    if (failure !== undefined) throw failure;
    return this.clients.shift() ?? this.client;
  }
}

function event(name: string, payload: unknown): ChimpbaseEventRecord {
  const payloadJson = JSON.stringify(payload);
  return { id: 1, name, payload, payloadJson };
}

async function waitFor(predicate: () => boolean, timeoutMs = 2500): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
  throw new Error("waitFor timed out");
}

afterEach(() => mock.restore());

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

  test("stop destroys the listening connection without a cleanup round-trip", async () => {
    const pool = new FakePool();
    const bus = new PostgresListenEventBus({
      channel: "chimpbase_events",
      pool: pool,
    });

    bus.start(async () => {});
    await waitFor(() => pool.client.queries.some((q) => q.sql === "LISTEN chimpbase_events"));

    bus.stop();

    await waitFor(() => pool.client.released);
    expect(pool.client.destroyed).toBe(true);
    expect(pool.client.queries.map((q) => q.sql)).toEqual(["LISTEN chimpbase_events"]);
  });

  test("retries initial connection and LISTEN failures", async () => {
    spyOn(console, "error").mockImplementation(() => {});
    const pool = new FakePool();
    pool.connectFailures.push(new Error("database unavailable"));
    pool.client.queryFailure = new Error("LISTEN connection lost");
    const recovered = new FakeClient();
    pool.clients.push(recovered);
    const bus = new PostgresListenEventBus({ pool });
    const received: ChimpbaseEventRecord[] = [];
    bus.start(async (events) => { received.push(...events); });
    try {
      await waitFor(() => recovered.queries.length > 0);
      expect(pool.connectCalls).toBe(3);
      expect(pool.client.destroyed).toBe(true);
      expect(pool.client.releaseCalls).toBe(1);
      recovered.emit("notification", {
        channel: "chimpbase_events",
        payload: JSON.stringify({ event: event("recovered", {}), origin: "other" }),
      });
      await waitFor(() => received.length > 0);
      expect(received[0].name).toBe("recovered");
    } finally {
      bus.stop();
    }
  });

  test("discards disconnected clients and resumes delivery after error and end", async () => {
    spyOn(console, "error").mockImplementation(() => {});
    const pool = new FakePool();
    const second = new FakeClient();
    const third = new FakeClient();
    pool.clients.push(second, third);
    const bus = new PostgresListenEventBus({ pool });
    const received: ChimpbaseEventRecord[] = [];
    bus.start(async (events) => { received.push(...events); });
    try {
      await waitFor(() => pool.client.queries.length > 0);
      expect(() => pool.client.emit("error", new Error("network disconnected"))).not.toThrow();
      await waitFor(() => second.queries.length > 0);
      expect(pool.client.destroyed).toBe(true);
      expect(pool.client.releaseCalls).toBe(1);
      expect(pool.client.listenerCount("notification")).toBe(0);
      second.emit("end");
      await waitFor(() => third.queries.length > 0);
      expect(second.destroyed).toBe(true);
      third.emit("notification", {
        channel: "chimpbase_events",
        payload: JSON.stringify({ event: event("after-reconnect", {}), origin: "other" }),
      });
      await waitFor(() => received.length > 0);
      expect(received[0].name).toBe("after-reconnect");
      expect(pool.connectCalls).toBe(3);
    } finally {
      bus.stop();
    }
  });

  test("stop releases a pending connection without LISTEN and allows restart", async () => {
    const pool = new FakePool();
    const pending = Promise.withResolvers<FakeClient>();
    spyOn(pool, "connect").mockImplementationOnce(() => pending.promise);
    const bus = new PostgresListenEventBus({ pool });
    bus.start(async () => {});
    bus.stop();
    const lateClient = new FakeClient();
    pending.resolve(lateClient);
    await waitFor(() => lateClient.released);
    expect(lateClient.queries).toEqual([]);
    expect(lateClient.releaseCalls).toBe(1);
    bus.start(async () => {});
    try {
      await waitFor(() => pool.client.queries.length > 0);
      expect(pool.client.queries[0].sql).toBe("LISTEN chimpbase_events");
    } finally {
      bus.stop();
    }
  });

  test("stop discards a stalled LISTEN connection without awaiting its query", async () => {
    spyOn(console, "error").mockImplementation(() => {});
    const pool = new FakePool();
    const pending = Promise.withResolvers<{ rows: unknown[] }>();
    pool.client.query = async (sql, params = []) => {
      pool.client.queries.push({ sql, params });
      return pending.promise;
    };
    const bus = new PostgresListenEventBus({ pool });
    bus.start(async () => {});
    await waitFor(() => pool.client.queries.length > 0);
    bus.stop();
    expect(pool.client.released).toBe(true);
    expect(pool.client.destroyed).toBe(true);
    expect(pool.client.releaseCalls).toBe(1);
    expect(pool.client.listenerCount("error")).toBe(0);
    pending.reject(new Error("LISTEN connection lost"));
    await Promise.resolve();
    expect(pool.client.queries.map((q) => q.sql)).toEqual(["LISTEN chimpbase_events"]);
  });

  test("start is idempotent and stop cancels pending reconnection", async () => {
    spyOn(console, "error").mockImplementation(() => {});
    const pool = new FakePool();
    const bus = new PostgresListenEventBus({ pool });
    bus.start(async () => {});
    bus.start(async () => {});
    await waitFor(() => pool.client.queries.length > 0);
    expect(pool.connectCalls).toBe(1);
    pool.client.emit("error", new Error("connection lost"));
    bus.stop();
    await new Promise((resolve) => setTimeout(resolve, 1100));
    expect(pool.connectCalls).toBe(1);
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
