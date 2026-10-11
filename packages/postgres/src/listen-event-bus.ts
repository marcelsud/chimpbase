import type {
  ChimpbaseEventBus,
  ChimpbaseEventBusCallback,
  ChimpbaseEventRecord,
} from "@chimpbase/core";
import { parseJson, v } from "@chimpbase/runtime";


const NOTIFY_PAYLOAD_LIMIT = 7800;
const CHANNEL_IDENTIFIER = /^[a-zA-Z_][a-zA-Z0-9_]{0,62}$/;
const RECONNECT_DELAY_MS = 1000;

interface PostgresListenClient {
  on(event: "notification", listener: (message: { channel: string; payload?: string }) => void): this;
  on(event: "error", listener: (error: Error) => void): this;
  on(event: "end", listener: () => void): this;
  query(sql: string, params?: unknown[]): Promise<unknown>;
  release(destroy?: boolean): void;
  removeListener(event: "notification", listener: (message: { channel: string; payload?: string }) => void): this;
  removeListener(event: "error", listener: (error: Error) => void): this;
  removeListener(event: "end", listener: () => void): this;
}

export interface PostgresListenPool {
  connect(): Promise<PostgresListenClient>;
  query(sql: string, params?: unknown[]): Promise<unknown>;
}

export interface PostgresListenEventBusOptions {
  channel?: string;
  originId?: string;
  pool: PostgresListenPool;
}

export class PayloadTooLargeError extends Error {
  constructor(
    public readonly size: number,
    public readonly limit: number,
    public readonly eventName: string,
  ) {
    super(
      `pg_notify payload ${size}B exceeds ${limit}B for event "${eventName}". ` +
        `Switch to a transport with larger payload capacity (e.g. Redis Streams).`,
    );
    this.name = "PayloadTooLargeError";
  }
}

interface NotifyEnvelope {
  event: ChimpbaseEventRecord;
  origin: string;
}
const notifyEnvelopeValidator = v.object({
  event: v.object({
    id: v.integer().optional(),
    name: v.string(),
    payload: v.unknown().optional(),
    payloadJson: v.string(),
  }),
  origin: v.string(),
});


export class PostgresListenEventBus implements ChimpbaseEventBus {
  readonly mode = "wake" as const;
  private readonly channel: string;
  private readonly originId: string;
  private readonly pool: PostgresListenPool;
  private callback: ChimpbaseEventBusCallback | null = null;
  private detach: (() => void) | null = null;
  private retryTimer: ReturnType<typeof setTimeout> | null = null;
  private generation = 0;

  constructor(options: PostgresListenEventBusOptions) {
    if (!CHANNEL_IDENTIFIER.test(options.channel ?? "chimpbase_events")) {
      throw new Error(`invalid channel name: ${options.channel}`);
    }

    this.pool = options.pool;
    this.channel = options.channel ?? "chimpbase_events";
    this.originId = options.originId ?? randomId();
  }

  async publish(events: ChimpbaseEventRecord[]): Promise<void> {
    for (const event of events) {
      const envelope: NotifyEnvelope = { event, origin: this.originId };
      const payload = JSON.stringify(envelope);
      const size = Buffer.byteLength(payload, "utf8");

      if (size > NOTIFY_PAYLOAD_LIMIT) {
        throw new PayloadTooLargeError(size, NOTIFY_PAYLOAD_LIMIT, event.name);
      }

      await this.pool.query("SELECT pg_notify($1, $2)", [this.channel, payload]);
    }
  }

  start(callback: ChimpbaseEventBusCallback): void {
    const started = this.callback !== null;
    this.callback = callback;
    if (!started) void this.attach(++this.generation);
  }

  stop(): void {
    this.generation += 1;
    this.callback = null;
    if (this.retryTimer !== null) {
      clearTimeout(this.retryTimer);
      this.retryTimer = null;
    }
    this.detach?.();
  }

  private async attach(generation: number): Promise<void> {
    let detach: (() => void) | null = null;
    try {
      const client = await this.pool.connect();
      if (generation !== this.generation) {
        client.release();
        return;
      }

      let released = false;
      const handler = (msg: { channel: string; payload?: string }) => {
        if (released || generation !== this.generation) return;
        if (msg.channel !== this.channel || !(msg.payload !== undefined && msg.payload.length > 0)) return;
        void this.dispatch(msg.payload);
      };
      const onError = (error: Error) => {
        console.error("[@chimpbase/postgres][listen-event-bus] connection error", error);
        detach?.();
        this.retry(generation);
      };
      const onEnd = () => {
        detach?.();
        this.retry(generation);
      };
      detach = () => {
        if (released) return;
        released = true;
        if (this.detach === detach) this.detach = null;
        client.removeListener("notification", handler);
        client.removeListener("end", onEnd);
        // Destroy the dedicated listener without waiting on an unavailable server.
        client.release(true);
        client.removeListener("error", onError);
      };

      this.detach = detach;
      client.on("error", onError);
      client.on("end", onEnd);
      client.on("notification", handler);
      await client.query(`LISTEN ${this.channel}`);
    } catch (error) {
      detach?.();
      if (generation !== this.generation) return;
      console.error("[@chimpbase/postgres][listen-event-bus] attach failed", error);
      this.retry(generation);
    }
  }

  private retry(generation: number): void {
    if (generation !== this.generation || this.detach !== null || this.retryTimer !== null) return;
    this.retryTimer = setTimeout(() => {
      this.retryTimer = null;
      void this.attach(generation);
    }, RECONNECT_DELAY_MS);
  }

  private async dispatch(payload: string): Promise<void> {
    const callback = this.callback;
    if (!(callback !== null)) return;

    let envelope: NotifyEnvelope;
    try {
      const parsed = notifyEnvelopeValidator.parse(
        parseJson(payload, "Postgres notification"),
        "Postgres notification",
      );
      envelope = {
        event: { ...parsed.event, payload: parsed.event.payload },
        origin: parsed.origin,
      };
    } catch (error) {
      console.error("[@chimpbase/postgres][listen-event-bus] invalid payload", error);
      return;
    }

    if (envelope.origin === this.originId) return;

    try {
      await callback([envelope.event]);
    } catch (error) {
      console.error("[@chimpbase/postgres][listen-event-bus] callback error", error);
    }
  }
}

function randomId(): string {
  return (
    Math.random().toString(36).slice(2) +
    Date.now().toString(36) +
    Math.random().toString(36).slice(2)
  );
}
