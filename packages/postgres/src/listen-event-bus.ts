import type { Pool, PoolClient } from "pg";
import type {
  ChimpbaseEventBus,
  ChimpbaseEventBusCallback,
  ChimpbaseEventRecord,
} from "@chimpbase/core";

const NOTIFY_PAYLOAD_LIMIT = 7800;
const CHANNEL_IDENTIFIER = /^[a-zA-Z_][a-zA-Z0-9_]{0,62}$/;

export interface PostgresListenEventBusOptions {
  channel?: string;
  originId?: string;
  pool: Pool;
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

export class PostgresListenEventBus implements ChimpbaseEventBus {
  private readonly channel: string;
  private readonly originId: string;
  private readonly pool: Pool;
  private client: PoolClient | null = null;
  private callback: ChimpbaseEventBusCallback | null = null;
  private notificationHandler: ((msg: { channel: string; payload?: string }) => void) | null = null;

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
    this.callback = callback;
    void this.attach();
  }

  stop(): void {
    this.callback = null;

    if (this.client !== null && this.notificationHandler !== null) {
      this.client.removeListener("notification", this.notificationHandler);
    }

    this.notificationHandler = null;

    if ((this.client !== null)) {
      const client = this.client;
      this.client = null;
      void (async () => {
        try {
          await client.query(`UNLISTEN ${this.channel}`);
        } finally {
          client.release();
        }
      })();
    }
  }

  private async attach(): Promise<void> {
    try {
      const client = await this.pool.connect();
      this.client = client;

      const handler = (msg: { channel: string; payload?: string }) => {
        if (msg.channel !== this.channel || !(msg.payload !== undefined && msg.payload.length > 0)) return;
        void this.dispatch(msg.payload);
      };

      this.notificationHandler = handler;
      client.on("notification", handler);
      await client.query(`LISTEN ${this.channel}`);
    } catch (error) {
      console.error("[@chimpbase/postgres][listen-event-bus] attach failed", error);
    }
  }

  private async dispatch(payload: string): Promise<void> {
    const callback = this.callback;
    if (!(callback !== null)) return;

    let envelope: NotifyEnvelope;
    try {
      envelope = JSON.parse(payload) as NotifyEnvelope;
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
