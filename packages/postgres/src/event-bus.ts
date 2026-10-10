import type { Pool } from "pg";
import type { ChimpbaseEventBus, ChimpbaseEventBusCallback, ChimpbaseEventRecord } from "@chimpbase/core";
import { parseJson, v } from "@chimpbase/runtime";

export interface PostgresPollingEventBusOptions {
  pollIntervalMs?: number;
  pool: Pool;
}

const eventIdValidator = v.integer();
const maxDeliveryAttempts = 3;

interface PolledEvent {
  event_name: string;
  id: number;
  payload_json: string;
  transaction_id: string;
}

function parseSnapshot(snapshot: string): { xmax: string; inProgress: string[] } {
  const [, xmax, inProgress] = snapshot.split(":");
  return { xmax, inProgress: inProgress ? inProgress.split(",") : [] };
}

export class PostgresPollingEventBus implements ChimpbaseEventBus {
  readonly mode = "wake" as const;
  private readonly pollIntervalMs: number;
  private readonly pool: Pool;
  private interval: ReturnType<typeof setInterval> | null = null;
  private snapshot: ReturnType<typeof parseSnapshot> | null = null;
  private windowSnapshot: string | null = null;
  private readonly seenIds = new Set<number>();
  private readonly publishedIds = new Set<number>();
  private readonly retries = new Map<number, { row: PolledEvent; attempts: number }>();
  private polling = false;
  private generation = 0;

  constructor(options: PostgresPollingEventBusOptions) {
    this.pool = options.pool;
    this.pollIntervalMs = options.pollIntervalMs ?? 1000;
  }

  async publish(events: ChimpbaseEventRecord[]): Promise<void> {
    if (this.interval === null) return;
    for (const event of events) {
      if (event.id !== undefined) {
        this.publishedIds.add(event.id);
      }
    }
  }

  start(callback: ChimpbaseEventBusCallback): void {
    if (this.interval !== null) return;
    const generation = ++this.generation;
    this.snapshot = null;
    this.windowSnapshot = null;
    const initialized = this.initializeSnapshot(generation);

    this.interval = setInterval(() => {
      void initialized.then(async () => {
        if (generation === this.generation) await this.poll(callback, generation);
      });
    }, this.pollIntervalMs);
  }

  stop(): void {
    this.generation += 1;
    if ((this.interval !== null)) {
      clearInterval(this.interval);
      this.interval = null;
    }
    this.publishedIds.clear();
    this.seenIds.clear();
    this.retries.clear();
  }

  private async initializeSnapshot(generation: number): Promise<void> {
    try {
      const result = await this.pool.query<{ snapshot: string }>(
        "SELECT txid_current_snapshot()::text AS snapshot",
      );
      if (generation === this.generation) this.snapshot = parseSnapshot(result.rows[0].snapshot);
    } catch (error) {
      console.error("[@chimpbase/postgres][event-bus] failed to initialize snapshot", error);
    }
  }

  private async poll(callback: ChimpbaseEventBusCallback, generation: number): Promise<void> {
    if (this.polling) return;
    this.polling = true;

    try {
      if (this.snapshot === null) await this.initializeSnapshot(generation);
      if (this.snapshot === null || generation !== this.generation) return;
      const result = await this.pool.query<PolledEvent & { snapshot: string }>(
        `SELECT snapshot.snapshot, events.*
         FROM (SELECT COALESCE($4::txid_snapshot, txid_current_snapshot())::text AS snapshot) snapshot
         LEFT JOIN LATERAL (
           SELECT CAST(id AS DOUBLE PRECISION) AS id, event_name,
             payload_json::text AS payload_json, transaction_id::text AS transaction_id
           FROM _chimpbase_events
           WHERE (transaction_id >= $1::bigint OR transaction_id = ANY($2::bigint[]))
             AND txid_visible_in_snapshot(transaction_id, snapshot.snapshot::txid_snapshot)
             AND NOT (id = ANY($3::bigint[]))
           ORDER BY id ASC
           LIMIT 100
         ) events ON TRUE`,
        [this.snapshot.xmax, this.snapshot.inProgress, [...this.seenIds], this.windowSnapshot],
      );
      if (generation !== this.generation) return;
      this.windowSnapshot = result.rows[0].snapshot;
      const rows = result.rows.filter((row) => row.id !== null);
      const retryRows = [...this.retries.values()].map(({ row }) => row);
      for (const row of [...retryRows, ...rows]) {
        if (generation !== this.generation) return;
        if (!this.publishedIds.delete(row.id)) await this.deliver(row, callback, generation);
        if (generation !== this.generation) return;
        this.seenIds.add(row.id);
      }
      // Freeze the window while paging so new arrivals cannot prevent cursor advancement.
      if (rows.length < 100) {
        this.snapshot = parseSnapshot(result.rows[0].snapshot);
        this.windowSnapshot = null;
        this.seenIds.clear();
      }
    } catch (error) {
      console.error("[@chimpbase/postgres][event-bus] poll error", error);
    } finally {
      this.polling = false;
    }
  }

  private async deliver(row: PolledEvent, callback: ChimpbaseEventBusCallback, generation: number): Promise<void> {
    const attempts = (this.retries.get(row.id)?.attempts ?? 0) + 1;
    try {
      await callback([{
        id: eventIdValidator.parse(row.id, "polled event id"),
        name: row.event_name,
        payload: parseJson(row.payload_json, `event ${row.id} payload`),
        payloadJson: row.payload_json,
      }]);
      if (generation === this.generation) this.retries.delete(row.id);
    } catch (error) {
      if (generation !== this.generation) return;
      if (attempts < maxDeliveryAttempts) this.retries.set(row.id, { row, attempts });
      else this.retries.delete(row.id);
      console.error("[@chimpbase/postgres][event-bus] delivery failed", {
        eventId: row.id, eventName: row.event_name, attempts,
        exhausted: attempts === maxDeliveryAttempts, error,
      });
    }
  }
}
