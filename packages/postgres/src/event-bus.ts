import type { Pool } from "pg";
import type { ChimpbaseEventBus, ChimpbaseEventBusCallback, ChimpbaseEventRecord } from "@chimpbase/core";
import { parseJson, v } from "@chimpbase/runtime";

export interface PostgresPollingEventBusOptions {
  pollIntervalMs?: number;
  pool: Pool;
}

const eventIdValidator = v.integer();

export class PostgresPollingEventBus implements ChimpbaseEventBus {
  readonly mode = "wake" as const;
  private readonly pollIntervalMs: number;
  private readonly pool: Pool;
  private interval: ReturnType<typeof setInterval> | null = null;
  private lastSeenId = 0;
  private polling = false;

  constructor(options: PostgresPollingEventBusOptions) {
    this.pool = options.pool;
    this.pollIntervalMs = options.pollIntervalMs ?? 1000;
  }

  async publish(_events: ChimpbaseEventRecord[]): Promise<void> {
    // Advance high-water mark past events this process just committed,
    // so the poller does not re-deliver them locally.
    const result = await this.pool.query<{ max_id: string | null }>(
      "SELECT MAX(id) AS max_id FROM _chimpbase_events",
    );
    const maxId = result.rows[0]?.max_id;
    if ((maxId !== null && maxId.length > 0)) {
      this.lastSeenId = Math.max(this.lastSeenId, Number(maxId));
    }
  }

  start(callback: ChimpbaseEventBusCallback): void {
    void this.initializeHighWaterMark();

    this.interval = setInterval(() => {
      void this.poll(callback);
    }, this.pollIntervalMs);
  }

  stop(): void {
    if ((this.interval !== null)) {
      clearInterval(this.interval);
      this.interval = null;
    }
  }

  private async initializeHighWaterMark(): Promise<void> {
    try {
      const result = await this.pool.query<{ max_id: string | null }>(
        "SELECT MAX(id) AS max_id FROM _chimpbase_events",
      );
      const maxId = result.rows[0]?.max_id;
      const parsedMaxId = (maxId !== null && maxId.length > 0) ? Number(maxId) : 0;
      this.lastSeenId = eventIdValidator.parse(parsedMaxId, "event high-water mark");
    } catch (error) {
      console.error("[@chimpbase/postgres][event-bus] failed to initialize high-water mark", error);
    }
  }

  private async poll(callback: ChimpbaseEventBusCallback): Promise<void> {
    if (this.polling) return;
    this.polling = true;

    try {
      const result = await this.pool.query<{
        event_name: string;
        id: number;
        payload_json: string;
      }>(
        `SELECT CAST(id AS DOUBLE PRECISION) AS id, event_name, payload_json::text AS payload_json
         FROM _chimpbase_events
         WHERE id > $1
         ORDER BY id ASC
         LIMIT 100`,
        [this.lastSeenId],
      );

      if (result.rows.length === 0) return;

      const events: ChimpbaseEventRecord[] = result.rows.map((row) => ({
        id: eventIdValidator.parse(row.id, "polled event id"),
        name: row.event_name,
        payload: parseJson(row.payload_json, `event ${row.id} payload`),
        payloadJson: row.payload_json,
      }));

      this.lastSeenId = events[events.length - 1].id ?? this.lastSeenId;
      await callback(events);
    } catch (error) {
      console.error("[@chimpbase/postgres][event-bus] poll error", error);
    } finally {
      this.polling = false;
    }
  }
}
