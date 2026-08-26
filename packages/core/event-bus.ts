import type { ChimpbaseEventRecord } from "./engine.ts";

export type ChimpbaseEventBusCallback = (
  events: ChimpbaseEventRecord[],
  ack?: () => Promise<void>,
) => Promise<void>;

export interface ChimpbaseEventBus {
  /** `wake` buses only signal workers; persisted outbox jobs remain the delivery source. */
  readonly mode?: "delivery" | "wake";
  /** Notify the bus that new events were committed. */
  publish(events: ChimpbaseEventRecord[]): Promise<void>;

  /** Start listening for events from other processes. */
  start(callback: ChimpbaseEventBusCallback): void;

  /** Stop listening. */
  stop(): void;
}

export class NoopEventBus implements ChimpbaseEventBus {
  async publish(_events: ChimpbaseEventRecord[]): Promise<void> {}
  start(_callback: ChimpbaseEventBusCallback): void {}
  stop(): void {}
}
