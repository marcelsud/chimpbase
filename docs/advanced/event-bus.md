# Event Bus Transports

An event bus delivers committed pub/sub events to other processes. For application-level handlers, start with [Subscriptions](/subscriptions).

## Built-in Hosts

PostgreSQL hosts create a `PostgresPollingEventBus` automatically. Each host starts it when `host.start()` runs. SQLite and memory do not provide cross-process event delivery.

With `subscriptions.dispatch: "async"`, subscription jobs are also stored in the shared queue. Any worker with the same registrations can execute them. Use stable names and `idempotent: true` to deduplicate successful subscription deliveries.

Queue retries can repeat external effects. Use an idempotency key for email, payments, or calls to another service. See [Workers](/workers) for the transaction and retry behavior.

## Direct Engine Integrations

`chimpbase/postgres` exports two transports:

| Transport | Delivery | Limit |
| --- | --- | --- |
| `PostgresPollingEventBus` | Polls the persisted event table | Poll interval adds latency; no NOTIFY payload limit |
| `PostgresListenEventBus` | PostgreSQL LISTEN/NOTIFY | Full event envelope must fit within 7,800 bytes |

A directly constructed `ChimpbaseEngine` defaults to `NoopEventBus`. Supply a transport through its `eventBus` option and call `startEventBus(runOperation)` with the scheduler used for other engine operations. Nested `ctx.action` calls keep the current transaction.

Individual subscriptions can set `dispatch: "sync"` or `dispatch: "async"` in their options to override the host default. Synchronous handlers run in the publishing transaction on the local node and in the event listener on peers; asynchronous handlers run through shared queue jobs. Mesh broadcast subscriptions use the synchronous override to deliver to every listening node, independently of the host default.

The polling transport skips events committed by its own process and committed history from before the listener starts. It tracks PostgreSQL transaction snapshots rather than event ID order: an earlier event whose transaction commits later is still delivered, including transactions already in flight when the listener starts. Snapshot windows are frozen while reading pages of 100 events, so continuous arrivals cannot prevent cursor advancement. The event table's indexed `transaction_id` column records the publishing transaction; upgrading an existing table preserves old rows without backfilling this column.

Each polled event is delivered in its own callback and transaction. A failing event retries on subsequent polls for up to three attempts while healthy events continue. The final failure emits a structured error with `eventId`, `eventName`, `attempts`, and `exhausted: true`; it is then skipped on that listener. Successful events in other callbacks are not replayed. Several subscriptions to the same event still share its transaction and can repeat external effects when another subscription fails, so make those effects idempotent.

Polling broadcast state is held in memory: processed IDs are retained only while draining the current frozen window, and failed envelopes only until their retry limit. Broadcasts remain non-durable across listener restarts; stopping the listener clears retry state, and a new listener skips already committed history. The persisted event table has no automatic retention policy; remove old events only after active listeners have consumed them. Use shared queues or module outboxes for deliveries that must survive a node restart. LISTEN/NOTIFY filters its own origin and provides low-latency peer delivery.

An oversized LISTEN/NOTIFY envelope throws `PayloadTooLargeError` after the event has committed. The event remains stored, but the notification is not sent. Use polling for larger payloads.

Use [business modules](/advanced/modules) when you need durable, versioned module-event delivery. Their event log and outbox queue are the delivery source; event buses only wake consumers.
