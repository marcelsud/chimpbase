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

The polling transport skips events committed by its own process. It advances its cursor after successful delivery, so a failed callback retries on the next poll. LISTEN/NOTIFY filters its own origin and provides low-latency peer delivery.

An oversized LISTEN/NOTIFY envelope throws `PayloadTooLargeError` after the event has committed. The event remains stored, but the notification is not sent. Use polling for larger payloads.

Use [business modules](/advanced/modules) when you need durable, versioned module-event delivery. Their event log and outbox queue are the delivery source; event buses only wake consumers.
