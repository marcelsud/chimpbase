# Mesh

`chimpbase/mesh` adds Moleculer-style services with a distributed registry built on Chimpbase primitives — no broker required. Every participating node advertises itself into a Postgres registry table and refreshes its peers from that registry on startup and each heartbeat. Actions can be called locally or routed to a peer over HTTP.

Install:

```bash
bun add chimpbase
```

## `service()`

Group related actions, events, settings, and lifecycle hooks. Actions are registered under `v{version}.{name}.{action}`.

```ts
import { service } from "chimpbase/mesh";

const users = service({
  name: "users",
  version: 1,
  settings: { maxPerPage: 50 },
  methods: {
    normalize(email: string) {
      return email.toLowerCase();
    },
  },
  actions: {
    create: async (ctx, args: { email: string }, self) => {
      const email = self.methods.normalize(args.email);
      await ctx.db.query("INSERT INTO users (email) VALUES (?1)", [email]);
      return { email };
    },
  },
  events: {
    "user.deleted": async (_ctx, payload: { id: string }) => {
      console.log("deleted", payload.id);
    },
  },
});
```

`mixins` merge actions, events, methods, and settings before registration. Multiple mixins may share a base; circular references are rejected. Later mixins take precedence, followed by the service's own definitions.

## `chimpbaseMesh(options)`

Register services with the runtime and opt into distributed discovery and HTTP RPC.

```ts
import { chimpbaseMesh } from "chimpbase/mesh";

host.register(
  chimpbaseMesh({
    services: [users, orders],
    transport: "http",             // default — set "local-only" for single-node deployments
    advertisedUrl: "http://api:3000",
    meshToken: "MESH_TOKEN",       // secret name
    heartbeatMs: 10_000,           // default
    offlineAfterMs: 30_000,        // default
    gcAfterMs: 600_000,            // default
    defaultStrategy: "local-first",
    defaultTimeoutMs: 5_000,
  }),
);
```

### Options

| Option | Default | Purpose |
|---|---|---|
| `services` | — | Services to advertise and register. Required. |
| `transport` | `"http"` | `"local-only"` disables cross-node RPC and `meshToken`/`advertisedUrl` requirements. |
| `advertisedUrl` | env/hostname fallback | URL peers use to reach this node's RPC endpoint. |
| `meshToken` | — | Secret name (via `ctx.secret`) for authenticating inbound RPC. Required when `transport: "http"`. |
| `rpcPath` | `/__chimpbase/mesh/rpc` | Route registered on this node to receive RPC. |
| `heartbeatMs` | 10000 | Interval between heartbeats. |
| `offlineAfterMs` | 30000 | Peers with no heartbeat within this window are treated as offline. |
| `gcAfterMs` | 600000 | Cron sweep removes rows older than this. |
| `defaultStrategy` | `"local-first"` | `local-first` · `round-robin` · `random` · `cpu`. |
| `defaultTimeoutMs` | 5000 | Per-attempt deadline. |
| `defaultRetries` | 0 | Retry attempts on failure. |
| `middleware` | `[]` | Functions wrapping `ctx.mesh.call` (circuit breakers, tracing). |
| `meta` | `{}` | Published in the announce payload (e.g., `{ cpuLoad: 0.3 }`). |

## `ctx.mesh`

Every handler context receives a `mesh` client:

```ts
actions: {
  confirm: async (ctx, args: { orderId: string }) => {
    if (ctx.mesh === undefined) throw new Error("mesh unavailable");
    const summary = await ctx.mesh.call(
      "v1.billing.summarize",
      { orderId: args.orderId },
      summaryValidator,
      { timeoutMs: 2000, retry: { attempts: 2, delayMs: 100 } },
    );

    await ctx.mesh.emit("order.confirmed", { orderId: args.orderId });
    return summary;
  },
}
```

Methods:

- `call(name, args, resultValidator, options?)` — validate the local or remote result, then return it. Resolution prefers local actions before peers.
- `emit(event, payload, { balanced })` — balanced routes through a queue worker with retries. Default broadcasts via pubsub.
- `nodeId()` — this node's UUID (regenerated each boot).
- `peers()` — current live peers from the local cache.

Retries apply to remote transport failures, remote timeouts, and unavailable nodes. Application errors and invalid results are not retried. Result validation errors also bypass fallback.

A remote timeout does not guarantee that the receiving action stopped or rolled back. Retry remote actions only when repeated execution is safe, such as an operation that deduplicates by a request ID.

Local actions share the caller's transaction. Their errors and timeouts bypass retry and fallback so the caller can roll back partial writes. A local timeout waits for the action to settle before raising `MeshTimeoutError`; it does not cancel the action. An action that never settles keeps the call and transaction open.

PostgreSQL hosts give concurrent actions and HTTP requests separate engines and transactions. A chain such as `A.action → B.action → A.lookup` can complete while the first request on A is still active. Requests that contend for the same database locks can still wait for each other. SQLite and memory hosts serialize operations on their shared connection.

## Registry

The plugin creates `_chimpbase_mesh_nodes` on start:

```sql
CREATE TABLE IF NOT EXISTS _chimpbase_mesh_nodes (
  node_id            TEXT PRIMARY KEY,
  advertised_url     TEXT,
  metadata_json      TEXT,
  services_json      TEXT,
  started_at_ms      BIGINT,
  last_heartbeat_ms  BIGINT
);
```

- **Heartbeat** — `setInterval` updates `last_heartbeat_ms` and reloads live peers from the registry. PostgreSQL heartbeat queries use a separate adapter after startup, so active request transactions cannot hide or roll back them. Slow heartbeats do not overlap, and shutdown waits for the active heartbeat.
- **Announce / leave** — emitted via `ctx.pubsub.publish` on plugin start/stop. Shutdown rejects new HTTP routes with 503, stops event listeners and worker ticks, and waits for active operations before deleting this node's registry row.
- **Cache** — every node replaces its in-memory peer snapshot from the registry on startup and each heartbeat. Announce/leave events can update it between refreshes; peers past `offlineAfterMs` expire locally.
- **GC** — cron `* * * * *` sweeps rows older than `gcAfterMs`.

## Balanced events

To process an event through a shared queue rather than broadcast it to every node, declare it with `balanced: true`:

```ts
events: {
  "order.paid": {
    balanced: true,
    handler: async (ctx, payload) => { /* handle a claimed job */ },
  },
}
```

`ctx.mesh.emit("order.paid", p, { balanced: true })` enqueues a job on `__chimpbase.mesh.balanced.order.paid`. The producer does not need a local handler; only nodes with the balanced worker registered can claim the job. PostgreSQL coordinates claims across the cluster. Failed attempts can retry, so external effects such as email or payments must tolerate repeated execution. See [Workers & Queues](/workers) for transaction and retry guarantees.

Broadcast events use pubsub, with mesh subscriptions overriding delivery to `sync` even when the host uses `subscriptions.dispatch: "async"`. Each subscribed node processes the event through its event listener; ordinary subscriptions on the same event retain their configured dispatch mode. Broadcasts require nodes to be listening and do not provide the durable queue guarantees of balanced events.

## HTTP RPC

When `transport: "http"`, the plugin registers:

- Route `POST /__chimpbase/mesh/rpc` — validates the `x-chimpbase-mesh-token` header (timing-safe compare against `ctx.secret(meshToken)`) and forwards to the target action.
- Action `__chimpbase.mesh.rpc.execute` — the per-call dispatch invoked by the RPC route.

### Interaction with `chimpbase/auth`

Register `chimpbaseMesh` **before** `chimpbaseAuth` so the mesh route short-circuits its own path before the auth guard fires. Otherwise `/__chimpbase/mesh/rpc` will return 401 from auth.

## Versioning

`service({ version: 2 })` prefixes actions with `v2.{name}.`. Multiple versions can coexist in the same plugin — peers see all prefixed names.

## Troubleshooting

- **`MeshNoAvailableNodeError`** — no peer has advertised the action. Verify both nodes registered the same service and that the registry table has rows for both `node_id`s.
- **`MeshTimeoutError`** — increase `defaultTimeoutMs` or per-call `timeoutMs`. Confirm the peer's `advertised_url` is reachable from this node (NAT/container networking).
- **`unauthorized mesh rpc`** — check that both nodes resolve the same value via `ctx.secret(meshToken)`.
- **Peers missing after restart** — heartbeat interval × 2 > `offlineAfterMs`, so the window matters. Peers re-announce on `onStart`.
