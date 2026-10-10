# KV Store

A simple key-value store for application state, feature flags, counters, and caching. Supports TTL for automatic expiration.

## Set

```ts
await ctx.kv.set("workspace.theme", "dark");
```

### With TTL

```ts
await ctx.kv.set("session:abc123", { userId: 42 }, { ttlMs: 3_600_000 }); // 1 hour
```

Expired keys are invisible immediately on read — no need to wait for cleanup.

### Set if absent

```ts
const reserved = await ctx.kv.setIfAbsent("delivery:abc123", true, { ttlMs: 86_400_000 });
if (!reserved) return;
```

Returns `true` only when the key was absent or expired. The write is atomic across hosts sharing a database and participates in the current transaction: rollback releases the reservation. An existing live key keeps its value and expiration.

## Get

```ts
import { v } from "@chimpbase/runtime";

const theme = await ctx.kv.get("workspace.theme", v.string());
// "dark" or null if not set / expired
```

## Delete

```ts
await ctx.kv.delete("workspace.theme");
```

## List Keys

```ts
// All keys
const keys = await ctx.kv.list();

// Keys with prefix
const workspaceKeys = await ctx.kv.list({ prefix: "workspace." });
// ["workspace.theme", "workspace.language", "workspace.timezone"]
```

## Common Patterns

### Feature flags

```ts
await ctx.kv.set("feature:dark-mode", true);

const darkMode = await ctx.kv.get("feature:dark-mode", v.boolean());
if (darkMode) {
  // ...
}
```

### Rate limiting / counters

```ts
const key = `ratelimit:${userId}`;
const current = await ctx.kv.get(key, v.number()) ?? 0;
await ctx.kv.set(key, current + 1, { ttlMs: 60_000 }); // reset after 1 min
```

### Caching

```ts
const cacheKey = `cache:dashboard:${projectId}`;
const cached = await ctx.kv.get(cacheKey, dashboardDataValidator);
if (cached) return cached;

const data = await computeDashboard(ctx, projectId);
await ctx.kv.set(cacheKey, data, { ttlMs: 300_000 }); // 5 min TTL
return data;
```

## TTL Cleanup

Expired keys are automatically filtered on read. For storage reclamation, an optional cleanup cron can be enabled in the project configuration.
