# Streams

Streams are append-only event logs. Use them for activity feeds, audit trails, change data capture, or any scenario where you need an ordered sequence of events.

## Append

```ts
const eventId = await ctx.stream.append("todo.activity", "todo.created", {
  todoId: 42,
  title: "Ship new feature",
  assignee: "alice@example.com",
});
// eventId: 1 (sequential)
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `stream` | `string` | Stream name |
| `event` | `string` | Event type/name |
| `payload` | `unknown` | Event data (serialized as JSON) |

Returns the sequential event ID.

## Read

```ts
const events = await ctx.stream.read("todo.activity");
```

### With options

```ts
// Read with limit
const recent = await ctx.stream.read("todo.activity", { limit: 50 });

// Read events after a specific ID (pagination)
const newer = await ctx.stream.read("todo.activity", {
  sinceId: lastSeenId,
  limit: 100,
});
```

### Event shape

```ts
interface ChimpbaseStreamEvent<TPayload> {
  id: number;          // sequential event ID
  stream: string;      // stream name
  event: string;       // event type
  payload: TPayload;   // event data
  createdAt: string;   // ISO 8601 timestamp
}
```

## Common Patterns

### Activity feed

```ts
// Write
subscription("todo.created", async (ctx, todo) => {
  await ctx.stream.append("todo.activity", "todo.created", {
    todoId: todo.id,
    title: todo.title,
  });
}, { idempotent: true, name: "streamTodoCreated" });

// Read
const listActivity = action("listActivity", async (ctx, input: { sinceId?: number }) => {
  return await ctx.stream.read("todo.activity", {
    sinceId: input.sinceId,
    limit: 50,
  });
});
```

### Audit trail

```ts
await ctx.stream.append("audit", "user.login", {
  userId: user.id,
  ip: request.headers.get("x-forwarded-for"),
  timestamp: new Date().toISOString(),
});
```

## Streams vs. Subscriptions

| | Streams | Subscriptions |
|--|---------|---------------|
| **Purpose** | Persistent event log | Reactive event handler |
| **Storage** | Events stored permanently | Events are transient |
| **Reading** | Pull-based (read on demand) | Push-based (handler invoked) |
| **Use case** | Audit trail, activity feed | Trigger side effects |
