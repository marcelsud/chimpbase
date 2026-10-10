# Collections

Collections provide schemaless document storage. Documents are JSON objects with an auto-generated `id` field, stored in the framework's internal `_chimpbase_collections` table. No migrations needed.

## Insert

```ts
const id = await ctx.collection.insert("notes", {
  body: "First note",
  todoId: 42,
  createdAt: new Date().toISOString(),
});
// id: "550e8400-e29b-41d4-a716-446655440000"
```

Returns the generated document ID (UUID).

## Find

```ts
// Find with filter
const notes = await ctx.collection.find("notes", { todoId: 42 });

// Find with limit
const recent = await ctx.collection.find("notes", {}, { limit: 10 });
```

Filters compare top-level fields with strict equality. Strings, numbers, booleans and `null` match their JSON values; `undefined` matches a missing field. Objects and arrays are not compared structurally. Results are ordered by document ID. PostgreSQL applies filters and limits in the database.

## Find One

```ts
const note = await ctx.collection.findOne("notes", { id: "550e8400-..." });
// returns the document or null
```

## Update

```ts
const updated = await ctx.collection.update(
  "notes",
  { id: "550e8400-..." },
  { body: "Updated note" },
);
// returns number of documents updated
```

Patches replace top-level fields; nested objects are replaced rather than merged. Setting a field to `undefined` removes it from the stored JSON. PostgreSQL merges each patch atomically, so concurrent patches to different fields preserve both changes.

## Delete

```ts
const deleted = await ctx.collection.delete("notes", { id: "550e8400-..." });
// returns number of documents deleted

// Delete all in a collection
const deletedAll = await ctx.collection.delete("notes");
```

## List Collections

```ts
const names = await ctx.collection.list();
// ["notes", "snapshots", "preferences"]
```

## When to Use Collections vs. SQL

**Use collections when:**
- You need flexible document storage without migrations
- Schema evolves frequently
- You're building plugins that need generic storage

**Use `ctx.db.query()` when:**
- You need relational queries (joins, aggregations)
- You need indexes for performance
- Your data has a fixed schema

## REST Collections Plugin

To expose collections as REST APIs without writing routes, see the separate [REST Collections](/advanced/rest-collections) guide.
