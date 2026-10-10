# REST Collections

The `@chimpbase/rest-collections` plugin exposes Chimpbase collections as REST APIs without writing route handlers.

## Installation

```bash
bun add @chimpbase/rest-collections
```

## Quick Start

```ts
import { restCollections } from "@chimpbase/rest-collections";

const rest = restCollections({
  basePath: "/api",
  collections: {
    notes: {
      collection: "todo_notes",
      writableFields: ["body", "todoId"],
      filterableFields: { todoId: "number" },
    },
  },
});
```

This creates:

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/notes` | List notes |
| `GET` | `/api/notes/:id` | Get a note |
| `POST` | `/api/notes` | Create a note |
| `PATCH` | `/api/notes/:id` | Update a note |
| `DELETE` | `/api/notes/:id` | Delete a note |

## Configuration

```ts
restCollections({
  // URL prefix for all collection routes
  basePath: "/api",

  // Default pagination limit
  defaultLimit: 50,

  // Maximum pagination limit
  maxLimit: 100,

  collections: {
    collectionKey: {
      // Actual collection name in storage (defaults to key)
      collection: "my_collection",

      // URL path segment (defaults to key)
      path: "/my-items",

      // Which HTTP methods to expose
      methods: ["list", "get", "create", "update", "delete"],

      // Fields that can be written via POST/PATCH
      writableFields: ["title", "body", "status"],

      // Fields that can be used as query filters
      filterableFields: ["status", "priority"],
      // or with type parsing:
      filterableFields: {
        status: "string",
        priority: "string",
        todoId: "number",
        active: "boolean",
      },

      // Per-collection pagination
      defaultLimit: 20,
      maxLimit: 50,

      // Transform documents on read
      onRead: (context) => {
        return { ...context.document, displayName: context.document.name.toUpperCase() };
      },

      // Transform documents on write
      onWrite: (context) => {
        return { ...context.input, updatedAt: new Date().toISOString() };
      },
    },
  },
});
```

## Querying

### Filtering

```
GET /api/notes?todoId=42&status=active
```

Only fields listed in `filterableFields` can be used as filters.

### Pagination

```
GET /api/notes?limit=20
```

## Schema Versioning

Collections support schema versioning for data migrations:

```ts
restCollections({
  collections: {
    notes: {
      schemaVersion: 2,
      onRead: (context) => {
        if (context.schemaVersion === 1) {
          // migrate v1 document shape on read
          return { ...context.document, body: context.document.content };
        }
        return context.document;
      },
    },
  },
});
```

Versions are stored in collection metadata; legacy documents can still provide a `schemaVersion` field. Metadata is read only when a transform needs it. A list with `onRead` fetches the collection's metadata in one batch; a list without `onRead` skips that read.

## Registration

```ts
export default {
  registrations: [
    restCollections({ basePath: "/api", collections: { /* ... */ } }),
  ],
};
```
