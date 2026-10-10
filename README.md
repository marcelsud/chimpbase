# Chimpbase

Backend actions, queues and schedules in one TypeScript runtime. Use PostgreSQL for shared application and coordination state; use SQLite or memory for local development and tests.

## Get started

Install the single public package:

```bash
bun add chimpbase
```

Save this as `chimpbase.app.ts`:

```ts
import { action, route } from "chimpbase/runtime";

const listNotes = action({
  name: "listNotes",
  async handler(ctx) {
    return await ctx.collection.find("notes");
  },
});

const notes = route("GET", "/notes", async (_request, env) => {
  return Response.json(await env.action(listNotes));
});

export default {
  project: { name: "my-app" },
  registrations: [listNotes, notes],
};
```

Run it with:

```bash
bunx chimpbase dev
```

The CLI loads `chimpbase.app.ts`, starts HTTP on port 3000 and runs the background worker. The app uses local SQLite and needs no database server or HTTP framework.

Call `GET /notes` to list the stored notes:

```bash
curl http://localhost:3000/notes
```

Follow [Getting Started](docs/getting-started.md) to add an action and a `POST /notes` route for creating notes.

## Build your backend

- [Actions](docs/actions.md) validate inputs and run business operations in transactions.
- [HTTP routes](docs/routes.md) expose actions with standard `Request` and `Response` objects.
- [Collections](docs/collections.md) store JSON documents; [database access](docs/database.md) supports SQL and Kysely.
- [Subscriptions](docs/subscriptions.md) react to events; [workers](docs/workers.md) process queued jobs with retries.
- [Cron](docs/cron.md) runs recurring jobs.

See [Configuration](docs/configuration.md) for storage, CLI commands and runtime hosts. Application code imports the portable DSL from `chimpbase/runtime` and selects a host only when starting a runtime:

| Host | Import |
|------|--------|
| Bun | `chimpbase/runtime/bun` |
| Node | `chimpbase/runtime/node` |
| Deno | `chimpbase/runtime/deno` |

`chimpbase` and `chimpbase/runtime` expose the same portable DSL. They do not select a host automatically. Public imports are subpaths of `chimpbase`; the scoped repository workspaces are private implementation packages.

## Examples and advanced guides

Start with the smallest runnable example for your host: [Bun](examples/bun/basic), [Node](examples/node/basic), or [Deno](examples/deno/basic).

[Advanced guides](docs/advanced/index.md) collect workflows, business modules, plugins, framework integrations and deployment recipes. The intermediate and advanced examples live under each host's example directory.

## Contributing

Install workspace dependencies once at the repository root:

```bash
bun install
bun run dev:bun:basic
```

Run the repository checks with `bun run ci:check`. Build the documentation with `bun run docs:build`.

