# App Composition

Split an application into files when the single-file [Getting Started](/getting-started) app becomes hard to navigate. Keep `chimpbase.app.ts` as the composition root.

## Register imported primitives

Export named actions and routes from your own modules, then collect them in the app definition:

```ts
import type { ChimpbaseAppDefinitionInput } from "chimpbase/core";
import { createNote, listNotes } from "./src/notes/actions.ts";
import { notesRoute } from "./src/notes/routes.ts";
import { notifyNoteCreated } from "./src/notes/worker.ts";

export default {
  project: { name: "my-app" },
  registrations: [
    createNote,
    listNotes,
    notesRoute,
    notifyNoteCreated,
  ],
} satisfies ChimpbaseAppDefinitionInput;
```

Start it from the project directory:

```bash
bunx chimpbase dev
```

For actions in imported modules, specify `name` in `action({ name, ... })` so CLI and durable references stay stable. The project loader only infers names for unnamed actions exported directly from `chimpbase.app.ts`. Alternatively, register actions programmatically with `host.register({ createNote, listNotes })` to use the object keys as names.

## Application and runtime settings

The app definition contains registrations, migrations, optional `httpHandler`, worker retry settings, telemetry settings and workflow contracts. See [Configuration](/configuration) for the common fields and CLI commands.

Runtime settings such as storage, server port, worker concurrency, secrets, blob drivers and telemetry sinks belong to the host options. When you need those options in code, use a custom entry point.

## Custom entry point

Save this as `app.ts` beside `chimpbase.app.ts`:

```ts
import { fileURLToPath } from "node:url";
import { startChimpbaseApp } from "chimpbase/runtime/bun";
import app from "./chimpbase.app.ts";

const started = await startChimpbaseApp({
  app,
  projectDir: fileURLToPath(new URL(".", import.meta.url)),
  storage: { engine: "sqlite" },
});

// Your shutdown handler should await started.stop().
```

Run `bun app.ts`. `startChimpbaseApp` loads the definition and starts HTTP plus the worker. Its `stop()` stops the runtime and closes storage and telemetry sinks.

Select `chimpbase/runtime/node` or `chimpbase/runtime/deno` for another host. The application registrations remain portable.

To start only one role, add these host options:

| Options | Role |
|---------|------|
| `serve: true, runWorker: false` | HTTP only |
| `serve: false, runWorker: true` | Worker only |

Use PostgreSQL when HTTP and workers run in separate processes.

## Further organization

[Business modules](/advanced/modules) add ownership boundaries and versioned public interfaces. They are useful when ordinary file composition no longer provides enough separation.
