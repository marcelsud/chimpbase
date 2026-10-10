# Deployment

Start with one Chimpbase process for HTTP, workers, cron, and subscriptions. Use PostgreSQL when multiple processes need to share durable state and coordinate jobs.

## App entry point

Your `chimpbase.app.ts` exports the app definition. Create a separate entry point that starts it:

```ts
// app.ts
import { createChimpbase } from "chimpbase/runtime/bun";
import app from "./chimpbase.app.ts";

const chimpbase = await createChimpbase({
  ...app,
  projectDir: import.meta.dir,
  storage: { engine: "postgres", url: process.env.DATABASE_URL },
});

// APP_ROLE is an environment variable used by this entry point.
const role = process.env.APP_ROLE ?? "all";
await chimpbase.start({
  serve: role !== "worker",
  runWorker: role !== "api",
});
```

With no `APP_ROLE`, the process serves HTTP and runs background work. Set `APP_ROLE=api` to disable queue processing, or `APP_ROLE=worker` to disable HTTP. Lifecycle hooks and the event bus still start in both roles.

For Node, import from `chimpbase/runtime/node` and use your TypeScript runner or compiled entry point.

## Container

Install `chimpbase` in your application, then build a container:

```dockerfile
FROM oven/bun:1
WORKDIR /app
COPY . .
RUN bun install --production
CMD ["bun", "run", "app.ts"]
```

Exclude `node_modules`, local database files, and secrets from the build context with `.dockerignore`.

## Single process

```yaml
# docker-compose.yml
services:
  postgres:
    image: postgres:17
    environment:
      POSTGRES_DB: myapp
      POSTGRES_USER: myapp
      POSTGRES_PASSWORD: secret
    volumes:
      - pgdata:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U myapp -d myapp"]
      interval: 5s
      timeout: 5s
      retries: 10

  app:
    build: .
    environment:
      DATABASE_URL: postgres://myapp:secret@postgres:5432/myapp
    ports:
      - "3000:3000"
    depends_on:
      postgres:
        condition: service_healthy

volumes:
  pgdata:
```

Use deployment secrets for production credentials. This container serves HTTP, processes background jobs, and runs scheduled work.

## Separate API and workers

Keep the same database and image. Replace the `app` service with:

```yaml
  api:
    build: .
    environment:
      DATABASE_URL: postgres://myapp:secret@postgres:5432/myapp
      APP_ROLE: api
    ports:
      - "3000:3000"
    depends_on:
      postgres:
        condition: service_healthy

  worker:
    build: .
    environment:
      DATABASE_URL: postgres://myapp:secret@postgres:5432/myapp
      APP_ROLE: worker
      CHIMPBASE_WORKER_CONCURRENCY: 10
    depends_on:
      postgres:
        condition: service_healthy
```

Scale the worker service with `docker compose up --build --scale worker=3`. Each replica loads the same registrations and claims jobs from the shared PostgreSQL queue. See the [environment-variable reference](/configuration#environment-variables) for runtime settings and defaults.

## Coordination and retries

- Queue claims use row locks; failed handlers can retry. External effects such as email and payments need idempotency. See [Workers & Queues](/workers).
- Cron slots are claimed through PostgreSQL.
- Subscriptions with `idempotent: true` deduplicate committed handling across processes.
- Workflows persist their state so another worker can resume them.

## Working example

The [advanced Bun example](https://github.com/chimpbase/chimpbase/tree/main/examples/bun/advanced) demonstrates multiple replicas with shared PostgreSQL, authentication, storage, plugins, and telemetry.
