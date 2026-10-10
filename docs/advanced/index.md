# Advanced Guides

Start with [Getting Started](/getting-started). These guides cover capabilities you can add when your application needs them.

## Application structure

- [App composition](/advanced/app-composition) — split registrations into files or use a custom entry point.
- [Workflows](/advanced/workflows) — processes that survive delays, retries and restarts.
- [Business modules](/advanced/modules) — ownership boundaries and versioned interfaces.

## Storage and observability

- [State and storage](/advanced/state) — runtime storage and migrations.
- [Custom adapters](/advanced/storage-adapters) — implement a storage engine contract.
- [KV](/advanced/kv), [blobs](/advanced/blobs) and [streams](/advanced/streams) — specialized storage APIs.
- [Telemetry](/advanced/telemetry) — persistence and external sinks.

## Integrations

- Frameworks: [Hono](/advanced/hono), [NestJS](/advanced/nestjs), [Express](/advanced/express) and [Next.js](/advanced/nextjs).
- Plugins: [auth](/advanced/auth), [webhooks](/advanced/webhooks), [REST collections](/advanced/rest-collections) and [mesh](/advanced/mesh).
- [Custom plugins](/advanced/plugins) and [event delivery](/advanced/event-bus) — extend the runtime and configure transports.
- [Contract testing](/advanced/pact) — verify action consumers.

## Deployment

- [Self-hosting](/advanced/deployment) — PostgreSQL and multiple replicas.
- [Chimpbase Cloud](/advanced/cloud) and [cloud CLI](/advanced/cli) — hosted deployment.

## Examples

The repository keeps complete examples separate from the main guide. Start with a basic app, then use the next level when you need its features:

| Level | Bun | Node | Deno |
|-------|-----|------|------|
| Basic: actions and routes | [Source](https://github.com/chimpbase/chimpbase/tree/main/examples/bun/basic) | [Source](https://github.com/chimpbase/chimpbase/tree/main/examples/node/basic) | [Source](https://github.com/chimpbase/chimpbase/tree/main/examples/deno/basic) |
| Intermediate: workers and cron | [Source](https://github.com/chimpbase/chimpbase/tree/main/examples/bun/intermediate) | [Source](https://github.com/chimpbase/chimpbase/tree/main/examples/node/intermediate) | [Source](https://github.com/chimpbase/chimpbase/tree/main/examples/deno/intermediate) |
| Advanced: workflows and plugins | [Source](https://github.com/chimpbase/chimpbase/tree/main/examples/bun/advanced) | [Source](https://github.com/chimpbase/chimpbase/tree/main/examples/node/advanced) | [Source](https://github.com/chimpbase/chimpbase/tree/main/examples/deno/advanced) |
