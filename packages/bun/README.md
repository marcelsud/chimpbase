# Bun host

The Bun adapter is part of the single public `chimpbase` package.

```bash
bun add chimpbase
bunx chimpbase dev
```

The CLI loads `chimpbase.app.ts`, starts HTTP with `Bun.serve()` and runs the background worker. Follow [Getting Started](../../docs/getting-started.md) for the complete app.

For programmatic startup, import from `chimpbase/runtime/bun`. Portable actions, routes and other primitives come from `chimpbase/runtime`.

- [Configuration](../../docs/configuration.md) covers storage, environment variables and CLI commands.
- [App composition](../../docs/advanced/app-composition.md) covers a custom entry point and separate HTTP/worker roles.
- [Telemetry](../../docs/advanced/telemetry.md) covers buffered records, persistence and external sinks. Handler logs are not automatically printed to stdout.
- [Basic Bun example](../../examples/bun/basic) is the smallest repository app. Install workspace dependencies once at the repository root.

This directory is a private workspace implementation package. Published consumers receive compiled JavaScript and TypeScript declarations through `chimpbase/runtime/bun`.
