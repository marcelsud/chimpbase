# Core internals

`chimpbase/core` exposes the execution engine, registration contracts, app-definition types and host-facing internals.

Application code normally uses `chimpbase/runtime` and a runtime adapter. Import from `chimpbase/core` when you need app-definition types or low-level integration contracts.

- [Getting Started](../../docs/getting-started.md) shows an app definition.
- [Configuration](../../docs/configuration.md) describes application and runtime settings.
- [Custom storage adapters](../../docs/advanced/storage-adapters.md) describes low-level integration contracts.

This directory is a private workspace implementation package. The public `chimpbase` package ships compiled JavaScript and TypeScript declarations.
