# Runtime DSL

Import portable application primitives and context types from `chimpbase/runtime` (also available at the `chimpbase` root). Runtime adapters are selected separately; see [Configuration](../../docs/configuration.md).

- [Getting Started](../../docs/getting-started.md) is the complete runnable example.
- [Actions](../../docs/actions.md), [routes](../../docs/routes.md), [subscriptions](../../docs/subscriptions.md), [workers](../../docs/workers.md) and [cron](../../docs/cron.md) document the main primitives.
- [Advanced guides](../../docs/advanced/index.md) cover workflows, plugins and other integrations.

Actions can call other action references inside an active runtime scope. Use explicit action names or `host.register({ actionName })` for stable registration identifiers; [app composition](../../docs/advanced/app-composition.md) explains name inference.

This directory is a private workspace implementation package. Published consumers receive compiled JavaScript and TypeScript declarations through `chimpbase/runtime`.
