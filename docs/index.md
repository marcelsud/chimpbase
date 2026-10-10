---
layout: home
hero:
  name: Chimpbase
  text: Build backends with fewer moving parts.
  tagline: Actions, background jobs and schedules in one runtime. Start locally with SQLite; use PostgreSQL when processes need to coordinate.
  actions:
    - theme: brand
      text: Get Started
      link: /getting-started
    - theme: alt
      text: GitHub
      link: https://github.com/chimpbase/chimpbase
features:
  - title: Actions
    details: Validated business operations called from HTTP, CLI or other actions.
  - title: Background jobs
    details: Queues, workers and retries backed by your database.
  - title: Schedules
    details: Recurring work through the same runtime and storage.
---

## Start small

Install one package:

```bash
bun add chimpbase
```

Follow [Getting Started](/getting-started) to build and run a notes API without a database server or HTTP framework.

The main guide covers actions, routes, storage and background work. [Advanced guides](/advanced/) cover workflows, integrations, plugins and deployment when you need them.
