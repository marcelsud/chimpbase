# Chimpbase Cloud

::: warning Coming soon
Chimpbase Cloud is **not yet released**. This page describes what's being built. No sign-up, no SLA, no API keys — yet. Star [github.com/chimpbase/chimpbase](https://github.com/chimpbase/chimpbase) or check back for launch.
:::

**Chimpbase Cloud** is the hosted home for apps built on Chimpbase. Push code, get a URL. Managed Postgres, blobs, custom domains, logs, metrics, sleep-after-idle — all the pieces you'd otherwise wire up yourself.

Every customer app that runs on the cloud is orchestrated by [`@chimpbase/deployer`](https://github.com/chimpbase/deployer), which is already open source and usable standalone today. The cloud adds the control plane, dashboard, and managed services on top.

## What's coming

- **Postgres** — managed database with backups. Connection string injected as `CHIMPBASE_DATABASE_URL`; `chimpbase/postgres` picks it up automatically.
- **Blob storage** — R2-backed bucket per tenant, wired through `chimpbase/blobs` so customer code uses `ctx.blobs`.
- **Domains + TLS** — default wildcard `*.chimpbase.dev` + automatic Let's Encrypt for custom domains (`api.yourapp.com` → CNAME → `edge.chimpbase.dev`).
- **Observability** — logs + metrics in the dashboard. OpenTelemetry sink for traces.
- **Idle sleep** — apps sleep after inactivity and wake on the next request.
- **Secrets** — `chimpctl secrets set NAME VALUE`, AES-256-GCM envelope-encrypted at rest, rotated via the same command.
- **Builds** — `git push` or `chimpctl deploy` → rootless kaniko build → private registry → deploy. Supports Bun, Node, and Deno out of the box; buildpacks for everything else.

## How you'll deploy

```bash
chimpctl login       # GitHub device flow
chimpctl init        # scaffold chimpbase.config.ts
chimpctl deploy      # bundle, build, deploy — done
```

See the [CLI reference](/advanced/cli).

## Self-hosting today

You don't have to wait for the cloud. [`@chimpbase/deployer`](https://github.com/chimpbase/deployer) already orchestrates Chimpbase apps across Docker Swarm or Kubernetes (k3s tested) clusters you operate yourself. Same primitives, same HTTP API, no tenant gatekeeping.

```bash
git clone https://github.com/chimpbase/deployer
cd deployer
./hack/dev-up.sh   # 3-node Swarm in Multipass VMs
bun run dev        # deployer control plane on localhost:3000
```

Once Chimpbase Cloud launches, existing deployer users can either keep self-hosting or migrate with minimal config changes — the deployer will still be first-party and will continue to work without the cloud.

## Staying in the loop

- GitHub: [chimpbase/chimpbase](https://github.com/chimpbase/chimpbase), [chimpbase/deployer](https://github.com/chimpbase/deployer), [chimpbase/chimpctl](https://github.com/chimpbase/chimpctl)
- Launch announcement will land here and in the CHANGELOGs.
