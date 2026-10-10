# chimpctl

`chimpctl` is the customer CLI for [Chimpbase Cloud](https://chimpbase.dev). It talks to the cloud control plane over HTTP to create projects, deploy services, and manage api keys from your terminal.

Distributed as a single compiled binary (`bun build --compile`). No runtime dependency on Bun or Node after install.

Source: [github.com/chimpbase/chimpctl](https://github.com/chimpbase/chimpctl).

## Install

::: code-group

```bash [script]
curl -fsSL https://chimpbase.dev/install.sh | sh
```

```bash [bun]
bun add -g chimpctl
```

```bash [npm]
npm i -g chimpctl
```

:::

Or grab a pinned binary from the [GitHub Releases](https://github.com/chimpbase/chimpctl/releases) page (linux-x64, linux-arm64, darwin-x64, darwin-arm64).

## Quickstart

```bash
chimpctl login --api-url https://api.chimpbase.dev --api-key cbk_...
chimpctl projects create "My App"
chimpctl deploy --image nginx:1.27 --name web --port 80 --project my-app
chimpctl services list
```

## Commands (v0)

| Command | What it does |
|---|---|
| `chimpctl login --api-url <url> --api-key <cbk_...>` | Verify the key, persist `{apiUrl, apiKey, userId, accountId}` to `~/.config/chimpbase/auth.json` (mode 0600). |
| `chimpctl logout` | Remove the stored auth file. |
| `chimpctl whoami` | Print account + user + api url. |
| `chimpctl projects list` | List projects in the current account. |
| `chimpctl projects create <name>` | Create a project. Slug + subdomain are derived from the name. |
| `chimpctl deploy --image <img> --name <svc> [--project <slug\|id>] [--port <N>] [--replicas N]` | Create a service in a project and schedule it. Backend allocates a published port (3200–3999) and returns its URL. |
| `chimpctl services list [--project <slug\|id>]` | List services, optionally filtered by project. |
| `chimpctl services rm <id>` | Remove a service (best-effort teardown of the underlying deployer service). |
| `chimpctl help` | Usage. Also `--help`, `-h`. |
| `chimpctl version` | Version. Also `--version`, `-v`. |

### Not yet shipped

- Interactive login (GitHub device flow). For now `login` takes `--api-url`/`--api-key` flags.
- `chimpctl logs`, `env`, `secrets`, `domains`, `scale`, `rollback`, `status`, `open`, `init` — plumbing work in the backend first.

## Configuration

Per-user config lives at `~/.config/chimpbase/auth.json`:
```json
{
  "apiUrl": "https://api.chimpbase.dev",
  "apiKey": "cbk_...",
  "userId": "usr_...",
  "accountId": "acc_..."
}
```
Mode `0600`. CI-friendly override via env: `CHIMPBASE_API_URL` + `CHIMPBASE_API_KEY`.

`XDG_CONFIG_HOME` is honored — set it to keep per-project demo state out of your real dotfile.

## Error UX

- `401 Not authenticated. run \`chimpctl login\`.` — stored credentials missing or rejected.
- `404 Not found (or not yours).` — resource doesn't exist OR belongs to a different tenant. The CLI deliberately does not distinguish.
- Other 4xx errors print the server's `{"error": "..."}` message verbatim.

## See also

- Self-hosting the orchestrator without Chimpbase Cloud: [`@chimpbase/deployer`](https://github.com/chimpbase/deployer).
- Cloud roadmap + architecture: [cloud page](/advanced/cloud).
