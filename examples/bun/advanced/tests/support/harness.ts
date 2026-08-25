import { createChimpbase } from "@chimpbase/bun";

import app from "../../chimpbase.app.ts";

export const TEST_API_KEY = "test-bootstrap-key";
export const TEST_AUTH_HEADERS = { "X-API-Key": TEST_API_KEY } as const;

const SECRETS = new Map<string, string>([
  ["CHIMPBASE_BOOTSTRAP_API_KEY", TEST_API_KEY],
]);

export async function bootAdvanced(): Promise<{
  host: Awaited<ReturnType<typeof createChimpbase>>;
  started: Awaited<ReturnType<Awaited<ReturnType<typeof createChimpbase>>["start"]>>;
  baseUrl: string;
}> {
  const host = await createChimpbase({
    ...app,
    storage: { engine: "memory" },
    server: { port: 0 },
    subscriptions: { dispatch: "sync" },
    secrets: { get: (name: string) => SECRETS.get(name) ?? null },
  });
  const started = await host.start();
  const port = started.server?.port;
  if (!(port !== undefined && port > 0)) throw new Error("server failed to bind a port");
  return { host, started, baseUrl: `http://127.0.0.1:${port}` };
}

export async function authedPost(url: string, body: unknown): Promise<Response> {
  return await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json", ...TEST_AUTH_HEADERS },
    body: JSON.stringify(body),
  });
}

export async function authedGet(url: string): Promise<Response> {
  return await fetch(url, { headers: TEST_AUTH_HEADERS });
}
