import { createServer } from "node:net";

import { createChimpbase } from "@chimpbase/node";

import app from "../../chimpbase.app.ts";

export const TEST_API_KEY = "test-bootstrap-key";
export const TEST_AUTH_HEADERS = { "X-API-Key": TEST_API_KEY } as const;

const SECRETS = new Map<string, string>([
  ["CHIMPBASE_BOOTSTRAP_API_KEY", TEST_API_KEY],
]);

async function reservePort(): Promise<number> {
  return await new Promise((resolveFn, rejectFn) => {
    const server = createServer();
    server.once("error", rejectFn);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      if (address === null || typeof address === "string") {
        server.close(() => rejectFn(new Error("no port")));
        return;
      }
      const { port } = address;
      server.close((error) => ((error !== undefined) ? rejectFn(error) : resolveFn(port)));
    });
  });
}

export async function bootAdvanced(): Promise<{
  host: Awaited<ReturnType<typeof createChimpbase>>;
  started: Awaited<ReturnType<Awaited<ReturnType<typeof createChimpbase>>["start"]>>;
  baseUrl: string;
}> {
  const port = await reservePort();
  const host = await createChimpbase({
    ...app,
    storage: { engine: "memory" },
    server: { port },
    subscriptions: { dispatch: "sync" },
    secrets: { get: (name: string) => SECRETS.get(name) ?? null },
  });
  const started = await host.start();
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
