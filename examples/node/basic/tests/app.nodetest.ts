import { after, before, describe, test } from "node:test";
import { strict as assert } from "node:assert";
import { createServer } from "node:net";

import { createChimpbase } from "@chimpbase/node";

import app from "../chimpbase.app.ts";

type Host = Awaited<ReturnType<typeof createChimpbase>>;
type StartedHost = Awaited<ReturnType<Host["start"]>>;

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

describe("node/basic example", () => {
  let host: Host;
  let started: StartedHost;
  let baseUrl: string;

  before(async () => {
    const port = await reservePort();
    host = await createChimpbase({
      ...app,
      storage: { engine: "memory" },
      server: { port },
    });
    started = await host.start();
    baseUrl = `http://127.0.0.1:${port}`;
  });

  after(async () => {
    await started.stop();
  });

  test("creates and lists orders over HTTP", async () => {
    const created = await fetch(`${baseUrl}/orders`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ customer: "alice@example.com", amount: 4200 }),
    });
    assert.equal(created.status, 201);
    const createdBody = (await created.json()) as {
      id: number;
      customer: string;
      amount: number;
    };
    assert.equal(createdBody.customer, "alice@example.com");
    assert.equal(createdBody.amount, 4200);

    const list = await fetch(`${baseUrl}/orders`);
    assert.equal(list.status, 200);
    const rows = (await list.json()) as Array<{ customer: string; amount: number }>;
    assert.equal(rows.length, 1);
    assert.equal(rows[0].customer, "alice@example.com");
  });

  test("built-in health check responds", async () => {
    const res = await fetch(`${baseUrl}/health`);
    assert.equal(res.status, 200);
    assert.deepEqual(await res.json() as { ok: boolean }, { ok: true });
  });
});
