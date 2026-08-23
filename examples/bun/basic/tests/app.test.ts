import { afterEach, beforeEach, describe, expect, test } from "bun:test";

import { createChimpbase } from "@chimpbase/bun";

import app from "../chimpbase.app.ts";

type StartedHost = Awaited<ReturnType<Awaited<ReturnType<typeof createChimpbase>>["start"]>>;

describe("bun/basic example", () => {
  let started: StartedHost;
  let baseUrl: string;

  beforeEach(async () => {
    const chimpbase = await createChimpbase({
      ...app,
      storage: { engine: "memory" },
      server: { port: 0 },
    });
    started = await chimpbase.start();
    const port = started.server?.port;
    if (!port) throw new Error("server failed to bind a port");
    baseUrl = `http://127.0.0.1:${port}`;
  });

  afterEach(async () => {
    await started.stop();
  });

  test("creates and lists orders over HTTP", async () => {
    const created = await fetch(`${baseUrl}/orders`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ customer: "alice@example.com", amount: 4200 }),
    });
    expect(created.status).toBe(201);
    const createdBody = (await created.json()) as { id: number; customer: string; amount: number };
    expect(createdBody).toMatchObject({ customer: "alice@example.com", amount: 4200 });

    const list = await fetch(`${baseUrl}/orders`);
    expect(list.status).toBe(200);
    const rows = (await list.json()) as Array<{ customer: string; amount: number }>;
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({ customer: "alice@example.com", amount: 4200 });
  });

  test("built-in health check responds", async () => {
    const res = await fetch(`${baseUrl}/health`);
    expect(res.status).toBe(200);
    expect(await res.json() as { ok: boolean }).toEqual({ ok: true });
  });
});
