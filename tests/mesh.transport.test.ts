import { expect, spyOn, test } from "bun:test";
import { createServer } from "node:net";

import { createHttpDispatcher } from "../packages/mesh/src/transport-http.ts";
import { MeshCallError, MeshTimeoutError, type NodeRecord } from "../packages/mesh/src/types.ts";
import { CHIMPBASE_REQUEST_REJECTED_HEADER, v } from "../packages/runtime/index.ts";
import { createCallDispatcher } from "../packages/mesh/src/call.ts";
import { MeshPeerCache } from "../packages/mesh/src/discovery.ts";

const peer: NodeRecord = {
  advertisedUrl: "http://mesh-peer.test",
  lastHeartbeatMs: Date.now(),
  metadata: {},
  nodeId: "peer",
  services: [],
  startedAtMs: Date.now(),
};

test("a connection lost after HTTP headers retries a healthy replica", async () => {
  const visited: string[] = [];
  const broken = createServer((socket) => {
    socket.once("data", () => {
      visited.push("broken");
      socket.write('HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 100\r\nConnection: close\r\n\r\n{"ok":true');
      setTimeout(() => socket.destroy(), 20);
    });
  });
  await new Promise<void>((resolve, reject) => {
    broken.once("error", reject);
    broken.listen(0, "127.0.0.1", resolve);
  });
  const address = broken.address();
  if (address === null || typeof address === "string") throw new Error("TCP fixture has no port");
  const healthy = Bun.serve({
    hostname: "127.0.0.1", port: 0,
    fetch() {
      visited.push("healthy");
      return Response.json({ ok: true, result: "healthy" });
    },
  });
  const cache = new MeshPeerCache(30_000);
  for (const [nodeId, port] of [["broken", address.port], ["healthy", healthy.port]] as const) {
    cache.upsert({
      ...peer, nodeId, advertisedUrl: `http://127.0.0.1:${port}`,
      services: [{ name: "test", version: 1, actions: ["v1.test.run"], events: [] }],
    });
  }
  const call = createCallDispatcher({
    cache, defaultRetries: 1, defaultStrategy: "local-first", defaultTimeoutMs: 1_000,
    localActionNames: new Set(), localNodeId: "caller", middleware: [],
    remoteDispatcher: createHttpDispatcher({ callerNodeId: "caller", rpcPath: "/rpc", tokenProvider: () => "token" }),
  });
  try {
    expect(await call({} as never, "v1.test.run", {}, v.string(), { retry: { attempts: 1, delayMs: 0 } })).toBe("healthy");
    expect(visited).toEqual(["broken", "healthy"]);
  } finally {
    await healthy.stop(true);
    await new Promise<void>((resolve, reject) => broken.close((error) => error ? reject(error) : resolve()));
  }
});

test("a complete malformed JSON response does not retry another replica", async () => {
  const visited: string[] = [];
  const fetchMock = spyOn(globalThis, "fetch").mockImplementation(Object.assign(async (
    input: Parameters<typeof fetch>[0],
  ) => {
    const url = input instanceof Request ? input.url : String(input);
    visited.push(url);
    return url.includes("malformed") ? new Response("{") : Response.json({ ok: true, result: "healthy" });
  }, { preconnect: () => {} }));
  const cache = new MeshPeerCache(30_000);
  for (const nodeId of ["malformed", "healthy"]) {
    cache.upsert({
      ...peer, nodeId, advertisedUrl: `http://${nodeId}.test`,
      services: [{ name: "test", version: 1, actions: ["v1.test.run"], events: [] }],
    });
  }
  const call = createCallDispatcher({
    cache, defaultRetries: 2, defaultStrategy: "local-first", defaultTimeoutMs: 1_000,
    localActionNames: new Set(), localNodeId: "caller", middleware: [],
    remoteDispatcher: createHttpDispatcher({ callerNodeId: "caller", rpcPath: "/rpc", tokenProvider: () => "token" }),
  });
  try {
    await expect(call({} as never, "v1.test.run", {}, v.string(), {})).rejects.toMatchObject({
      message: "mesh RPC returned invalid JSON", retryable: false,
    });
    expect(visited).toEqual(["http://malformed.test/rpc"]);
  } finally {
    fetchMock.mockRestore();
  }
});

for (const marked of [false, true]) {
  test(`HTTP 503 ${marked ? "before execution retries a healthy peer" : "from an application is not retried"}`, async () => {
    const visited: string[] = [];
    const fetchMock = spyOn(globalThis, "fetch").mockImplementation(Object.assign(async (
      input: Parameters<typeof fetch>[0],
    ) => {
      const url = input instanceof Request ? input.url : String(input);
      visited.push(url);
      return url.includes("draining")
        ? new Response("runtime is stopping", { status: 503, headers: marked ? { [CHIMPBASE_REQUEST_REJECTED_HEADER]: "1" } : {} })
        : Response.json({ ok: true, result: "healthy" });
    }, { preconnect: () => {} }));
    const cache = new MeshPeerCache(30_000);
    for (const nodeId of ["draining", "healthy"]) {
      cache.upsert({
        ...peer, nodeId, advertisedUrl: `http://${nodeId}.test`,
        services: [{ name: "test", version: 1, actions: ["v1.test.run"], events: [] }],
      });
    }
    const call = createCallDispatcher({
      cache, defaultRetries: 2, defaultStrategy: "local-first", defaultTimeoutMs: 1_000,
      localActionNames: new Set(), localNodeId: "caller", middleware: [],
      remoteDispatcher: createHttpDispatcher({ callerNodeId: "caller", rpcPath: "/rpc", tokenProvider: () => "token" }),
    });
    try {
      const outcome = call({} as never, "v1.test.run", {}, v.string(), { retry: { attempts: 2, delayMs: 0 } });
      if (marked) {
        expect(await outcome).toBe("healthy");
        expect(visited).toEqual(["http://draining.test/rpc", "http://healthy.test/rpc"]);
      } else {
        await expect(outcome).rejects.toBeInstanceOf(MeshCallError);
        expect(visited).toEqual(["http://draining.test/rpc"]);
      }
    } finally {
      fetchMock.mockRestore();
    }
  });
}

for (const status of [200, 503]) {
  test(`HTTP deadline aborts a stalled ${status} response body`, async () => {
    let bodyController: ReadableStreamDefaultController<Uint8Array> | undefined;
    let aborted = false;
    const fetchMock = spyOn(globalThis, "fetch").mockImplementation(Object.assign(async (
      _input: Parameters<typeof fetch>[0],
      init?: Parameters<typeof fetch>[1],
    ) => {
      const signal = init?.signal;
      if (!signal) throw new Error("fixture requires an abort signal");
      const body = new ReadableStream<Uint8Array>({
        start(controller) {
          bodyController = controller;
          controller.enqueue(new TextEncoder().encode(status === 200 ? '{"ok":true,"result":' : "upstream stalled"));
          signal.addEventListener("abort", () => {
            aborted = true;
            controller.error(new DOMException("fixture aborted", "AbortError"));
          }, { once: true });
        },
      });
      return new Response(body, { status });
    }, { preconnect: () => {} }));
    const cleanupTimer = setTimeout(() => bodyController?.error(new Error("fixture deadline exceeded")), 250);
    const dispatch = createHttpDispatcher({
      callerNodeId: "caller",
      rpcPath: "/rpc",
      tokenProvider: () => "token",
    });

    try {
      await expect(dispatch({
        actionName: "v1.identity.get",
        args: {},
        deadlineMs: Date.now() + 30,
        peer,
      })).rejects.toBeInstanceOf(MeshTimeoutError);
      expect(aborted).toBe(true);
    } finally {
      clearTimeout(cleanupTimer);
      if (!aborted) bodyController?.error(new Error("fixture closed"));
      fetchMock.mockRestore();
    }
  });
}
