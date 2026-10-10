import { expect, spyOn, test } from "bun:test";

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
