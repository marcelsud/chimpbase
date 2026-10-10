import { expect, spyOn, test } from "bun:test";

import { createHttpDispatcher } from "../packages/mesh/src/transport-http.ts";
import { MeshTimeoutError, type NodeRecord } from "../packages/mesh/src/types.ts";

const peer: NodeRecord = {
  advertisedUrl: "http://mesh-peer.test",
  lastHeartbeatMs: Date.now(),
  metadata: {},
  nodeId: "peer",
  services: [],
  startedAtMs: Date.now(),
};

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
