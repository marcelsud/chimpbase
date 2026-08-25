import { timingSafeEqual } from "node:crypto";

import { isJsonObject, type ChimpbaseContext } from "@chimpbase/runtime";

import type { NodeRecord } from "./types.ts";
import { MeshCallError } from "./types.ts";

export const RPC_EXECUTE_ACTION = "__chimpbase.mesh.rpc.execute";
export const DEFAULT_RPC_PATH = "/__chimpbase/mesh/rpc";
export const MESH_TOKEN_HEADER = "x-chimpbase-mesh-token";
export const CALLER_NODE_HEADER = "x-chimpbase-mesh-caller";

export interface RpcEnvelope {
  actionName: string;
  args: unknown;
  callerNodeId: string;
  deadlineMs: number;
}

export interface CreateHttpDispatcherOptions {
  callerNodeId: string;
  rpcPath: string;
  tokenProvider: () => string | null;
}

export function createHttpDispatcher(
  options: CreateHttpDispatcherOptions,
): (params: {
  actionName: string;
  args: unknown;
  ctx: ChimpbaseContext;
  deadlineMs: number;
  peer: NodeRecord;
}) => Promise<unknown> {
  return async (params: {
    actionName: string;
    args: unknown;
    ctx: ChimpbaseContext;
    deadlineMs: number;
    peer: NodeRecord;
  }): Promise<unknown> => {
    const { peer, actionName, args, deadlineMs } = params;
    if (!(peer.advertisedUrl !== null && peer.advertisedUrl.length > 0)) {
      throw new MeshCallError(actionName, peer.nodeId, `peer ${peer.nodeId} has no advertised URL`);
    }

    const token = options.tokenProvider();
    if (!(token !== null && token.length > 0)) {
      throw new MeshCallError(actionName, peer.nodeId, "mesh token missing — cannot authenticate remote RPC");
    }

    const envelope: RpcEnvelope = {
      actionName,
      args,
      callerNodeId: options.callerNodeId,
      deadlineMs,
    };

    const url = joinUrl(peer.advertisedUrl, options.rpcPath);
    const controller = new AbortController();
    const timeoutMs = Math.max(deadlineMs - Date.now(), 1);
    const timer = setTimeout(() => controller.abort(), timeoutMs);

    let response: Response;
    try {
      response = await fetch(url, {
        body: JSON.stringify(envelope),
        headers: {
          "content-type": "application/json",
          [MESH_TOKEN_HEADER]: token,
          [CALLER_NODE_HEADER]: options.callerNodeId,
        },
        method: "POST",
        signal: controller.signal,
      });
    } catch (error) {
      throw new MeshCallError(
        actionName,
        peer.nodeId,
        `mesh RPC fetch failed: ${error instanceof Error ? error.message : String(error)}`,
        error,
      );
    } finally {
      clearTimeout(timer);
    }

    if (!response.ok) {
      const text = await safeText(response);
      throw new MeshCallError(
        actionName,
        peer.nodeId,
        `mesh RPC returned ${response.status}: ${text}`,
      );
    }

    const body: unknown = await response.json();
    if (!isJsonObject(body) || typeof body.ok !== "boolean") {
      throw new MeshCallError(actionName, peer.nodeId, "mesh RPC returned an invalid response");
    }
    if (!body.ok) {
      throw new MeshCallError(
        actionName,
        peer.nodeId,
        typeof body.error === "string" ? body.error : "mesh RPC remote error",
      );
    }

    return body.result;
  };
}

export function compareTokens(expected: string | null, received: string | null): boolean {
  if (!(expected !== null && expected.length > 0) || !(received !== null && received.length > 0)) {
    return false;
  }

  const expectedBuf = Buffer.from(expected);
  const receivedBuf = Buffer.from(received);
  if (expectedBuf.length !== receivedBuf.length) {
    return false;
  }

  return timingSafeEqual(expectedBuf, receivedBuf);
}

function joinUrl(base: string, path: string): string {
  const trimmedBase = base.endsWith("/") ? base.slice(0, -1) : base;
  const trimmedPath = path.startsWith("/") ? path : `/${path}`;
  return `${trimmedBase}${trimmedPath}`;
}

async function safeText(response: Response): Promise<string> {
  try {
    return await response.text();
  } catch {
    return "<no body>";
  }
}
