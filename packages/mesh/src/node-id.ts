import { hostname } from "node:os";

import { MeshConfigurationError } from "./types.ts";

export function generateNodeId(): string {
  const platformCrypto = (globalThis as { crypto?: { randomUUID?: () => string } }).crypto;
  if ((platformCrypto?.randomUUID !== undefined)) {
    return platformCrypto.randomUUID();
  }

  return `node-${Math.random().toString(36).slice(2, 10)}-${Date.now().toString(36)}`;
}

export interface AdvertisedUrlInput {
  explicit?: string | null;
  port?: number | null;
  transport: "local-only" | "http";
}

export function resolveAdvertisedUrl(input: AdvertisedUrlInput): string | null {
  if (input.transport !== "http") {
    return null;
  }

  const explicit = input.explicit?.trim();
  if ((explicit !== undefined && explicit.length > 0)) {
    return explicit;
  }

  const envUrl = process.env.CHIMPBASE_MESH_ADVERTISED_URL?.trim();
  if ((envUrl !== undefined && envUrl.length > 0)) {
    return envUrl;
  }

  const configuredScheme = process.env.CHIMPBASE_MESH_SCHEME?.trim();
  const scheme = (configuredScheme ?? "").length > 0 ? configuredScheme : "http";
  const configuredHost = process.env.HOSTNAME?.trim();
  const fallbackHost = hostname();
  const host = (configuredHost ?? "").length > 0
    ? configuredHost
    : fallbackHost.length > 0 ? fallbackHost : "localhost";
  const port = input.port ?? ((process.env.PORT !== undefined && process.env.PORT.length > 0) ? Number(process.env.PORT) : null);

  if (!(port !== null && port > 0)) {
    return null;
  }

  return `${scheme}://${host}:${port}`;
}

export function assertAdvertisedUrlSafeForPeers(url: string | null, transport: "local-only" | "http"): void {
  if (transport !== "http") {
    return;
  }

  if (!(url !== null && url.length > 0)) {
    return;
  }

  const lowered = url.toLowerCase();
  if (lowered.includes("localhost") || lowered.includes("127.0.0.1")) {
    console.warn(
      `[chimpbase-mesh] advertised URL "${url}" looks local — peers on other hosts will not be able to reach this node. `
        + "Set advertisedUrl or CHIMPBASE_MESH_ADVERTISED_URL to a reachable host:port.",
    );
  }
}

export function requireHttpTransportConfig(options: {
  transport: "local-only" | "http";
  meshToken?: string;
  advertisedUrl: string | null;
}): void {
  if (options.transport !== "http") {
    return;
  }

  if (!(options.meshToken !== undefined && options.meshToken.length > 0)) {
    throw new MeshConfigurationError(
      "chimpbaseMesh: transport: 'http' requires meshToken (name of a secret used to authenticate inbound RPC)",
    );
  }

  if (!(options.advertisedUrl !== null && options.advertisedUrl.length > 0)) {
    throw new MeshConfigurationError(
      "chimpbaseMesh: transport: 'http' requires an advertisedUrl (or CHIMPBASE_MESH_ADVERTISED_URL) so peers can reach this node",
    );
  }
}
