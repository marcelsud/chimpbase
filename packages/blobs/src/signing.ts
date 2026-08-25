import { createHmac, timingSafeEqual } from "node:crypto";

import type { ChimpbaseBlobSigner } from "@chimpbase/core";
import type { ChimpbaseBlobSignOptions } from "@chimpbase/runtime";

export interface CreateBlobSignerOptions {
  secret: string;
  baseUrl: string;
  routeBasePath?: string;
  clock?: () => number;
}

interface SignedTokenPayload {
  bucket: string;
  key: string;
  op: "get" | "put";
  exp: number;
  contentType?: string;
  sizeMax?: number;
  responseContentDisposition?: string;
  nonce: string;
}

function base64UrlEncode(input: Uint8Array | string): string {
  const bytes = typeof input === "string" ? new TextEncoder().encode(input) : input;
  return Buffer.from(bytes).toString("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/g, "");
}

function base64UrlDecode(input: string): Uint8Array {
  const padded = input.replace(/-/g, "+").replace(/_/g, "/").padEnd(Math.ceil(input.length / 4) * 4, "=");
  return Uint8Array.from(Buffer.from(padded, "base64"));
}

function randomNonce(): string {
  const bytes = new Uint8Array(12);
  crypto.getRandomValues(bytes);
  return base64UrlEncode(bytes);
}

export function createBlobSigner(options: CreateBlobSignerOptions): ChimpbaseBlobSigner & {
  verify(token: string): SignedTokenPayload | null;
  routeBasePath: string;
} {
  const { secret, baseUrl } = options;
  const routeBasePath = options.routeBasePath ?? "/_blobs";
  const clock = options.clock ?? (() => Date.now());
  if (!(secret.length > 0)) throw new Error("createBlobSigner requires secret");

  const sign = (input: ChimpbaseBlobSignOptions): string => {
    const payload: SignedTokenPayload = {
      bucket: input.bucket,
      key: input.key,
      op: input.op,
      exp: Math.floor(clock() / 1000) + Math.max(1, Math.floor(input.ttlSec)),
      contentType: input.contentType,
      sizeMax: input.sizeMax,
      responseContentDisposition: input.responseContentDisposition,
      nonce: randomNonce(),
    };
    const encoded = base64UrlEncode(JSON.stringify(payload));
    const sig = createHmac("sha256", secret).update(encoded).digest();
    const token = `${encoded}.${base64UrlEncode(sig)}`;
    const segment = input.op === "get" ? "get" : "put";
    const base = baseUrl.replace(/\/$/, "");
    return `${base}${routeBasePath}/${segment}?token=${encodeURIComponent(token)}`;
  };

  const verify = (token: string): SignedTokenPayload | null => {
    const [encoded, signature] = token.split(".");
    if (encoded.length === 0 || signature === undefined || signature.length === 0) return null;
    const expected = createHmac("sha256", secret).update(encoded).digest();
    const received = base64UrlDecode(signature);
    if (expected.length !== received.length) return null;
    try {
      if (!timingSafeEqual(expected, received)) return null;
    } catch {
      return null;
    }
    let payload: SignedTokenPayload;
    try {
      payload = JSON.parse(new TextDecoder().decode(base64UrlDecode(encoded))) as SignedTokenPayload;
    } catch {
      return null;
    }
    if (payload.exp <= Math.floor(clock() / 1000)) return null;
    return payload;
  };

  return { sign, verify, routeBasePath };
}
