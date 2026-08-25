import {
  cron,
  route,
  type ChimpbaseRegistration,
} from "@chimpbase/runtime";

import { createBlobSigner, type CreateBlobSignerOptions } from "./signing.ts";

export interface ChimpbaseBlobsPluginOptions extends CreateBlobSignerOptions {
  /**
   * Cron schedule for expiring stale multipart uploads.
   * Default: every hour at minute 0.
   */
  gcSchedule?: string;
  /**
   * Prefix used when registering routes. Defaults to the signer's routeBasePath.
   */
  routeBasePath?: string;
}

export interface ChimpbaseBlobsPlugin {
  signer: ReturnType<typeof createBlobSigner>;
  registrations: ChimpbaseRegistration[];
}

export function chimpbaseBlobs(options: ChimpbaseBlobsPluginOptions): ChimpbaseBlobsPlugin {
  const signer = createBlobSigner(options);
  const basePath = (options.routeBasePath ?? signer.routeBasePath).replace(/\/$/, "");

  const downloadRoute = route("chimpbase.blobs.download", async (request, env) => {
    const url = new URL(request.url);
    if (request.method !== "GET" || url.pathname !== `${basePath}/get`) return null;
    const token = url.searchParams.get("token");
    if (!(token !== null && token.length > 0)) return new Response("missing token", { status: 400 });
    const payload = signer.verify(token);
    if (!(payload !== null) || payload.op !== "get") {
      return new Response("invalid token", { status: 401 });
    }
    const obj = await env.blobs.get(payload.bucket, payload.key);
    if (!(obj !== null)) return new Response("not found", { status: 404 });
    const headers = new Headers({
      "content-type": obj.contentType,
      "content-length": String(obj.size),
      etag: obj.etag,
    });
    if ((payload.responseContentDisposition !== undefined && payload.responseContentDisposition.length > 0)) {
      headers.set("content-disposition", payload.responseContentDisposition);
    }
    return new Response(obj.body, { status: 200, headers });
  });

  const uploadRoute = route("chimpbase.blobs.upload", async (request, env) => {
    const url = new URL(request.url);
    if (request.method !== "PUT" || url.pathname !== `${basePath}/put`) return null;
    const token = url.searchParams.get("token");
    if (!(token !== null && token.length > 0)) return new Response("missing token", { status: 400 });
    const payload = signer.verify(token);
    if (!(payload !== null) || payload.op !== "put") {
      return new Response("invalid token", { status: 401 });
    }
    if (!(request.body !== null)) {
      return new Response("missing body", { status: 400 });
    }
    const contentType = payload.contentType
      ?? request.headers.get("content-type")
      ?? "application/octet-stream";
    let body: Uint8Array | ReadableStream<Uint8Array>;
    if (payload.sizeMax !== undefined) {
      const buf = new Uint8Array(await request.arrayBuffer());
      if (buf.byteLength > payload.sizeMax) {
        return new Response("payload too large", { status: 413 });
      }
      body = buf;
    } else {
      body = request.body as ReadableStream<Uint8Array>;
    }
    const result = await env.blobs.put(payload.bucket, payload.key, body, {
      contentType,
    });
    return Response.json({
      bucket: result.bucket,
      key: result.key,
      size: result.size,
      etag: result.etag,
    }, { status: 201 });
  });

  const gcHandler = cron(
    "__chimpbase.blobs.gc",
    options.gcSchedule ?? "0 * * * *",
    async (ctx) => {
      const now = Date.now();
      await ctx.db.query(
        "DELETE FROM _chimpbase_blob_upload_parts WHERE upload_id IN (SELECT upload_id FROM _chimpbase_blob_uploads WHERE expires_at_ms <= ?1)",
        [now],
      );
      await ctx.db.query(
        "DELETE FROM _chimpbase_blob_uploads WHERE expires_at_ms <= ?1",
        [now],
      );
    },
  );

  return {
    signer,
    registrations: [downloadRoute, uploadRoute, gcHandler],
  };
}
