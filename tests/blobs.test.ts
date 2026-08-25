import { afterEach, describe, expect, test } from "bun:test";
import { mkdtemp, rm, stat } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";

import { createChimpbase } from "../packages/bun/src/library.ts";
import { action, v } from "../packages/runtime/index.ts";
import {
  chimpbaseBlobs,
  fsBlobDriver,
  memoryBlobDriver,
} from "../packages/blobs/src/index.ts";

const cleanupDirs: string[] = [];

afterEach(async () => {
  while (cleanupDirs.length > 0) {
    const dir = cleanupDirs.pop();
    if ((dir !== undefined && dir.length > 0)) await rm(dir, { recursive: true, force: true });
  }
});

async function bootBlobsHost(options: {
  root?: string;
  useFs?: boolean;
  serve?: boolean;
  clock?: () => number;
  baseUrl?: string;
} = {}) {
  const driver = (options.useFs === true)
    ? fsBlobDriver({ root: options.root! })
    : memoryBlobDriver();

  const putBlob = action({
    name: "blobs.put",
    args: v.object({ bucket: v.string(), key: v.string(), body: v.string() }),
    async handler(ctx, input) {
      const bytes = new TextEncoder().encode(input.body);
      return await ctx.blobs.put(input.bucket, input.key, bytes, {
        contentType: "text/plain",
        metadata: { author: "tests" },
      });
    },
  });

  const getBlob = action({
    name: "blobs.get",
    args: v.object({ bucket: v.string(), key: v.string() }),
    async handler(ctx, input) {
      const obj = await ctx.blobs.get(input.bucket, input.key);
      if (!(obj !== null)) return null;
      const text = await new Response(obj.body).text();
      return { text, size: obj.size, etag: obj.etag, metadata: obj.metadata };
    },
  });

  const deleteBlob = action({
    name: "blobs.delete",
    args: v.object({ bucket: v.string(), key: v.string() }),
    async handler(ctx, input) {
      return await ctx.blobs.delete(input.bucket, input.key);
    },
  });

  const listBlobs = action({
    name: "blobs.list",
    args: v.object({ bucket: v.string(), prefix: v.string().optional(), delimiter: v.string().optional() }),
    async handler(ctx, input) {
      return await ctx.blobs.list(input.bucket, {
        prefix: input.prefix,
        delimiter: input.delimiter,
      });
    },
  });

  const copyBlob = action({
    name: "blobs.copy",
    args: v.object({
      srcBucket: v.string(), srcKey: v.string(),
      dstBucket: v.string(), dstKey: v.string(),
    }),
    async handler(ctx, input) {
      return await ctx.blobs.copy(
        { bucket: input.srcBucket, key: input.srcKey },
        { bucket: input.dstBucket, key: input.dstKey },
      );
    },
  });

  const multipartPut = action({
    name: "blobs.multipart",
    args: v.object({ bucket: v.string(), key: v.string(), parts: v.array(v.string()) }),
    async handler(ctx, input) {
      const upload = await ctx.blobs.createUpload(input.bucket, input.key, {
        contentType: "text/plain",
      });
      let partNumber = 1;
      for (const chunk of input.parts) {
        await upload.writePart(partNumber, new TextEncoder().encode(chunk));
        partNumber += 1;
      }
      return await upload.complete();
    },
  });

  const putWithIf = action({
    name: "blobs.put.if",
    args: v.object({
      bucket: v.string(), key: v.string(), body: v.string(),
      ifNoneMatch: v.string().optional(),
      ifMatch: v.string().optional(),
    }),
    async handler(ctx, input) {
      return await ctx.blobs.put(
        input.bucket,
        input.key,
        new TextEncoder().encode(input.body),
        { ifNoneMatch: input.ifNoneMatch, ifMatch: input.ifMatch },
      );
    },
  });

  const getRange = action({
    name: "blobs.get.range",
    args: v.object({
      bucket: v.string(), key: v.string(),
      start: v.number(), end: v.number().optional(),
    }),
    async handler(ctx, input) {
      const obj = await ctx.blobs.get(input.bucket, input.key, {
        range: { start: input.start, end: input.end },
      });
      if (!(obj !== null)) return null;
      return { text: await new Response(obj.body).text(), size: obj.size };
    },
  });

  const abortUpload = action({
    name: "blobs.upload.abort",
    args: v.object({ bucket: v.string(), key: v.string() }),
    async handler(ctx, input) {
      const upload = await ctx.blobs.createUpload(input.bucket, input.key);
      await upload.writePart(1, new TextEncoder().encode("partial"));
      await upload.abort();
      return { id: upload.id };
    },
  });

  const signBlob = action({
    name: "blobs.sign",
    args: v.object({ bucket: v.string(), key: v.string(), op: v.string(), ttlSec: v.number() }),
    async handler(ctx, input) {
      return ctx.blobs.sign({
        bucket: input.bucket,
        key: input.key,
        op: input.op as "get" | "put",
        ttlSec: input.ttlSec,
      });
    },
  });

  const plugin = chimpbaseBlobs({
    secret: "test-secret",
    baseUrl: options.baseUrl ?? "http://127.0.0.1",
    clock: options.clock,
  });

  const host = await createChimpbase({
    project: { name: "blobs-test" },
    registrations: [
      putBlob, getBlob, deleteBlob, listBlobs, copyBlob, multipartPut, signBlob,
      putWithIf, getRange, abortUpload,
      ...plugin.registrations,
    ],
    storage: { engine: "memory" },
    server: { port: 0 },
    blobs: {
      driver,
      buckets: ["uploads", "archive"],
      signer: plugin.signer,
    },
  });

  const started = await host.start((options.serve === true) ? {} : { serve: false, runWorker: false });
  const port = started.server?.port;
  const serverBaseUrl = (port !== undefined && port > 0) ? `http://127.0.0.1:${port}` : null;
  return { host, started, plugin, baseUrl: serverBaseUrl };
}

interface PutResult { size: number; etag: string }
interface ListResult { entries: { key: string }[]; commonPrefixes: string[]; nextCursor: string | null }

describe("chimpbase blobs primitive (memory driver)", () => {
  test("put/head/get/delete roundtrip", async () => {
    const { host, started } = await bootBlobsHost();
    try {
      const put = await host.executeAction("blobs.put", {
        bucket: "uploads", key: "hello.txt", body: "hello world",
      });
      const putResult = put.result as PutResult;
      expect(putResult.size).toBe(11);
      expect(putResult.etag).toHaveLength(64);

      const head = await host.executeAction("blobs.get", {
        bucket: "uploads", key: "hello.txt",
      });
      expect(head.result).toMatchObject({
        text: "hello world",
        size: 11,
        metadata: { author: "tests" },
      });

      const deleted = await host.executeAction("blobs.delete", {
        bucket: "uploads", key: "hello.txt",
      });
      expect(deleted.result).toBe(true);

      const missing = await host.executeAction("blobs.get", {
        bucket: "uploads", key: "hello.txt",
      });
      expect(missing.result).toBe(null);
    } finally {
      await started.stop();
    }
  });

  test("list with prefix + delimiter", async () => {
    const { host, started } = await bootBlobsHost();
    try {
      for (const key of ["photos/a.jpg", "photos/b.jpg", "docs/a.txt", "root.txt"]) {
        await host.executeAction("blobs.put", { bucket: "uploads", key, body: "x" });
      }

      const listed = await host.executeAction("blobs.list", {
        bucket: "uploads", prefix: "photos/",
      });
      const listedResult = listed.result as ListResult;
      expect(listedResult.entries.map((e) => e.key)).toEqual([
        "photos/a.jpg", "photos/b.jpg",
      ]);

      const grouped = await host.executeAction("blobs.list", {
        bucket: "uploads", delimiter: "/",
      });
      const groupedResult = grouped.result as ListResult;
      expect(groupedResult.commonPrefixes.sort()).toEqual(["docs/", "photos/"]);
      expect(groupedResult.entries.map((e) => e.key)).toEqual(["root.txt"]);
    } finally {
      await started.stop();
    }
  });

  test("copy between buckets", async () => {
    const { host, started } = await bootBlobsHost();
    try {
      await host.executeAction("blobs.put", {
        bucket: "uploads", key: "report.txt", body: "original",
      });
      await host.executeAction("blobs.copy", {
        srcBucket: "uploads", srcKey: "report.txt",
        dstBucket: "archive", dstKey: "report-copy.txt",
      });
      const copied = await host.executeAction("blobs.get", {
        bucket: "archive", key: "report-copy.txt",
      });
      expect((copied.result as { text: string }).text).toBe("original");
    } finally {
      await started.stop();
    }
  });

  test("multipart upload assembles parts in order", async () => {
    const { host, started } = await bootBlobsHost();
    try {
      const complete = await host.executeAction("blobs.multipart", {
        bucket: "uploads",
        key: "big.txt",
        parts: ["aaa", "bbb", "ccc"],
      });
      const completeResult = complete.result as PutResult;
      expect(completeResult.size).toBe(9);
      expect(completeResult.etag.endsWith("-3")).toBe(true);

      const fetched = await host.executeAction("blobs.get", {
        bucket: "uploads", key: "big.txt",
      });
      expect((fetched.result as { text: string }).text).toBe("aaabbbccc");
    } finally {
      await started.stop();
    }
  });

  test("sign produces url whose token verifies", async () => {
    const { host, started, plugin } = await bootBlobsHost();
    try {
      const signed = await host.executeAction("blobs.sign", {
        bucket: "uploads", key: "hello.txt", op: "get", ttlSec: 60,
      });
      const url = new URL(signed.result as string);
      const token = url.searchParams.get("token");
      expect(token).toBeTruthy();
      const payload = plugin.signer.verify(token!);
      expect(payload).not.toBeNull();
      expect(payload!.bucket).toBe("uploads");
      expect(payload!.op).toBe("get");

      const tampered = `${token}x`;
      expect(plugin.signer.verify(tampered)).toBeNull();
      expect(plugin.signer.verify("missing-signature")).toBeNull();
    } finally {
      await started.stop();
    }
  });

  test("unknown bucket is rejected", async () => {
    const { host, started } = await bootBlobsHost();
    try {
      await expect(
        host.executeAction("blobs.put", { bucket: "ghost", key: "x", body: "y" }),
      ).rejects.toThrow(/unknown blob bucket/);
    } finally {
      await started.stop();
    }
  });

  test("ifNoneMatch=* fails when object exists, ifMatch fails on wrong etag", async () => {
    const { host, started } = await bootBlobsHost();
    try {
      await host.executeAction("blobs.put.if", {
        bucket: "uploads", key: "cond.txt", body: "first",
      });
      await expect(
        host.executeAction("blobs.put.if", {
          bucket: "uploads", key: "cond.txt", body: "second", ifNoneMatch: "*",
        }),
      ).rejects.toThrow(/already exists/);

      await expect(
        host.executeAction("blobs.put.if", {
          bucket: "uploads", key: "cond.txt", body: "third", ifMatch: "not-the-real-etag",
        }),
      ).rejects.toThrow(/etag mismatch/);
    } finally {
      await started.stop();
    }
  });

  test("range read returns slice", async () => {
    const { host, started } = await bootBlobsHost();
    try {
      await host.executeAction("blobs.put", {
        bucket: "uploads", key: "range.txt", body: "abcdefghij",
      });
      const sliced = await host.executeAction("blobs.get.range", {
        bucket: "uploads", key: "range.txt", start: 2, end: 5,
      });
      expect(sliced.result).toEqual({ text: "cdef", size: 4 });
    } finally {
      await started.stop();
    }
  });

  test("abort removes upload rows and staging parts", async () => {
    const { host, started } = await bootBlobsHost();
    try {
      const aborted = await host.executeAction("blobs.upload.abort", {
        bucket: "uploads", key: "aborted.txt",
      });
      const uploadId = (aborted.result as { id: string }).id;
      const rows = await host.executeAction("blobs.list", {
        bucket: "uploads", prefix: "aborted.txt",
      });
      expect((rows.result as ListResult).entries).toEqual([]);
      // ensure metadata is gone
      const direct = await host.engine.createRouteEnv().blobs.head("uploads", "aborted.txt");
      expect(direct).toBeNull();
      // and the upload row is gone too — resumeUpload should throw
      await expect(
        host.engine.createRouteEnv().blobs.resumeUpload(uploadId),
      ).rejects.toThrow(/not found/);
    } finally {
      await started.stop();
    }
  });
});

describe("chimpbase blobs primitive (fs driver)", () => {
  test("writes bytes under the configured root and lists via engine", async () => {
    const root = await mkdtemp(join(tmpdir(), "chimpbase-blobs-fs-"));
    cleanupDirs.push(root);
    const { host, started } = await bootBlobsHost({ useFs: true, root });
    try {
      const put = await host.executeAction("blobs.put", {
        bucket: "uploads", key: "nested/report.txt", body: "payload",
      });
      expect((put.result as PutResult).size).toBe(7);

      const diskRoot = join(root, "uploads", "objects");
      await expect(stat(diskRoot)).resolves.toHaveProperty("isDirectory");

      const listed = await host.executeAction("blobs.list", {
        bucket: "uploads", prefix: "nested/",
      });
      expect((listed.result as ListResult).entries).toHaveLength(1);

      const deleted = await host.executeAction("blobs.delete", {
        bucket: "uploads", key: "nested/report.txt",
      });
      expect(deleted.result).toBe(true);
    } finally {
      await started.stop();
    }
  });
});

describe("chimpbase blobs signed URLs", () => {
  test("GET signed URL streams stored body and rejects tampered/expired tokens", async () => {
    const boot = await bootBlobsHost({ serve: true, baseUrl: "http://127.0.0.1" });
    try {
      await boot.host.executeAction("blobs.put", {
        bucket: "uploads", key: "signed.txt", body: "hello signed",
      });

      const signed = await boot.host.executeAction("blobs.sign", {
        bucket: "uploads", key: "signed.txt", op: "get", ttlSec: 60,
      });
      const signedUrl = new URL(signed.result as string);
      const path = `${signedUrl.pathname}?${signedUrl.searchParams.toString()}`;
      const ok = await fetch(`${boot.baseUrl}${path}`);
      expect(ok.status).toBe(200);
      expect(ok.headers.get("etag")).toHaveLength(64);
      expect(await ok.text()).toBe("hello signed");

      const tampered = signedUrl.searchParams.get("token") + "XY";
      const bad = await fetch(`${boot.baseUrl}${signedUrl.pathname}?token=${encodeURIComponent(tampered)}`);
      expect(bad.status).toBe(401);
    } finally {
      await boot.started.stop();
    }
  });

  test("PUT signed URL writes body into the bucket", async () => {
    const boot = await bootBlobsHost({ serve: true, baseUrl: "http://127.0.0.1" });
    try {
      const signed = await boot.host.executeAction("blobs.sign", {
        bucket: "uploads", key: "uploaded.txt", op: "put", ttlSec: 60,
      });
      const signedUrl = new URL(signed.result as string);
      const res = await fetch(`${boot.baseUrl}${signedUrl.pathname}?${signedUrl.searchParams.toString()}`, {
        method: "PUT",
        body: "uploaded via signed url",
      });
      expect(res.status).toBe(201);

      const fetched = await boot.host.executeAction("blobs.get", {
        bucket: "uploads", key: "uploaded.txt",
      });
      expect((fetched.result as { text: string }).text).toBe("uploaded via signed url");
    } finally {
      await boot.started.stop();
    }
  });

  test("expired token is rejected", async () => {
    let now = 1_000_000_000_000;
    const boot = await bootBlobsHost({
      serve: true,
      baseUrl: "http://127.0.0.1",
      clock: () => now,
    });
    try {
      await boot.host.executeAction("blobs.put", {
        bucket: "uploads", key: "maybe.txt", body: "payload",
      });
      const signed = await boot.host.executeAction("blobs.sign", {
        bucket: "uploads", key: "maybe.txt", op: "get", ttlSec: 60,
      });
      now += 120 * 1000;
      const signedUrl = new URL(signed.result as string);
      const res = await fetch(`${boot.baseUrl}${signedUrl.pathname}?${signedUrl.searchParams.toString()}`);
      expect(res.status).toBe(401);
    } finally {
      await boot.started.stop();
    }
  });
});
