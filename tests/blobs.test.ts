import { afterEach, describe, expect, test } from "bun:test";
import { mkdtemp, readdir, rm, stat } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";

import { createChimpbase } from "../packages/bun/src/library.ts";
import { action, v } from "../packages/runtime/index.ts";
import {
  chimpbaseBlobs,
  fsBlobDriver,
  memoryBlobDriver,
} from "../packages/blobs/src/index.ts";
import type { ChimpbaseBlobDriver, ChimpbaseBlobMetaRow } from "../packages/core/index.ts";


const cleanupDirs: string[] = [];

for (const engine of ["memory", "sqlite", "postgres"] as const) {
  for (const useFs of [false, true]) {
    const pgUrl = process.env.CHIMPBASE_TEST_PG_URL;
    (engine === "postgres" && !pgUrl ? test.skip : test)(`expired upload cleanup retains failed IDs and removes staging on retry (${engine}, ${useFs ? "filesystem" : "memory"})`, async () => {
      const root = await mkdtemp(join(tmpdir(), "chimpbase-expired-upload-"));
      cleanupDirs.push(root);
      const driver = useFs ? fsBlobDriver({ root }) : memoryBlobDriver();
      const plugin = chimpbaseBlobs({ secret: "test-secret", baseUrl: "http://test.local" });
      const gc = plugin.registrations.find((entry) => entry.kind === "cron");
      if (gc === undefined) throw new Error("missing cleanup cron");
      const bucket = `gc-${crypto.randomUUID()}`;
      let expiredId = "";
      let liveId = "";
      const abortUpload = driver.abortUpload.bind(driver);
      let cleanupCalls = 0;
      driver.abortUpload = async (id) => {
        if (id === expiredId) {
          cleanupCalls += 1;
          if (cleanupCalls === 1) throw new Error("staging cleanup failed");
        }
        await abortUpload(id);
      };
      const host = await createChimpbase({
        projectDir: root, storage: engine === "postgres" ? { engine, url: pgUrl } : { engine },
        blobs: { driver, buckets: [bucket] },
        registrations: [
          ...plugin.registrations,
          action("seedExpired", async (ctx) => {
            const expired = await ctx.blobs.createUpload(bucket, "expired.txt");
            expiredId = expired.id;
            await expired.writePart(1, new TextEncoder().encode("expired bytes"));
            const live = await ctx.blobs.createUpload(bucket, "live.txt");
            liveId = live.id;
            await live.writePart(1, new TextEncoder().encode("live bytes"));
          }),
          action("expireUpload", async (ctx) => await ctx.db.query("UPDATE _chimpbase_blob_uploads SET expires_at_ms = 0 WHERE upload_id = ?1", [expiredId])),
          action("runGc", async (ctx) => {
            const fireAtMs = Date.now();
            await gc.handler(ctx, { fireAt: new Date(fireAtMs).toISOString(), fireAtMs, name: gc.name, schedule: gc.schedule });
          }),
          action("inspectGc", async (ctx) => ({
            uploads: await ctx.db.query("SELECT upload_id FROM _chimpbase_blob_uploads WHERE bucket = ?1 ORDER BY upload_id", [bucket], v.object({ upload_id: v.string() })),
            parts: await ctx.db.query("SELECT upload_id FROM _chimpbase_blob_upload_parts WHERE upload_id IN (?1, ?2) ORDER BY upload_id", [expiredId, liveId], v.object({ upload_id: v.string() })),
          })),
          action("cleanupGcFixture", async (ctx) => {
            const adapter = host.engine.getBlobsAdapter();
            for (const id of [expiredId, liveId]) await adapter.blobAbortUpload(id);
            await ctx.blobs.deleteMany(bucket, ["expired.txt", "live.txt"]);
          }),
        ],
      });
      const inspect = async () => (await host.executeAction("inspectGc")).result;
      try {
        await host.executeAction("seedExpired");
        const before = await inspect();
        await host.executeAction("runGc");
        expect(await inspect()).toEqual(before);
        expect(cleanupCalls).toBe(0);
        await host.executeAction("expireUpload");
        await expect(host.executeAction("runGc")).rejects.toThrow("staging cleanup failed");
        expect(await inspect()).toEqual(before);
        expect((await host.routeEnv().blobs.resumeUpload(expiredId)).id).toBe(expiredId);
        if (useFs) {
          const [part] = await host.engine.getBlobsAdapter().blobListParts(expiredId);
          if (part === undefined) throw new Error("missing expired upload part");
          expect(await Bun.file(part.driverRef).text()).toBe("expired bytes");
        }
        await host.executeAction("runGc");
        expect(await inspect()).toEqual({ uploads: [{ upload_id: liveId }], parts: [{ upload_id: liveId }] });
        await expect(host.routeEnv().blobs.resumeUpload(expiredId)).rejects.toThrow("not found");
        if (useFs) {
          await expect(stat(join(root, "_uploads", expiredId))).rejects.toMatchObject({ code: "ENOENT" });
          const [part] = await host.engine.getBlobsAdapter().blobListParts(liveId);
          if (part === undefined) throw new Error("missing live upload part");
          expect(await Bun.file(part.driverRef).text()).toBe("live bytes");
        } else {
          await expect(driver.assemble(expiredId, [{ partNumber: 1, driverRef: `${expiredId}/1` }], bucket, "expired.txt")).rejects.toThrow("missing upload");
        }
        await host.executeAction("runGc");
        expect(await inspect()).toEqual({ uploads: [{ upload_id: liveId }], parts: [{ upload_id: liveId }] });
        expect(cleanupCalls).toBe(2);
        const live = await host.routeEnv().blobs.resumeUpload(liveId);
        await live.complete();
        const object = await host.routeEnv().blobs.get(bucket, "live.txt");
        expect(object === null ? null : await new Response(object.body).text()).toBe("live bytes");
      } finally {
        try { await host.executeAction("cleanupGcFixture"); }
        finally { await host.close(); }
      }
    });
  }
}

for (const engine of ["memory", "sqlite", "postgres"] as const) {
  const pgUrl = process.env.CHIMPBASE_TEST_PG_URL;
  (engine === "postgres" && !pgUrl ? test.skip : test)(`literal prefixes preserve KV, blob and multipart pagination (${engine})`, async () => {
    const projectDir = await mkdtemp(join(tmpdir(), "chimpbase-literal-prefix-"));
    cleanupDirs.push(projectDir);
    const bucket = `prefix-${crypto.randomUUID()}`;
    const keys = ["user_one", "user_two", "userXtwo", "literal%one", "literal%two", "literalXtwo", "bang!one", "bangXone", "bang!_one", "bang!%one", "slash\\one", "slashXone", "ordinary/one"];
    const uploadIds: string[] = [];
    const host = await createChimpbase({
      projectDir, storage: engine === "postgres" ? { engine, url: pgUrl } : { engine },
      blobs: { driver: memoryBlobDriver(), buckets: [bucket] },
      registrations: [
        action("seedPrefixes", async (ctx) => {
          for (const key of keys) {
            await ctx.kv.set(key, true);
            await ctx.blobs.put(bucket, key, new Uint8Array([1]));
            uploadIds.push((await ctx.blobs.createUpload(bucket, key)).id);
          }
        }),
        action("inspectPrefixes", async (ctx, prefix: string) => {
          const blobs: string[] = [];
          const uploads: string[] = [];
          let cursor: string | undefined;
          for (let i = 0; i <= keys.length; i++) {
            const page = await ctx.blobs.list(bucket, { prefix, cursor, limit: 1 });
            blobs.push(...page.entries.map((entry) => entry.key));
            if (page.nextCursor === null) break;
            if (page.nextCursor <= (cursor ?? "") || i === keys.length) throw new Error("blob cursor did not advance");
            cursor = page.nextCursor;
          }
          cursor = undefined;
          for (let i = 0; i <= keys.length; i++) {
            const page = await ctx.blobs.listUploads(bucket, { prefix, cursor, limit: 1 });
            uploads.push(...page.uploads.map((entry) => entry.key));
            if (page.nextCursor === null) break;
            if (page.nextCursor <= (cursor ?? "") || i === keys.length) throw new Error("upload cursor did not advance");
            cursor = page.nextCursor;
          }
          return { kv: await ctx.kv.list({ prefix }), blobs, uploads: uploads.sort() };
        }),
        action("cleanupPrefixes", async (ctx) => {
          for (const key of keys) await ctx.kv.delete(key);
          await ctx.blobs.deleteMany(bucket, keys);
          for (const id of uploadIds) await (await ctx.blobs.resumeUpload(id)).abort();
        }),
      ],
    });
    try {
      await host.executeAction("seedPrefixes");
      for (const prefix of ["user_", "literal%", "bang!", "bang!_", "bang!%", "slash\\", "ordinary/", "", "missing"]) {
        const expected = keys.filter((key) => key.startsWith(prefix)).sort();
        expect((await host.executeAction("inspectPrefixes", prefix)).result).toEqual({ kv: expected, blobs: expected, uploads: expected });
      }
    } finally {
      try { await host.executeAction("cleanupPrefixes"); }
      finally { await host.close(); }
    }
  });
}

const paginationFixtures = [
  { name: "folders", keys: ["a/one", "b/one", "c/one"], delimiter: "/", expected: ["a/", "b/", "c/"] },
  { name: "large folder", keys: [...Array.from({ length: 1002 }, (_, i) => `a/${String(i).padStart(4, "0")}`), "b/one", "b/two", "c/one"], delimiter: "/", expected: ["a/", "b/", "c/"] },
  { name: "one full batch", keys: Array.from({ length: 1000 }, (_, i) => `a/${String(i).padStart(4, "0")}`), delimiter: "/", expected: ["a/"] },
  { name: "mixed files and folders", keys: ["a.txt", "b/one", "b/two", "c.txt", "d/one", "d/two", "e.txt"], delimiter: "/", expected: ["a.txt", "b/", "c.txt", "d/", "e.txt"] },
  { name: "files", keys: ["a/one", "a/two", "b/one"], delimiter: "", expected: ["a/one", "a/two", "b/one"] },
  { name: "custom prefix and delimiter", keys: ["docs::a::one", "docs::a::two", "docs::b::one", "docs::file", "other"], prefix: "docs::", delimiter: "::", expected: ["docs::a::", "docs::b::", "docs::file"] },
  { name: "empty", keys: [], delimiter: "/", expected: [] },
];

for (const engine of ["memory", "sqlite", "postgres"] as const) {
  for (const fixture of paginationFixtures) {
    for (const limit of [1, 2]) {
      const pgUrl = process.env.CHIMPBASE_TEST_PG_URL;
      const runTest = engine === "postgres" && !pgUrl ? test.skip : test;
      runTest(`blob pagination walks ${fixture.name} with limit ${limit} (${engine})`, async () => {
        const projectDir = await mkdtemp(join(tmpdir(), "chimpbase-blobs-pagination-"));
        cleanupDirs.push(projectDir);
        const bucket = `pagination-${crypto.randomUUID()}`;
        const host = await createChimpbase({
          projectDir,
          storage: engine === "postgres" ? { engine, url: pgUrl } : { engine },
          blobs: { driver: memoryBlobDriver(), buckets: [bucket] },
          registrations: [action("seedPagination", async (ctx) => {
            for (const key of fixture.keys) await ctx.blobs.put(bucket, key, new Uint8Array([1]));
          }), action("cleanupPagination", async (ctx) => await ctx.blobs.deleteMany(bucket, fixture.keys))],
        });
        try {
          await host.executeAction("seedPagination");
          const returned: string[] = [];
          let cursor: string | undefined;
          const blobs = host.routeEnv().blobs;
          for (let pageNumber = 0; pageNumber <= fixture.expected.length; pageNumber++) {
            const page = await blobs.list(bucket, { prefix: fixture.prefix, delimiter: fixture.delimiter, limit, cursor });
            const results = [...page.entries.map((entry) => entry.key), ...page.commonPrefixes];
            expect(results.length).toBeLessThanOrEqual(limit);
            returned.push(...results);
            if (page.nextCursor === null) break;
            expect(results.length).toBe(limit);
            expect(page.nextCursor > (cursor ?? "")).toBe(true);
            cursor = page.nextCursor;
            if (pageNumber === fixture.expected.length) throw new Error("pagination did not terminate");
          }
          expect(returned.sort()).toEqual(fixture.expected);
          expect(new Set(returned).size).toBe(returned.length);
        } finally {
          try { await host.executeAction("cleanupPagination"); }
          finally { await host.close(); }
        }
      });
    }
  }
}

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
  let driver: ChimpbaseBlobDriver;
  if (options.useFs === true) {
    if (options.root === undefined) {
      throw new Error("filesystem blob tests require a root");
    }
    driver = fsBlobDriver({ root: options.root });
  } else {
    driver = memoryBlobDriver();
  }

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
    args: v.object({ bucket: v.string(), key: v.string(), op: v.enum(["get", "put"] as const), ttlSec: v.number() }),
    async handler(ctx, input) {
      return ctx.blobs.sign({
        bucket: input.bucket,
        key: input.key,
        op: input.op,
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
  return { host, started, plugin, driver, baseUrl: serverBaseUrl };
}

interface PutResult { size: number; etag: string }
interface ListResult { entries: { key: string }[]; commonPrefixes: string[]; nextCursor: string | null }
const idResultValidator = v.object({ id: v.string() });
const listResultValidator = v.object({
  commonPrefixes: v.string().array(),
  entries: v.object({ key: v.string() }).array(),
  nextCursor: v.string().nullable(),
});
const putResultValidator = v.object({
  etag: v.string(),
  size: v.number(),
});
const textResultValidator = v.object({ text: v.string() });


describe("chimpbase blobs primitive (memory driver)", () => {
  test("put/head/get/delete roundtrip", async () => {
    const { host, started } = await bootBlobsHost();
    try {
      const put = await host.executeAction("blobs.put", {
        bucket: "uploads", key: "hello.txt", body: "hello world",
      });
      const putResult = putResultValidator.parse(put.result, "blob put result");
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
      const listedResult = listResultValidator.parse(listed.result, "blob list result");
      expect(listedResult.entries.map((e) => e.key)).toEqual([
        "photos/a.jpg", "photos/b.jpg",
      ]);

      const grouped = await host.executeAction("blobs.list", {
        bucket: "uploads", delimiter: "/",
      });
      const groupedResult = listResultValidator.parse(grouped.result, "grouped blob list result");
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
      expect((textResultValidator.parse(copied.result, "copied blob result")).text).toBe("original");
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
      const completeResult = putResultValidator.parse(complete.result, "multipart completion result");
      expect(completeResult.size).toBe(9);
      expect(completeResult.etag.endsWith("-3")).toBe(true);

      const fetched = await host.executeAction("blobs.get", {
        bucket: "uploads", key: "big.txt",
      });
      expect((textResultValidator.parse(fetched.result, "fetched blob result")).text).toBe("aaabbbccc");
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
      const url = new URL(v.string().parse(signed.result, "signed blob URL"));
      const token = url.searchParams.get("token");
      if (token === null) throw new Error("signed URL missing token");
      const payload = plugin.signer.verify(token);
      if (payload === null) throw new Error("signed token failed verification");
      expect(payload.bucket).toBe("uploads");
      expect(payload.op).toBe("get");

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
      const uploadId = (idResultValidator.parse(aborted.result, "aborted upload result")).id;
      const rows = await host.executeAction("blobs.list", {
        bucket: "uploads", prefix: "aborted.txt",
      });
      expect((listResultValidator.parse(rows.result, "aborted upload list result")).entries).toEqual([]);
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

for (const useFs of [false, true]) {
  describe(`chimpbase blob transactions (${useFs ? "filesystem" : "memory"} driver)`, () => {
    for (const operation of ["put", "copy", "multipart", "delete", "deleteMany"]) {
      test(`${operation} preserves bytes and metadata on rollback and retires payloads on commit`, async () => {
        const root = useFs ? await mkdtemp(join(tmpdir(), "chimpbase-blobs-rollback-")) : undefined;
        if (root !== undefined) cleanupDirs.push(root);
        const { host, started, driver } = await bootBlobsHost({ useFs, root });
        const adapter = host.engine.getBlobsAdapter();
        const commit = adapter.commitTransaction.bind(adapter);
        const staged: ChimpbaseBlobMetaRow[] = [];
        const readPayload = async (row: ChimpbaseBlobMetaRow) => {
          const object = await driver.get(row.bucket, row.key, row.driverRef);
          return object === null ? null : await new Response(object.body).text();
        };
        try {
          for (const [key, body] of [["target.txt", "before"], ["second.txt", "keep"], ["source.txt", "after"]]) {
            await host.executeAction("blobs.put", { bucket: "uploads", key, body });
          }
          const before = await adapter.blobGetMetadata("uploads", "target.txt");
          const second = await adapter.blobGetMetadata("uploads", "second.txt");
          if (before === null || second === null) throw new Error("missing test blob metadata");
          host.register(
            action("blobs.mutate", async (ctx, fail: boolean) => {
              if (operation === "put") {
                await ctx.blobs.put("uploads", "target.txt", new TextEncoder().encode("after"));
              } else if (operation === "copy") {
                await ctx.blobs.copy({ bucket: "uploads", key: "source.txt" }, { bucket: "uploads", key: "target.txt" });
              } else if (operation === "multipart") {
                const upload = await ctx.blobs.createUpload("uploads", "target.txt");
                await upload.writePart(1, new TextEncoder().encode("after"));
                await upload.complete();
              } else if (operation === "delete") {
                expect(await ctx.blobs.delete("uploads", "target.txt")).toBe(true);
              } else {
                expect(await ctx.blobs.deleteMany("uploads", ["target.txt", "second.txt", "missing.txt"])).toEqual({
                  deleted: ["target.txt", "second.txt"], errors: [],
                });
              }
              const row = await adapter.blobGetMetadata("uploads", "target.txt");
              if (row !== null) staged.push(row);
              expect(await readPayload(before)).toBe("before");
              if (fail) throw new Error("failed after mutation");
            }),
            action("blobs.outerFail", async (ctx) => {
              await ctx.action("blobs.mutate", false);
              throw new Error("failed after nested action");
            }),
            action("blobs.innerFail", async (ctx) => {
              await ctx.action("blobs.mutate", true);
            }),
          );
          for (const failure of ["action", "outer", "inner", "commit"]) {
            if (failure === "commit") {
              adapter.commitTransaction = async () => { throw new Error("failed during commit"); };
            }
            const name = failure === "outer" ? "blobs.outerFail" : failure === "inner" ? "blobs.innerFail" : "blobs.mutate";
            await expect(host.executeAction(name, failure === "action")).rejects.toThrow(/failed/);
            adapter.commitTransaction = commit;
            expect(await adapter.blobGetMetadata("uploads", "target.txt")).toEqual(before);
            expect(await adapter.blobGetMetadata("uploads", "second.txt")).toEqual(second);
            expect(await readPayload(before)).toBe("before");
            expect(await readPayload(second)).toBe("keep");
            for (const row of staged.splice(0)) expect(await readPayload(row)).toBeNull();
            expect((await host.routeEnv().blobs.listUploads("uploads")).uploads).toEqual([]);
            if (root !== undefined && operation === "multipart") {
              expect(await readdir(join(root, "_uploads"))).toEqual([]);
            }
          }
          await host.executeAction("blobs.mutate", false);
          expect(await readPayload(before)).toBeNull();
          const after = await adapter.blobGetMetadata("uploads", "target.txt");
          if (operation === "delete" || operation === "deleteMany") {
            expect(after).toBeNull();
          } else {
            if (after === null) throw new Error("missing committed blob metadata");
            expect(after.driverRef).not.toBe(before.driverRef);
            expect(after.etag).not.toBe(before.etag);
            expect(await readPayload(after)).toBe("after");
          }
          expect(await readPayload(second)).toBe(operation === "deleteMany" ? null : "keep");
        } finally {
          adapter.commitTransaction = commit;
          await started.stop();
        }
      });
    }

    test("multipart replacements preserve committed bytes on handler or commit failure and clean retired parts", async () => {
      const root = useFs ? await mkdtemp(join(tmpdir(), "chimpbase-blobs-parts-")) : undefined;
      if (root !== undefined) cleanupDirs.push(root);
      const { host, started, driver } = await bootBlobsHost({ useFs, root });
      const adapter = host.engine.getBlobsAdapter();
      const commit = adapter.commitTransaction.bind(adapter);
      const stagedRefs: string[] = [];
      try {
        const upload = await host.routeEnv().blobs.createUpload("uploads", "target.txt");
        await upload.writePart(1, new TextEncoder().encode("before"));
        const originalParts = await adapter.blobListParts(upload.id);
        const original = originalParts[0];
        if (original === undefined) throw new Error("missing original upload part");
        host.register(action("blobs.replaceParts", async (ctx, fail: boolean) => {
          const resumed = await ctx.blobs.resumeUpload(upload.id);
          for (const [partNumber, body] of [[1, "intermediate"], [1, "after"], [2, "tail"]] as const) {
            await resumed.writePart(partNumber, new TextEncoder().encode(body));
            const part = (await adapter.blobListParts(upload.id)).find((part) => part.partNumber === partNumber);
            if (part === undefined) throw new Error("missing replacement upload part");
            stagedRefs.push(part.driverRef);
          }
          if (fail) throw new Error("failed after replacing parts");
        }));
        for (const failure of ["handler", "commit"]) {
          if (failure === "commit") adapter.commitTransaction = async () => { throw new Error("part commit failed"); };
          await expect(host.executeAction("blobs.replaceParts", failure === "handler"))
            .rejects.toThrow(failure === "handler" ? "failed after replacing parts" : "part commit failed");
          adapter.commitTransaction = commit;
          expect(await adapter.blobListParts(upload.id)).toEqual(originalParts);
          const preserved = await driver.get("uploads", "target.txt", original.driverRef);
          expect(preserved === null ? null : await new Response(preserved.body).text()).toBe("before");
          for (const ref of stagedRefs.splice(0)) expect(await driver.get("uploads", "target.txt", ref)).toBeNull();
          if (root !== undefined) expect(await readdir(join(root, "_uploads", upload.id))).toHaveLength(1);
        }
        await host.executeAction("blobs.replaceParts", false);
        expect(await driver.get("uploads", "target.txt", original.driverRef)).toBeNull();
        expect(await driver.get("uploads", "target.txt", stagedRefs[0]!)).toBeNull();
        await upload.complete();
        const completed = await host.routeEnv().blobs.get("uploads", "target.txt");
        expect(completed === null ? null : await new Response(completed.body).text()).toBe("aftertail");
        for (const ref of stagedRefs) expect(await driver.get("uploads", "target.txt", ref)).toBeNull();
      } finally {
        adapter.commitTransaction = commit;
        await started.stop();
        await host.close();
      }
    });

    test("failed part streams preserve the original part and leave no partial replacement", async () => {
      const root = useFs ? await mkdtemp(join(tmpdir(), "chimpbase-blobs-part-stream-")) : undefined;
      if (root !== undefined) cleanupDirs.push(root);
      const { host, started } = await bootBlobsHost({ useFs, root });
      try {
        const upload = await host.routeEnv().blobs.createUpload("uploads", "target.txt");
        await upload.writePart(1, new TextEncoder().encode("before"));
        const parts = await upload.listParts();
        const files = root === undefined ? [] : await readdir(join(root, "_uploads", upload.id));
        const body = new ReadableStream<Uint8Array>({
          start(controller) {
            controller.enqueue(new TextEncoder().encode("partial"));
            controller.error(new Error("part stream failed"));
          },
        });
        await expect(upload.writePart(1, body)).rejects.toThrow("part stream failed");
        expect(await upload.listParts()).toEqual(parts);
        if (root !== undefined) expect(await readdir(join(root, "_uploads", upload.id))).toEqual(files);
        await upload.complete();
        const completed = await host.routeEnv().blobs.get("uploads", "target.txt");
        expect(completed === null ? null : await new Response(completed.body).text()).toBe("before");
      } finally {
        await started.stop();
        await host.close();
      }
    });

    test("multipart completion and abort keep committed parts available after rollback", async () => {
      const root = useFs ? await mkdtemp(join(tmpdir(), "chimpbase-blobs-multipart-")) : undefined;
      if (root !== undefined) cleanupDirs.push(root);
      const { host, started } = await bootBlobsHost({ useFs, root });
      try {
        const blobs = host.routeEnv().blobs;
        await blobs.put("uploads", "target.txt", new TextEncoder().encode("before"));
        const before = await blobs.head("uploads", "target.txt");
        const upload = await blobs.createUpload("uploads", "target.txt");
        await upload.writePart(1, new TextEncoder().encode("after"));
        const parts = await upload.listParts();
        host.register(action("blobs.uploadFail", async (ctx, abort: boolean) => {
          const resumed = await ctx.blobs.resumeUpload(upload.id);
          if (abort) await resumed.abort();
          else await resumed.complete();
          throw new Error("failed after upload mutation");
        }));
        for (const abort of [false, true]) {
          await expect(host.executeAction("blobs.uploadFail", abort)).rejects.toThrow(/failed after upload mutation/);
          expect(await blobs.head("uploads", "target.txt")).toEqual(before);
          const preserved = await blobs.get("uploads", "target.txt");
          expect(preserved === null ? null : await new Response(preserved.body).text()).toBe("before");
          const resumed = await blobs.resumeUpload(upload.id);
          expect(await resumed.listParts()).toEqual(parts);
        }
        await (await blobs.resumeUpload(upload.id)).complete();
        const completed = await blobs.get("uploads", "target.txt");
        expect(completed === null ? null : await new Response(completed.body).text()).toBe("after");
        await expect(blobs.resumeUpload(upload.id)).rejects.toThrow(/not found/);
        if (root !== undefined) await expect(stat(join(root, "_uploads", upload.id))).rejects.toHaveProperty("code", "ENOENT");
      } finally {
        await started.stop();
      }
    });

    test("multiple replacements and newly created payloads follow the root transaction", async () => {
      const root = useFs ? await mkdtemp(join(tmpdir(), "chimpbase-blobs-replacements-")) : undefined;
      if (root !== undefined) cleanupDirs.push(root);
      const { host, started, driver } = await bootBlobsHost({ useFs, root });
      const adapter = host.engine.getBlobsAdapter();
      const staged: ChimpbaseBlobMetaRow[] = [];
      try {
        await host.executeAction("blobs.put", { bucket: "uploads", key: "target.txt", body: "before" });
        const before = await adapter.blobGetMetadata("uploads", "target.txt");
        if (before === null) throw new Error("missing test blob metadata");
        host.register(action("blobs.replaceMany", async (ctx, fail: boolean) => {
          for (const [key, body] of [["target.txt", "intermediate"], ["new.txt", "new"], ["target.txt", "final"]]) {
            await ctx.blobs.put("uploads", key, new TextEncoder().encode(body));
            const row = await adapter.blobGetMetadata("uploads", key);
            if (row !== null) staged.push(row);
          }
          if (fail) throw new Error("failed after replacements");
        }));
        await expect(host.executeAction("blobs.replaceMany", true)).rejects.toThrow(/failed after replacements/);
        expect(await adapter.blobGetMetadata("uploads", "target.txt")).toEqual(before);
        expect(await adapter.blobGetMetadata("uploads", "new.txt")).toBeNull();
        for (const row of staged.splice(0)) {
          expect(await driver.get(row.bucket, row.key, row.driverRef)).toBeNull();
        }
        await host.executeAction("blobs.replaceMany", false);
        expect(await driver.get(before.bucket, before.key, before.driverRef)).toBeNull();
        const intermediate = staged[0];
        if (intermediate === undefined) throw new Error("missing intermediate payload");
        expect(await driver.get(intermediate.bucket, intermediate.key, intermediate.driverRef)).toBeNull();
        const result = await host.executeAction("blobs.get", { bucket: "uploads", key: "target.txt" });
        expect(textResultValidator.parse(result.result).text).toBe("final");
        const created = await host.executeAction("blobs.get", { bucket: "uploads", key: "new.txt" });
        expect(textResultValidator.parse(created.result).text).toBe("new");
      } finally {
        await started.stop();
      }
    });

    test("failed input streams preserve the existing payload and leave no partial replacement", async () => {
      const root = useFs ? await mkdtemp(join(tmpdir(), "chimpbase-blobs-stream-")) : undefined;
      if (root !== undefined) cleanupDirs.push(root);
      const { host, started } = await bootBlobsHost({ useFs, root });
      try {
        const blobs = host.routeEnv().blobs;
        await blobs.put("uploads", "target.txt", new TextEncoder().encode("before"));
        const before = await blobs.head("uploads", "target.txt");
        const files = root === undefined ? [] : await readdir(join(root, "uploads", "objects"), { recursive: true });
        const body = new ReadableStream<Uint8Array>({
          start(controller) {
            controller.enqueue(new TextEncoder().encode("partial"));
            controller.error(new Error("input stream failed"));
          },
        });
        await expect(blobs.put("uploads", "target.txt", body)).rejects.toThrow(/input stream failed/);
        expect(await blobs.head("uploads", "target.txt")).toEqual(before);
        const preserved = await blobs.get("uploads", "target.txt");
        expect(preserved === null ? null : await new Response(preserved.body).text()).toBe("before");
        if (root !== undefined) {
          expect((await readdir(join(root, "uploads", "objects"), { recursive: true })).sort()).toEqual(files.sort());
        }
      } finally {
        await started.stop();
      }
    });
  });
}

test("filesystem put, copy and multipart support long ASCII and Unicode keys", async () => {
  const root = await mkdtemp(join(tmpdir(), "chimpbase-blobs-long-keys-"));
  cleanupDirs.push(root);
  const { host, started } = await bootBlobsHost({ useFs: true, root });
  try {
    const blobs = host.routeEnv().blobs;
    for (const key of ["a".repeat(230), "é".repeat(200)]) {
      await blobs.put("uploads", key, new TextEncoder().encode("before"));
      await blobs.copy({ bucket: "uploads", key }, { bucket: "uploads", key: `${key}.copy` });
      const upload = await blobs.createUpload("uploads", `${key}.multipart`);
      await upload.writePart(1, new TextEncoder().encode("before"));
      await upload.complete();
      for (const storedKey of [key, `${key}.copy`, `${key}.multipart`]) {
        const object = await blobs.get("uploads", storedKey);
        expect(object?.key).toBe(storedKey);
        expect(object === null ? null : await new Response(object.body).text()).toBe("before");
      }
    }
  } finally {
    await started.stop();
    await host.close();
  }
});

test("failed payload cleanup after commit preserves the new blob and records a warning", async () => {
  const { host, started, driver } = await bootBlobsHost();
  const remove = driver.delete.bind(driver);
  try {
    await host.executeAction("blobs.put", { bucket: "uploads", key: "target.txt", body: "before" });
    const before = await host.engine.getBlobsAdapter().blobGetMetadata("uploads", "target.txt");
    if (before === null) throw new Error("missing test blob metadata");
    driver.delete = async (bucket, key, driverRef) => {
      if (driverRef === before.driverRef) throw new Error("cleanup unavailable");
      await remove(bucket, key, driverRef);
    };
    await host.executeAction("blobs.put", { bucket: "uploads", key: "target.txt", body: "after" });
    const fetched = await host.executeAction("blobs.get", { bucket: "uploads", key: "target.txt" });
    expect(textResultValidator.parse(fetched.result).text).toBe("after");
    const warning = host.engine.drainTelemetryRecords().find((record) => record.kind === "log" && record.message === "blob payload cleanup failed");
    expect(warning).toMatchObject({
      kind: "log", level: "warn", message: "blob payload cleanup failed",
      attributes: { committed: true, error: "cleanup unavailable" },
    });
    await host.executeAction("blobs.put", { bucket: "uploads", key: "target.txt", body: "next" });
    const next = await host.executeAction("blobs.get", { bucket: "uploads", key: "target.txt" });
    expect(textResultValidator.parse(next.result).text).toBe("next");
  } finally {
    driver.delete = remove;
    await started.stop();
  }
});

describe("chimpbase blobs primitive (fs driver)", () => {
  for (const destination of [
    { bucket: "uploads", key: "source.txt" },
    { bucket: "uploads", key: "copy.txt" },
    { bucket: "archive", key: "copy.txt" },
  ]) {
    test(`copy to ${destination.bucket}/${destination.key} preserves a 256 KiB object and metadata`, async () => {
      const root = await mkdtemp(join(tmpdir(), "chimpbase-blobs-fs-copy-"));
      cleanupDirs.push(root);
      const { host, started } = await bootBlobsHost({ useFs: true, root });
      const body = "0123456789abcdef".repeat(16 * 1024);
      try {
        const blobs = host.routeEnv().blobs;
        const original = await blobs.put("uploads", "source.txt", new TextEncoder().encode(body), {
          contentType: "text/plain", metadata: { author: "tests" },
        });
        const copied = await blobs.copy({ bucket: "uploads", key: "source.txt" }, destination);
        expect(copied).toEqual({ ...original, ...destination });
        for (const object of [{ bucket: "uploads", key: "source.txt" }, destination]) {
          const result = await blobs.get(object.bucket, object.key);
          expect(result).not.toBeNull();
          if (result === null) throw new Error("copied object is missing");
          expect(result.size).toBe(256 * 1024);
          expect(result.etag).toBe(original.etag);
          expect(result.contentType).toBe("text/plain");
          expect(result.metadata).toEqual({ author: "tests" });
          expect(await new Response(result.body).text()).toBe(body);
        }
      } finally {
        await started.stop();
      }
    });
  }

  test("writes bytes under the configured root and lists via engine", async () => {
    const root = await mkdtemp(join(tmpdir(), "chimpbase-blobs-fs-"));
    cleanupDirs.push(root);
    const { host, started } = await bootBlobsHost({ useFs: true, root });
    try {
      const put = await host.executeAction("blobs.put", {
        bucket: "uploads", key: "nested/report.txt", body: "payload",
      });
      expect((putResultValidator.parse(put.result, "blob put result")).size).toBe(7);

      const diskRoot = join(root, "uploads", "objects");
      await expect(stat(diskRoot)).resolves.toHaveProperty("isDirectory");

      const listed = await host.executeAction("blobs.list", {
        bucket: "uploads", prefix: "nested/",
      });
      expect((listResultValidator.parse(listed.result, "blob list result")).entries).toHaveLength(1);

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
      const signedUrl = new URL(v.string().parse(signed.result, "signed blob URL"));
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
      const signedUrl = new URL(v.string().parse(signed.result, "signed blob URL"));
      const res = await fetch(`${boot.baseUrl}${signedUrl.pathname}?${signedUrl.searchParams.toString()}`, {
        method: "PUT",
        body: "uploaded via signed url",
      });
      expect(res.status).toBe(201);

      const fetched = await boot.host.executeAction("blobs.get", {
        bucket: "uploads", key: "uploaded.txt",
      });
      expect((textResultValidator.parse(fetched.result, "fetched blob result")).text).toBe("uploaded via signed url");
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
      const signedUrl = new URL(v.string().parse(signed.result, "signed blob URL"));
      const res = await fetch(`${boot.baseUrl}${signedUrl.pathname}?${signedUrl.searchParams.toString()}`);
      expect(res.status).toBe(401);
    } finally {
      await boot.started.stop();
    }
  });
});
