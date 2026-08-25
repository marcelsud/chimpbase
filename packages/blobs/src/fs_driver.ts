import { createHash } from "node:crypto";
import { createReadStream, createWriteStream } from "node:fs";
import type { Stats } from "node:fs";
import { mkdir, rename, rm, stat } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";

import type {
  ChimpbaseBlobDriver,
  ChimpbaseBlobDriverGetResult,
  ChimpbaseBlobDriverPutResult,
  ChimpbaseBlobDriverRange,
} from "@chimpbase/core";

export interface FsBlobDriverOptions {
  root: string;
  shardBytes?: number;
}

const UPLOADS_DIR = "_uploads";
const OBJECTS_DIR = "objects";

function encodeKey(key: string): string {
  return encodeURIComponent(key);
}

function shardPrefix(key: string, shardBytes: number): string {
  const digest = createHash("sha256").update(key).digest("hex");
  return digest.slice(0, Math.max(2, Math.min(shardBytes, 8)));
}

async function ensureDir(path: string): Promise<void> {
  await mkdir(path, { recursive: true });
}

function isByteReadableStream(value: unknown): value is ReadableStream<Uint8Array> {
  return value instanceof ReadableStream;
}

function isErrnoException(error: unknown): error is NodeJS.ErrnoException {
  return error instanceof Error
    && (!("code" in error) || error.code === undefined || typeof error.code === "string");
}

async function* readWebStream(
  stream: ReadableStream<Uint8Array>,
): AsyncGenerator<Uint8Array, void, undefined> {
  const reader = stream.getReader();
  try {
    while (true) {
      const chunk = await reader.read();
      if (chunk.done) return;
      yield chunk.value;
    }
  } finally {
    reader.releaseLock();
  }
}

function webToNode(stream: ReadableStream<Uint8Array>): Readable {
  return Readable.from(readWebStream(stream));
}

function nodeToWeb(stream: Readable): ReadableStream<Uint8Array> {
  const result: unknown = Readable.toWeb(stream);
  if (!isByteReadableStream(result)) {
    throw new TypeError("Node readable did not produce a web byte stream");
  }
  return result;
}

function toBuffer(chunk: unknown): Buffer {
  if (Buffer.isBuffer(chunk)) return chunk;
  if (typeof chunk === "string") return Buffer.from(chunk);
  if (chunk instanceof Uint8Array) return Buffer.from(chunk);
  throw new Error("unexpected stream chunk type");
}

export function fsBlobDriver(options: FsBlobDriverOptions): ChimpbaseBlobDriver {
  const root = resolve(options.root);
  const shardBytes = options.shardBytes ?? 2;

  const bucketRoot = (bucket: string) => join(root, bucket, OBJECTS_DIR);
  const uploadsRoot = (uploadId: string) => join(root, UPLOADS_DIR, uploadId);
  const objectPath = (bucket: string, key: string) =>
    join(bucketRoot(bucket), shardPrefix(key, shardBytes), encodeKey(key));

  return {
    async ensureBucket(bucket: string) {
      await ensureDir(join(root, bucket, OBJECTS_DIR));
    },
    async put(bucket, key, body) {
      const target = objectPath(bucket, key);
      await ensureDir(dirname(target));
      const hash = createHash("sha256");
      let size = 0;
      const tempTarget = `${target}.tmp-${Date.now()}`;
      const writeStream = createWriteStream(tempTarget);
      const reader = webToNode(body);
      reader.on("data", (chunk: unknown) => {
        const buf = toBuffer(chunk);
        hash.update(buf);
        size += buf.byteLength;
      });
      await pipeline(reader, writeStream);
      await rename(tempTarget, target);
      return { driverRef: target, size, sha256: hash.digest("hex") };
    },
    async get(_bucket, _key, driverRef, range): Promise<ChimpbaseBlobDriverGetResult | null> {
      let statResult: Stats;
      try {
        statResult = await stat(driverRef);
      } catch (error) {
        if (isErrnoException(error) && error.code === "ENOENT") return null;
        throw error;
      }
      const total = statResult.size;
      const start = (range !== undefined) ? Math.max(0, Math.floor(range.start)) : 0;
      const endExclusive = (range !== undefined) && range.end !== undefined
        ? Math.min(total, Math.floor(range.end) + 1)
        : total;
      if (endExclusive <= start) {
        return { body: new ReadableStream<Uint8Array>({ start(c) { c.close(); } }), size: 0 };
      }
      const nodeStream = createReadStream(driverRef, { start, end: endExclusive - 1 });
      return { body: nodeToWeb(nodeStream), size: endExclusive - start };
    },
    async delete(_bucket, _key, driverRef) {
      try {
        await rm(driverRef, { force: true });
      } catch (error) {
        if (!isErrnoException(error) || error.code !== "ENOENT") throw error;
      }
    },
    async copy(src, dst): Promise<ChimpbaseBlobDriverPutResult> {
      const source = src.driverRef;
      const target = objectPath(dst.bucket, dst.key);
      await ensureDir(dirname(target));
      const reader = createReadStream(source);
      const writer = createWriteStream(target);
      const hash = createHash("sha256");
      let size = 0;
      reader.on("data", (chunk: unknown) => {
        const buf = toBuffer(chunk);
        hash.update(buf);
        size += buf.byteLength;
      });
      await pipeline(reader, writer);
      return { driverRef: target, size, sha256: hash.digest("hex") };
    },
    async putPart(uploadId, partNumber, body) {
      const dir = uploadsRoot(uploadId);
      await ensureDir(dir);
      const target = join(dir, `part-${String(partNumber).padStart(6, "0")}`);
      const hash = createHash("sha256");
      let size = 0;
      const writer = createWriteStream(target);
      const reader = webToNode(body);
      reader.on("data", (chunk: unknown) => {
        const buf = toBuffer(chunk);
        hash.update(buf);
        size += buf.byteLength;
      });
      await pipeline(reader, writer);
      return { driverRef: target, size, sha256: hash.digest("hex") };
    },
    async assemble(uploadId, parts, finalBucket, finalKey) {
      const target = objectPath(finalBucket, finalKey);
      await ensureDir(dirname(target));
      const ordered = parts.slice().sort((a, b) => a.partNumber - b.partNumber);
      const hash = createHash("sha256");
      let size = 0;
      const writer = createWriteStream(target);
      try {
        for (const part of ordered) {
          const reader = createReadStream(part.driverRef);
          reader.on("data", (chunk: unknown) => {
            const buf = toBuffer(chunk);
            hash.update(buf);
            size += buf.byteLength;
          });
          await pipeline(reader, writer, { end: false });
        }
        writer.end();
        await new Promise<void>((resolveClose, rejectClose) => {
          writer.once("close", () => resolveClose());
          writer.once("error", rejectClose);
        });
      } catch (error) {
        writer.destroy();
        throw error;
      }
      try {
        await rm(uploadsRoot(uploadId), { recursive: true, force: true });
      } catch {
        /* best-effort cleanup */
      }
      return { driverRef: target, size, sha256: hash.digest("hex") };
    },
    async abortUpload(uploadId) {
      try {
        await rm(uploadsRoot(uploadId), { recursive: true, force: true });
      } catch (error) {
        if (!isErrnoException(error) || error.code !== "ENOENT") throw error;
      }
    },
  };
}
