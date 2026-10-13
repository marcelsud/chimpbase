import { createHash, randomUUID } from "node:crypto";
import { createReadStream, createWriteStream } from "node:fs";
import { mkdir, open, rename, rm } from "node:fs/promises";
import { dirname, isAbsolute, join, relative, resolve, sep } from "node:path";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";

import type {
  ChimpbaseBlobDriver,
  ChimpbaseBlobDriverGetResult,
  ChimpbaseBlobDriverPutResult,
} from "@chimpbase/core";

export interface FsBlobDriverOptions {
  root: string;
  shardBytes?: number;
}

const UPLOADS_DIR = "_uploads";
const OBJECTS_DIR = "objects";

function shardPrefix(key: string, shardBytes: number): string {
  const digest = createHash("sha256").update(key).digest("hex");
  return digest.slice(0, Math.max(2, Math.min(shardBytes, 8)));
}

async function ensureDir(path: string): Promise<void> {
  await mkdir(path, { recursive: true });
}

function childPath(directory: string, ...segments: string[]): string {
  const path = resolve(directory, ...segments);
  const child = relative(directory, path);
  if (child === "" || child === ".." || child.startsWith(`..${sep}`) || isAbsolute(child)) {
    throw new Error("blob path must remain inside storage root");
  }
  return path;
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
  let completed = false;
  try {
    while (true) {
      const chunk = await reader.read();
      if (chunk.done) {
        completed = true;
        return;
      }
      yield chunk.value;
    }
  } finally {
    if (!completed) {
      try {
        await reader.cancel("Node stream consumer stopped");
      } catch {
      }
    }
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

async function writeBlob(
  target: string,
  reader: Readable,
  writePath = target,
): Promise<ChimpbaseBlobDriverPutResult> {
  const hash = createHash("sha256");
  let size = 0;
  reader.on("data", (chunk: unknown) => {
    const buf = toBuffer(chunk);
    hash.update(buf);
    size += buf.byteLength;
  });
  try {
    await pipeline(reader, createWriteStream(writePath));
    if (writePath !== target) await rename(writePath, target);
  } catch (error) {
    await rm(writePath, { force: true });
    throw error;
  }
  return { driverRef: target, size, sha256: hash.digest("hex") };
}

export function fsBlobDriver(options: FsBlobDriverOptions): ChimpbaseBlobDriver {
  const root = resolve(options.root);
  const shardBytes = options.shardBytes ?? 2;

  const bucketRoot = (bucket: string) => childPath(root, bucket, OBJECTS_DIR);
  const uploadsRoot = (uploadId: string) => childPath(join(root, UPLOADS_DIR), uploadId);
  const objectPath = (bucket: string, key: string) =>
    join(bucketRoot(bucket), shardPrefix(key, shardBytes), randomUUID());

  return {
    async ensureBucket(bucket: string) {
      await ensureDir(bucketRoot(bucket));
    },
    async put(bucket, key, body) {
      const target = objectPath(bucket, key);
      await ensureDir(dirname(target));
      return await writeBlob(target, webToNode(body), `${target}.tmp-${Date.now()}`);
    },
    async get(_bucket, _key, driverRef, range): Promise<ChimpbaseBlobDriverGetResult | null> {
      const source = childPath(root, driverRef);
      const handle = await open(source, "r").catch((error: unknown) => {
        if (isErrnoException(error) && error.code === "ENOENT") return null;
        throw error;
      });
      if (handle === null) return null;
      try {
        const { size: total } = await handle.stat();
        const start = (range !== undefined) ? Math.max(0, Math.floor(range.start)) : 0;
        const endExclusive = (range !== undefined) && range.end !== undefined
          ? Math.min(total, Math.floor(range.end) + 1)
          : total;
        if (endExclusive <= start) {
          await handle.close();
          return { body: new ReadableStream<Uint8Array>({ start(c) { c.close(); } }), size: 0 };
        }
        const nodeStream = handle.createReadStream({ start, end: endExclusive - 1 });
        return { body: nodeToWeb(nodeStream), size: endExclusive - start };
      } catch (error) {
        await handle.close().catch(() => {});
        throw error;
      }
    },
    async delete(_bucket, _key, driverRef) {
      try {
        await rm(childPath(root, driverRef), { force: true });
      } catch (error) {
        if (!isErrnoException(error) || error.code !== "ENOENT") throw error;
      }
    },
    async copy(src, dst): Promise<ChimpbaseBlobDriverPutResult> {
      const source = childPath(root, src.driverRef);
      const target = objectPath(dst.bucket, dst.key);
      await ensureDir(dirname(target));
      return await writeBlob(target, createReadStream(source));
    },
    async putPart(uploadId, partNumber, body) {
      const dir = uploadsRoot(uploadId);
      await ensureDir(dir);
      const target = join(dir, `part-${String(partNumber).padStart(6, "0")}-${randomUUID()}`);
      return await writeBlob(target, webToNode(body));
    },
    async assemble(uploadId, parts, finalBucket, finalKey) {
      const target = objectPath(finalBucket, finalKey);
      const ordered = parts.map((part) => ({ ...part, driverRef: childPath(root, part.driverRef) }))
        .sort((a, b) => a.partNumber - b.partNumber);
      await ensureDir(dirname(target));
      const reader = Readable.from((async function* (): AsyncGenerator<unknown, void, undefined> {
        for (const part of ordered) {
          yield* createReadStream(part.driverRef);
        }
      })());
      return await writeBlob(target, reader);
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
