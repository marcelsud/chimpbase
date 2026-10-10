import { createHash, randomUUID } from "node:crypto";

import type {
  ChimpbaseBlobDriver,
  ChimpbaseBlobDriverGetResult,
  ChimpbaseBlobDriverPutResult,
  ChimpbaseBlobDriverRange,
} from "@chimpbase/core";

interface BlobRecord {
  bytes: Uint8Array;
  sha256: string;
  uploadId?: string;
}

async function readAll(stream: ReadableStream<Uint8Array>): Promise<Uint8Array> {
  const reader = stream.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  try {
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      if ((value !== null && value !== undefined)) {
        chunks.push(value);
        total += value.byteLength;
      }
    }
  } finally {
    reader.releaseLock();
  }
  const out = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    out.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return out;
}

function hashBytes(bytes: Uint8Array): string {
  return createHash("sha256").update(bytes).digest("hex");
}

function streamFrom(bytes: Uint8Array): ReadableStream<Uint8Array> {
  return new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(bytes);
      controller.close();
    },
  });
}

function sliceRange(bytes: Uint8Array, range?: ChimpbaseBlobDriverRange): Uint8Array {
  if (!(range !== undefined)) return bytes;
  const start = Math.max(0, Math.floor(range.start));
  const endExclusive = range.end === undefined ? bytes.byteLength : Math.min(bytes.byteLength, Math.floor(range.end) + 1);
  if (endExclusive <= start) return new Uint8Array(0);
  return bytes.subarray(start, endExclusive);
}

export function memoryBlobDriver(): ChimpbaseBlobDriver {
  const blobs = new Map<string, BlobRecord>();
  const uploads = new Map<string, Set<string>>();
  const objectRef = (bucket: string, key: string) => `${bucket}/${key}/${randomUUID()}`;

  return {
    async ensureBucket(_bucket: string) {
      /* in-memory driver has no bucket state */
    },
    async put(bucket, key_, body) {
      const bytes = await readAll(body);
      const sha256 = hashBytes(bytes);
      const driverRef = objectRef(bucket, key_);
      blobs.set(driverRef, { bytes, sha256 });
      return { driverRef, size: bytes.byteLength, sha256 };
    },
    async get(_bucket, _key, driverRef, range): Promise<ChimpbaseBlobDriverGetResult | null> {
      const record = blobs.get(driverRef);
      if (!(record !== undefined)) return null;
      const slice = sliceRange(record.bytes, range);
      return { body: streamFrom(slice), size: slice.byteLength };
    },
    async delete(_bucket, _key, driverRef) {
      const uploadId = blobs.get(driverRef)?.uploadId;
      blobs.delete(driverRef);
      if (uploadId !== undefined) uploads.get(uploadId)?.delete(driverRef);
    },
    async copy(src, dst): Promise<ChimpbaseBlobDriverPutResult> {
      const record = blobs.get(src.driverRef);
      if (!(record !== undefined)) {
        throw new Error(`memory driver copy missing source ${src.bucket}/${src.key}`);
      }
      const clone = new Uint8Array(record.bytes);
      const driverRef = objectRef(dst.bucket, dst.key);
      blobs.set(driverRef, { bytes: clone, sha256: record.sha256 });
      return { driverRef, size: clone.byteLength, sha256: record.sha256 };
    },
    async putPart(uploadId, partNumber, body) {
      const bytes = await readAll(body);
      const sha256 = hashBytes(bytes);
      const driverRef = `${uploadId}/${partNumber}/${randomUUID()}`;
      const parts = uploads.get(uploadId) ?? new Set<string>();
      blobs.set(driverRef, { bytes, sha256, uploadId });
      parts.add(driverRef);
      uploads.set(uploadId, parts);
      return { driverRef, size: bytes.byteLength, sha256 };
    },
    async assemble(uploadId, parts, finalBucket, finalKey) {
      const staged = uploads.get(uploadId);
      if (!(staged !== undefined)) throw new Error(`memory driver assemble missing upload ${uploadId}`);
      const ordered = parts.slice().sort((a, b) => a.partNumber - b.partNumber);
      const buffers: Uint8Array[] = [];
      let total = 0;
      for (const part of ordered) {
        const record = staged.has(part.driverRef) ? blobs.get(part.driverRef) : undefined;
        if (!(record !== undefined)) throw new Error(`memory driver assemble missing part ${part.partNumber}`);
        buffers.push(record.bytes);
        total += record.bytes.byteLength;
      }
      const out = new Uint8Array(total);
      let offset = 0;
      for (const buf of buffers) {
        out.set(buf, offset);
        offset += buf.byteLength;
      }
      const sha256 = hashBytes(out);
      const driverRef = objectRef(finalBucket, finalKey);
      blobs.set(driverRef, { bytes: out, sha256 });
      return { driverRef, size: out.byteLength, sha256 };
    },
    async abortUpload(uploadId) {
      for (const driverRef of uploads.get(uploadId) ?? []) blobs.delete(driverRef);
      uploads.delete(uploadId);
    },
  };
}
