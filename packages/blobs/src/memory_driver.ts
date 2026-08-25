import { createHash } from "node:crypto";

import type {
  ChimpbaseBlobDriver,
  ChimpbaseBlobDriverGetResult,
  ChimpbaseBlobDriverPutResult,
  ChimpbaseBlobDriverRange,
} from "@chimpbase/core";

interface BlobRecord {
  bytes: Uint8Array;
  sha256: string;
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
  const uploads = new Map<string, Map<number, BlobRecord>>();
  const key = (bucket: string, key: string) => `${bucket}\u0000${key}`;

  return {
    async ensureBucket(_bucket: string) {
      /* in-memory driver has no bucket state */
    },
    async put(bucket, key_, body) {
      const bytes = await readAll(body);
      const sha256 = hashBytes(bytes);
      blobs.set(key(bucket, key_), { bytes, sha256 });
      return { driverRef: `${bucket}/${key_}`, size: bytes.byteLength, sha256 };
    },
    async get(bucket, key_, _driverRef, range): Promise<ChimpbaseBlobDriverGetResult | null> {
      const record = blobs.get(key(bucket, key_));
      if (!(record !== undefined)) return null;
      const slice = sliceRange(record.bytes, range);
      return { body: streamFrom(slice), size: slice.byteLength };
    },
    async delete(bucket, key_, _driverRef) {
      blobs.delete(key(bucket, key_));
    },
    async copy(src, dst): Promise<ChimpbaseBlobDriverPutResult> {
      const record = blobs.get(key(src.bucket, src.key));
      if (!(record !== undefined)) {
        throw new Error(`memory driver copy missing source ${src.bucket}/${src.key}`);
      }
      const clone = new Uint8Array(record.bytes);
      blobs.set(key(dst.bucket, dst.key), { bytes: clone, sha256: record.sha256 });
      return { driverRef: `${dst.bucket}/${dst.key}`, size: clone.byteLength, sha256: record.sha256 };
    },
    async putPart(uploadId, partNumber, body) {
      const bytes = await readAll(body);
      const sha256 = hashBytes(bytes);
      const parts = uploads.get(uploadId) ?? new Map<number, BlobRecord>();
      parts.set(partNumber, { bytes, sha256 });
      uploads.set(uploadId, parts);
      return { driverRef: `${uploadId}/${partNumber}`, size: bytes.byteLength, sha256 };
    },
    async assemble(uploadId, parts, finalBucket, finalKey) {
      const staged = uploads.get(uploadId);
      if (!(staged !== undefined)) throw new Error(`memory driver assemble missing upload ${uploadId}`);
      const ordered = parts.slice().sort((a, b) => a.partNumber - b.partNumber);
      const buffers: Uint8Array[] = [];
      let total = 0;
      for (const part of ordered) {
        const record = staged.get(part.partNumber);
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
      blobs.set(key(finalBucket, finalKey), { bytes: out, sha256 });
      uploads.delete(uploadId);
      return { driverRef: `${finalBucket}/${finalKey}`, size: out.byteLength, sha256 };
    },
    async abortUpload(uploadId) {
      uploads.delete(uploadId);
    },
  };
}
