# Blob storage

`chimpbase/blobs` adds binary object storage through `ctx.blobs`. Install the public package with `bun add chimpbase`; no separate plugin package is needed.

See the [blob storage guide](../../docs/advanced/blobs.md) for driver setup, buckets, uploads, signed URLs and handler examples.

| Export | Purpose |
|--------|---------|
| `fsBlobDriver({ root })` | Filesystem storage under the configured root |
| `memoryBlobDriver()` | Temporary storage for tests |
| `chimpbaseBlobs(options)` | Signed URL routes and upload cleanup registrations |
| `createBlobSigner(options)` | Signing without route registration |

Custom drivers implement `ChimpbaseBlobDriver`, exported from `chimpbase/blobs` and `chimpbase/core`.

This directory is a private workspace implementation package. The public `chimpbase` package ships compiled JavaScript and TypeScript declarations.
