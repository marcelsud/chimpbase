import { normalizeProjectConfig } from "@chimpbase/core";
import { createChimpbase } from "@chimpbase/bun";
import { chimpbaseBlobs, fsBlobDriver, memoryBlobDriver } from "@chimpbase/blobs";
import { createOtelSink } from "@chimpbase/otel";
import { loadLocalSecretStore } from "@chimpbase/tooling/secrets";

import app from "./chimpbase.app.ts";

const EXAMPLE_ENV_FILE = ".env";
const EXAMPLE_SECRETS_DIR = "run/secrets";

export async function createAdvancedApp() {
  const secrets = await loadLocalSecretStore(
    import.meta.dir,
    normalizeProjectConfig({
      secrets: { dir: EXAMPLE_SECRETS_DIR, envFile: EXAMPLE_ENV_FILE },
    }),
  );

  const otelEndpoint = process.env.OTEL_EXPORTER_OTLP_ENDPOINT;
  const sinks = (otelEndpoint !== undefined && otelEndpoint.length > 0)
    ? [
        createOtelSink({
          endpoint: otelEndpoint,
          serviceName: process.env.OTEL_SERVICE_NAME ?? "bun-advanced",
        }),
      ]
    : [];

  const blobsRoot = process.env.ATTACHMENTS_ROOT;
  const driver = (blobsRoot !== undefined && blobsRoot.length > 0) ? fsBlobDriver({ root: blobsRoot }) : memoryBlobDriver();
  const blobsPlugin = chimpbaseBlobs({
    secret: process.env.BLOBS_SIGNING_SECRET ?? "advanced-example-secret",
    baseUrl: process.env.BLOBS_BASE_URL ?? "http://127.0.0.1:3000",
  });

  return await createChimpbase({
    ...app,
    projectDir: import.meta.dir,
    secrets,
    sinks,
    blobs: {
      driver,
      buckets: ["attachments"],
      signer: blobsPlugin.signer,
    },
    registrations: [...(app.registrations ?? []), ...blobsPlugin.registrations],
  });
}

if (import.meta.main) {
  const chimpbase = await createAdvancedApp();
  await chimpbase.start();
}
