import { fileURLToPath } from "node:url";

import { normalizeProjectConfig } from "@chimpbase/core";
import { createChimpbase } from "@chimpbase/node";
import { createOtelSink } from "@chimpbase/otel";
import { loadLocalSecretStore } from "@chimpbase/tooling/secrets";

import app from "./chimpbase.app.ts";

const EXAMPLE_ENV_FILE = ".env";
const EXAMPLE_SECRETS_DIR = "run/secrets";
const projectDir = fileURLToPath(new URL(".", import.meta.url));

export async function createAdvancedApp() {
  const secrets = await loadLocalSecretStore(
    projectDir,
    normalizeProjectConfig({
      secrets: { dir: EXAMPLE_SECRETS_DIR, envFile: EXAMPLE_ENV_FILE },
    }),
  );

  const otelEndpoint = process.env.OTEL_EXPORTER_OTLP_ENDPOINT;
  const sinks = (otelEndpoint !== undefined && otelEndpoint.length > 0)
    ? [
        createOtelSink({
          endpoint: otelEndpoint,
          serviceName: process.env.OTEL_SERVICE_NAME ?? "node-advanced",
        }),
      ]
    : [];

  return await createChimpbase({
    ...app,
    projectDir,
    secrets,
    sinks,
  });
}

const invokedDirectly = import.meta.url === `file://${process.argv[1]}`;

if (invokedDirectly) {
  const chimpbase = await createAdvancedApp();
  await chimpbase.start();
}
