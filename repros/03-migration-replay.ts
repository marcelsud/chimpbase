import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { createChimpbase, type CreateChimpbaseOptions } from "../packages/bun/src/library.ts";

const projectDir = await mkdtemp(join(tmpdir(), "chimpbase-repro-03-"));
const options: CreateChimpbaseOptions = {
  projectDir,
  storage: { engine: "sqlite", path: "app.db" },
  secrets: { get: () => null },
  migrations: {
    sqlite: [{
      name: "001_create_items",
      sql: "CREATE TABLE repro_items (id INTEGER PRIMARY KEY)",
    }],
  },
};
try {
  const first = await createChimpbase(options);
  await first.close();
  console.log({ issue: 3, firstBoot: "passed", expectedSecondBoot: "passed" });

  const second = await createChimpbase(options);
  await second.close();
  console.log({ issue: 3, secondBoot: "passed" });
} finally {
  await rm(projectDir, { recursive: true, force: true });
}
