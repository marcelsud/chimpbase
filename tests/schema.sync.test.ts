import { afterAll, afterEach, beforeAll, describe, expect, test } from "bun:test";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { join, resolve } from "node:path";
import { tmpdir } from "node:os";

import {
  canUseDocker,
  startPostgresDocker,
  type PostgresDockerHandle,
} from "../packages/bun/src/postgres_docker.ts";
import { syncChimpbaseSchemaArtifacts } from "../packages/bun/src/schema.ts";

const dockerAvailable = await canUseDocker();
const cleanupDirs: string[] = [];

if (!dockerAvailable) {
  test.skip("schema sync requires Docker", () => {});
} else {
  describe("schema sync", () => {
    let postgres: PostgresDockerHandle;

    beforeAll(async () => {
      postgres = await startPostgresDocker();
    }, 30000);

    afterEach(async () => {
      while (cleanupDirs.length > 0) {
        const dir = cleanupDirs.pop();
        if ((dir !== undefined && dir.length > 0)) {
          await rm(dir, { recursive: true, force: true });
        }
      }
    });

    afterAll(async () => {
      await postgres?.stop();
    }, 30000);

    test("generates schema snapshot and types from postgres migrations", async () => {
      const projectDir = await createSchemaFixture("generate", [
        "CREATE TYPE account_status AS ENUM ('pending', 'active');",
        "",
        "CREATE TABLE accounts (",
        "  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,",
        "  email TEXT NOT NULL UNIQUE,",
        "  status account_status NOT NULL DEFAULT 'pending',",
        "  metadata JSONB,",
        "  tags TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],",
        "  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()",
        ");",
      ].join("\n"));

      const firstDatabase = await postgres.createDatabase("schema_generate");
      const generated = await syncChimpbaseSchemaArtifacts(projectDir, {
        databaseUrl: firstDatabase.url,
      });

      expect(generated.status).toBe("written");
      expect(generated.snapshot.tables).toEqual([
        expect.objectContaining({
          name: "accounts",
        }),
      ]);
      expect(generated.snapshot.enums).toEqual([
        {
          name: "account_status",
          schema: "public",
          values: ["pending", "active"],
        },
      ]);

      const typesFile = await readFile(generated.typesPath, "utf8");
      expect(typesFile).toContain('export type AccountStatus = "pending" | "active";');
      expect(typesFile).toContain('"id": GeneratedAlways<string>;');
      expect(typesFile).toContain('"status": Generated<AccountStatus>;');
      expect(typesFile).toContain('"metadata": unknown | null;');
      expect(typesFile).toContain('"tags": Generated<string[]>;');
      expect(typesFile).toContain('export interface Database {');
      expect(typesFile).toContain('"accounts": AccountsTable;');

      const secondDatabase = await postgres.createDatabase("schema_check");
      const checked = await syncChimpbaseSchemaArtifacts(projectDir, {
        check: true,
        databaseUrl: secondDatabase.url,
      });

      expect(checked.status).toBe("unchanged");
    }, 30000);

    test("fails check when migrations advance without regenerating schema artifacts", async () => {
      const projectDir = await createSchemaFixture("drift", [
        "CREATE TABLE accounts (",
        "  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,",
        "  email TEXT NOT NULL UNIQUE",
        ");",
      ].join("\n"));

      const initialDatabase = await postgres.createDatabase("schema_initial");
      await syncChimpbaseSchemaArtifacts(projectDir, {
        databaseUrl: initialDatabase.url,
      });

      await writeFile(
        resolve(projectDir, "migrations/postgres/002_add_name.sql"),
        [
          "ALTER TABLE accounts",
          "ADD COLUMN name TEXT NOT NULL DEFAULT '';",
        ].join("\n"),
      );

      const changedDatabase = await postgres.createDatabase("schema_changed");

      await expect(syncChimpbaseSchemaArtifacts(projectDir, {
        check: true,
        databaseUrl: changedDatabase.url,
      })).rejects.toThrow("generated schema types are out of date");
    }, 30000);

    test("generates schema artifacts from chimpbase.migrations.ts", async () => {
      const projectDir = await createTypedSchemaFixture("typed", [
        "CREATE TABLE typed_accounts (",
        "  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,",
        "  email TEXT NOT NULL UNIQUE",
        ");",
      ].join("\n"));

      const database = await postgres.createDatabase("schema_typed");
      const generated = await syncChimpbaseSchemaArtifacts(projectDir, {
        databaseUrl: database.url,
      });

      expect(generated.snapshot.tables).toEqual([
        expect.objectContaining({
          name: "typed_accounts",
        }),
      ]);

      const typesFile = await readFile(generated.typesPath, "utf8");
      expect(typesFile).toContain('"typed_accounts": TypedAccountsTable;');
    }, 30000);
  });
}

async function createSchemaFixture(label: string, migrationSql: string): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), `chimpbase-schema-${label}-`));
  cleanupDirs.push(dir);

  await mkdir(resolve(dir, "migrations/postgres"), { recursive: true });
  await writeFile(
    resolve(dir, "chimpbase.app.ts"),
    [
      "export default {",
      '  project: { name: "schema-test" },',
      "};",
    ].join("\n"),
  );
  await writeFile(resolve(dir, "migrations/postgres/001_init.sql"), migrationSql);

  return dir;
}

async function createTypedSchemaFixture(label: string, migrationSql: string): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), `chimpbase-schema-typed-${label}-`));
  cleanupDirs.push(dir);

  await writeFile(
    resolve(dir, "chimpbase.app.ts"),
    [
      'import migrations from "./chimpbase.migrations.ts";',
      "",
      "export default {",
      '  migrations,',
      '  project: { name: "schema-test" },',
      "};",
    ].join("\n"),
  );
  await writeFile(
    resolve(dir, "chimpbase.migrations.ts"),
    [
      "export default {",
      "  postgres: [",
      `    { name: "001_init", sql: ${JSON.stringify(migrationSql)} },`,
      "  ],",
      "};",
    ].join("\n"),
  );

  return dir;
}
