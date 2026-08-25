import { mkdir, readFile, writeFile } from "node:fs/promises";
import { join, resolve } from "node:path";

import { PostgresDialect, Kysely, type ColumnMetadata, type TableMetadata } from "kysely";
import { Pool } from "pg";

import { loadProjectAppDefinition } from "./app.ts";
import { loadProjectPostgresMigrations } from "./migrations.ts";
import { canUseDocker, startPostgresDocker } from "./postgres_docker.ts";

interface SchemaEnumSnapshot {
  name: string;
  schema: string;
  values: string[];
}

interface SchemaColumnSnapshot {
  dataType: string;
  dataTypeSchema?: string;
  hasDefaultValue: boolean;
  isAutoIncrementing: boolean;
  isNullable: boolean;
  name: string;
}

interface SchemaTableSnapshot {
  columns: SchemaColumnSnapshot[];
  isView: boolean;
  name: string;
  schema?: string;
}

export interface ChimpbaseSchemaSnapshot {
  dialect: "postgres";
  enums: SchemaEnumSnapshot[];
  formatVersion: 1;
  tables: SchemaTableSnapshot[];
}

export interface ChimpbaseSchemaSyncOptions {
  check?: boolean;
  databaseUrl?: string;
  dockerImage?: string;
  outputDir?: string;
}

export interface ChimpbaseSchemaSyncResult {
  outputDir: string;
  projectName: string;
  snapshot: ChimpbaseSchemaSnapshot;
  snapshotPath: string;
  status: "unchanged" | "written";
  typesPath: string;
}

interface IntrospectedEnumRow {
  enum_name: string;
  enum_value: string;
  schema_name: string;
}

export async function syncChimpbaseSchemaArtifacts(
  projectDirInput: string,
  options: ChimpbaseSchemaSyncOptions = {},
): Promise<ChimpbaseSchemaSyncResult> {
  const projectDir = resolve(projectDirInput);
  const outputDir = resolve(projectDir, options.outputDir ?? "db");
  const snapshotPath = join(outputDir, "schema.snapshot.json");
  const typesPath = join(outputDir, "schema.generated.ts");
  const snapshot = await generateChimpbaseSchemaSnapshot(projectDir, options);
  const nextSnapshotText = `${JSON.stringify(snapshot, null, 2)}\n`;
  const nextTypesText = renderSchemaTypes(snapshot);
  const [currentSnapshotText, currentTypesText] = await Promise.all([
    readOptionalText(snapshotPath),
    readOptionalText(typesPath),
  ]);
  const issues: string[] = [];

  if (currentSnapshotText !== nextSnapshotText) {
    issues.push(`schema snapshot is out of date: ${snapshotPath}`);
  }

  if (currentTypesText !== nextTypesText) {
    issues.push(`generated schema types are out of date: ${typesPath}`);
  }

  if ((options.check === true)) {
    if (issues.length > 0) {
      throw new Error(issues.join("\n"));
    }

    return {
      outputDir,
      projectName: await resolveProjectName(projectDir),
      snapshot,
      snapshotPath,
      status: "unchanged",
      typesPath,
    };
  }

  const changed = issues.length > 0;

  if (changed) {
    await mkdir(outputDir, { recursive: true });
    await Promise.all([
      writeFile(snapshotPath, nextSnapshotText),
      writeFile(typesPath, nextTypesText),
    ]);
  }

  return {
    outputDir,
    projectName: await resolveProjectName(projectDir),
    snapshot,
    snapshotPath,
    status: changed ? "written" : "unchanged",
    typesPath,
  };
}

async function generateChimpbaseSchemaSnapshot(
  projectDir: string,
  options: ChimpbaseSchemaSyncOptions,
): Promise<ChimpbaseSchemaSnapshot> {
  return await withIntrospectionDatabase(projectDir, options, async (databaseUrl) => {
    const pool = new Pool({
      connectionString: databaseUrl,
    });
    const db = new Kysely<Record<string, never>>({
      dialect: new PostgresDialect({ pool }),
    });

    try {
      const [tables, enums] = await Promise.all([
        db.introspection.getTables({ withInternalKyselyTables: false }),
        readEnums(pool),
      ]);

      return {
        dialect: "postgres",
        enums: normalizeEnums(enums),
        formatVersion: 1,
        tables: normalizeTables(tables),
      };
    } finally {
      await db.destroy();
    }
  });
}

async function withIntrospectionDatabase<TResult>(
  projectDir: string,
  options: ChimpbaseSchemaSyncOptions,
  callback: (databaseUrl: string) => Promise<TResult>,
): Promise<TResult> {
  if ((options.databaseUrl !== undefined && options.databaseUrl.length > 0)) {
    return await prepareSchemaDatabase(projectDir, options.databaseUrl, callback);
  }

  if (!await canUseDocker()) {
    throw new Error("docker is required for schema generate/check");
  }

  const docker = await startPostgresDocker({
    image: options.dockerImage,
  });

  try {
    const database = await docker.createDatabase("schema");
    return await prepareSchemaDatabase(projectDir, database.url, callback);
  } finally {
    await docker.stop();
  }
}

async function prepareSchemaDatabase<TResult>(
  projectDir: string,
  databaseUrl: string,
  callback: (databaseUrl: string) => Promise<TResult>,
): Promise<TResult> {
  const migrations = await loadProjectPostgresMigrations(projectDir);
  const pool = new Pool({
    connectionString: databaseUrl,
  });

  try {
    for (const migration of migrations) {
      await pool.query(migration.sql);
    }
  } finally {
    await pool.end();
  }

  return await callback(databaseUrl);
}

async function resolveProjectName(projectDir: string): Promise<string> {
  const app = await loadProjectAppDefinition(projectDir);
  if (!(app !== null)) {
    throw new Error(`missing chimpbase.app.ts in ${projectDir}`);
  }

  return app.project.name;
}

async function readEnums(pool: Pool): Promise<SchemaEnumSnapshot[]> {
  const result = await pool.query<IntrospectedEnumRow>(
    `
      SELECT
        ns.nspname AS schema_name,
        typ.typname AS enum_name,
        enum.enumlabel AS enum_value
      FROM pg_type typ
      INNER JOIN pg_namespace ns ON ns.oid = typ.typnamespace
      INNER JOIN pg_enum enum ON enum.enumtypid = typ.oid
      WHERE ns.nspname !~ '^pg_'
        AND ns.nspname <> 'information_schema'
      ORDER BY ns.nspname ASC, typ.typname ASC, enum.enumsortorder ASC
    `,
  );

  const enums = new Map<string, SchemaEnumSnapshot>();

  for (const row of result.rows) {
    const key = `${row.schema_name}.${row.enum_name}`;
    const current = enums.get(key) ?? {
      name: row.enum_name,
      schema: row.schema_name,
      values: [],
    };
    current.values.push(row.enum_value);
    enums.set(key, current);
  }

  return [...enums.values()];
}

function normalizeTables(tables: TableMetadata[]): SchemaTableSnapshot[] {
  return [...tables]
    .map((table) => ({
      columns: [...table.columns]
        .map((column) => normalizeColumn(column))
        .sort((left, right) => left.name.localeCompare(right.name)),
      isView: table.isView,
      name: table.name,
      schema: table.schema,
    }))
    .sort((left, right) =>
      `${left.schema ?? "public"}.${left.name}`.localeCompare(`${right.schema ?? "public"}.${right.name}`),
    );
}

function normalizeColumn(column: ColumnMetadata): SchemaColumnSnapshot {
  return {
    dataType: column.dataType,
    dataTypeSchema: column.dataTypeSchema,
    hasDefaultValue: column.hasDefaultValue,
    isAutoIncrementing: column.isAutoIncrementing,
    isNullable: column.isNullable,
    name: column.name,
  };
}

function normalizeEnums(enums: SchemaEnumSnapshot[]): SchemaEnumSnapshot[] {
  return [...enums]
    .map((entry) => ({
      ...entry,
      values: [...entry.values],
    }))
    .sort((left, right) => `${left.schema}.${left.name}`.localeCompare(`${right.schema}.${right.name}`));
}

async function readOptionalText(path: string): Promise<string | null> {
  try {
    return await readFile(path, "utf8");
  } catch {
    return null;
  }
}

function renderSchemaTypes(snapshot: ChimpbaseSchemaSnapshot): string {
  const imports = new Set<string>();
  const enumTypeNames = new Map<string, string>();
  const lines: string[] = [
    "// Generated by `chimpbase schema generate`. Do not edit manually.",
    "",
  ];

  for (const entry of snapshot.enums) {
    enumTypeNames.set(enumKey(entry.schema, entry.name), enumTypeName(entry));
  }

  const databaseLines = [
    "export interface Database {",
    ...snapshot.tables.map((table) => `  ${JSON.stringify(databaseTableKey(table))}: ${tableTypeName(table)};`),
    "}",
  ];

  for (const entry of snapshot.enums) {
    lines.push(`export type ${enumTypeName(entry)} = ${entry.values.map((value) => JSON.stringify(value)).join(" | ")};`);
    lines.push("");
  }

  for (const table of snapshot.tables) {
    lines.push(`export interface ${tableTypeName(table)} {`);

    for (const column of table.columns) {
      const type = renderColumnType(column, enumTypeNames, imports);
      lines.push(`  ${JSON.stringify(column.name)}: ${type};`);
    }

    lines.push("}");
    lines.push("");
  }

  if (imports.size > 0) {
    lines.splice(
      1,
      0,
      `import type { ${[...imports].sort().join(", ")} } from "kysely";`,
      "",
    );
  }

  lines.push(...databaseLines, "");
  return `${lines.join("\n")}\n`;
}

function renderColumnType(
  column: SchemaColumnSnapshot,
  enumTypeNames: Map<string, string>,
  imports: Set<string>,
): string {
  let type = mapTypeReference(column.dataType, column.dataTypeSchema, enumTypeNames);

  if (column.isNullable) {
    type = `${type} | null`;
  }

  if (column.isAutoIncrementing) {
    imports.add("GeneratedAlways");
    return `GeneratedAlways<${type}>`;
  }

  if (column.hasDefaultValue) {
    imports.add("Generated");
    return `Generated<${type}>`;
  }

  return type;
}

function mapTypeReference(
  dataType: string,
  dataTypeSchema: string | undefined,
  enumTypeNames: Map<string, string>,
): string {
  if (dataType.startsWith("_")) {
    const inner = mapTypeReference(dataType.slice(1), dataTypeSchema, enumTypeNames);
    return needsParens(inner) ? `(${inner})[]` : `${inner}[]`;
  }

  const enumType = enumTypeNames.get(enumKey(dataTypeSchema ?? "public", dataType));
  if ((enumType !== undefined && enumType.length > 0)) {
    return enumType;
  }

  switch (dataType) {
    case "bool":
      return "boolean";
    case "bytea":
      return "Uint8Array";
    case "float4":
    case "float8":
    case "int2":
    case "int4":
    case "oid":
      return "number";
    case "int8":
    case "numeric":
      return "string";
    case "json":
    case "jsonb":
      return "unknown";
    case "bpchar":
    case "cidr":
    case "date":
    case "inet":
    case "macaddr":
    case "macaddr8":
    case "name":
    case "pg_lsn":
    case "text":
    case "time":
    case "timestamp":
    case "timestamptz":
    case "timetz":
    case "uuid":
    case "varchar":
    case "xml":
      return "string";
    default:
      return "unknown";
  }
}

function databaseTableKey(table: SchemaTableSnapshot): string {
  return (table.schema !== undefined && table.schema.length > 0) && table.schema !== "public"
    ? `${table.schema}.${table.name}`
    : table.name;
}

function tableTypeName(table: SchemaTableSnapshot): string {
  return `${schemaPrefix(table.schema)}${pascalCase(table.name)}Table`;
}

function enumTypeName(entry: SchemaEnumSnapshot): string {
  return `${schemaPrefix(entry.schema)}${pascalCase(entry.name)}`;
}

function schemaPrefix(schema: string | undefined): string {
  return (schema !== undefined && schema.length > 0) && schema !== "public" ? pascalCase(schema) : "";
}

function enumKey(schema: string, name: string): string {
  return `${schema}.${name}`;
}

function pascalCase(value: string): string {
  return value
    .split(/[^a-zA-Z0-9]+/)
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join("");
}

function needsParens(value: string): boolean {
  return value.includes("|");
}
