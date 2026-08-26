const MODULE_SCHEMA = /\bchimpbase_[a-z0-9_]+\b/gi;
const RUNTIME_STATEMENT = /^(?:WITH\b|SELECT\b|INSERT\s+INTO\b|UPDATE\b|DELETE\s+FROM\b)/i;
const MIGRATION_STATEMENT = /^(?:(?:CREATE|ALTER|DROP)\s+(?:MATERIALIZED\s+)?(?:TABLE|VIEW|SEQUENCE|INDEX)\b|CREATE\s+UNIQUE\s+INDEX\b|INSERT\s+INTO\b|UPDATE\b|DELETE\s+FROM\b)/i;

export function assertChimpbaseModuleRuntimeSql(
  moduleName: string,
  schema: string,
  sql: string,
): void {
  const sanitized = maskSqlLiteralsAndComments(sql).trim();
  const statements = sanitized.split(";").map((entry) => entry.trim()).filter(Boolean);
  if (statements.length !== 1 || !RUNTIME_STATEMENT.test(statements[0] ?? "")) {
    throw new Error(
      `module ${moduleName} raw SQL allows one SELECT, INSERT, UPDATE, DELETE, or WITH statement`,
    );
  }
  assertKnownSchemas(moduleName, schema, sanitized, "raw SQL");
  assertRelationReferences(moduleName, schema, sanitized, "raw SQL", true);
}

export function assertChimpbaseModuleMigrationSql(
  moduleName: string,
  schema: string,
  sql: string,
): void {
  const sanitized = maskSqlLiteralsAndComments(sql);
  const statements = sanitized.split(";").map((entry) => entry.trim()).filter(Boolean);
  for (const statement of statements) {
    if (!MIGRATION_STATEMENT.test(statement)) {
      throw new Error(`module ${moduleName} migration contains an unsupported SQL statement`);
    }
    assertKnownSchemas(moduleName, schema, statement, "migration");
    assertRelationReferences(moduleName, schema, statement, "migration", true);
    assertMigrationObjectTarget(moduleName, schema, statement);
  }
}

export function assertChimpbaseModuleCompiledSql(schema: string, sql: string): void {
  assertChimpbaseModuleRuntimeSql(schema, schema, sql);
}

function assertKnownSchemas(
  moduleName: string,
  schema: string,
  sql: string,
  operation: string,
): void {
  for (const match of sql.matchAll(MODULE_SCHEMA)) {
    const referenced = match[0].toLowerCase();
    if (referenced !== schema) {
      throw new Error(`module ${moduleName} ${operation} cannot access schema ${referenced}; owned schema is ${schema}`);
    }
  }
  if (/\bpublic\s*\./i.test(sql)) {
    throw new Error(`module ${moduleName} ${operation} cannot access schema public; owned schema is ${schema}`);
  }
}

function assertRelationReferences(
  moduleName: string,
  schema: string,
  sql: string,
  operation: string,
  allowCtes: boolean,
): void {
  const ctes = allowCtes ? extractCteNames(sql) : new Set<string>();
  for (const match of sql.matchAll(
    /\b(?:DELETE\s+FROM|FROM|INSERT\s+INTO|JOIN|REFERENCES|UPDATE|USING)\s+(?:ONLY\s+)?"?([a-z_][a-z0-9_]*)"?(?:\s*\.\s*"?([a-z_][a-z0-9_]*)"?)?/gi,
  )) {
    const first = match[1]?.toLowerCase();
    const second = match[2]?.toLowerCase();
    if (first === undefined || (second === undefined && ctes.has(first))) continue;
    if (second === undefined) {
      throw new Error(`module ${moduleName} ${operation} must qualify owned relations with schema ${schema}`);
    }
    if (first !== schema) {
      throw new Error(`module ${moduleName} ${operation} cannot access schema ${first}; owned schema is ${schema}`);
    }
  }
}

function assertMigrationObjectTarget(moduleName: string, schema: string, statement: string): void {
  const target = statement.match(
    /^(?:CREATE\s+(?:UNIQUE\s+)?|ALTER\s+|DROP\s+)(?:MATERIALIZED\s+)?(?:TABLE|VIEW|SEQUENCE|INDEX)\s+(?:IF\s+(?:NOT\s+)?EXISTS\s+)?"?([a-z_][a-z0-9_]*)"?(?:\s*\.\s*"?([a-z_][a-z0-9_]*)"?)?/i,
  );
  if (target === null) return;
  const targetSchema = target[1]?.toLowerCase();
  const objectName = target[2]?.toLowerCase();
  if (targetSchema === undefined || objectName === undefined) {
    throw new Error(`module ${moduleName} migration must qualify owned objects with schema ${schema}`);
  }
  if (targetSchema !== schema) {
    throw new Error(`module ${moduleName} migration references foreign schema ${targetSchema}`);
  }
}

function extractCteNames(sql: string): Set<string> {
  return new Set(
    [...sql.matchAll(/\b(?:WITH(?:\s+RECURSIVE)?|,)\s*"?([a-z_][a-z0-9_]*)"?\s+AS\s*(?:NOT\s+MATERIALIZED\s*|MATERIALIZED\s*)?\(/gi)]
      .map((match) => match[1]?.toLowerCase())
      .filter((name): name is string => name !== undefined),
  );
}

function maskSqlLiteralsAndComments(sql: string): string {
  let result = "";
  for (let index = 0; index < sql.length;) {
    const current = sql[index];
    const next = sql[index + 1];
    if (current === "-" && next === "-") {
      const end = sql.indexOf("\n", index + 2);
      const length = (end < 0 ? sql.length : end) - index;
      result += " ".repeat(length);
      index += length;
      continue;
    }
    if (current === "/" && next === "*") {
      const end = sql.indexOf("*/", index + 2);
      const length = (end < 0 ? sql.length : end + 2) - index;
      result += " ".repeat(length);
      index += length;
      continue;
    }
    if (current === "'") {
      let end = index + 1;
      while (end < sql.length) {
        if (sql[end] === "'" && sql[end + 1] === "'") {
          end += 2;
          continue;
        }
        if (sql[end] === "'") {
          end += 1;
          break;
        }
        end += 1;
      }
      result += " ".repeat(end - index);
      index = end;
      continue;
    }
    result += current;
    index += 1;
  }
  return result;
}
