const RUNTIME_STATEMENT = /^(?:WITH\b|SELECT\b|INSERT\s+INTO\b|UPDATE\b|DELETE\s+FROM\b)/i;
const MIGRATION_STATEMENT = /^(?:(?:CREATE|ALTER|DROP)\s+(?:MATERIALIZED\s+)?(?:TABLE|VIEW|SEQUENCE|INDEX)\b|CREATE\s+UNIQUE\s+INDEX\b|INSERT\s+INTO\b|UPDATE\b|DELETE\s+FROM\b)/i;

export function assertChimpbaseModuleRuntimeSql(
  moduleName: string,
  schema: string,
  sql: string,
): void {
  const sanitized = maskSqlLiteralsAndComments(sql).trim();
  const statements = splitSqlStatements(sanitized);
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
  const statements = splitSqlStatements(sanitized);
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
  const tokens = sqlTokens(sql);
  const canMoveObjects = operation === "migration" && /^ALTER\b/i.test(sql.trim());
  const schemaIdentifier = (index: number) => tokens[index]?.startsWith('"')
    ? tokens[index]?.slice(1, -1).replaceAll('""', '"')
    : tokens[index]?.toLowerCase();
  for (let index = 0; index < tokens.length; index++) {
    const referenced = schemaIdentifier(index);
    if (referenced?.startsWith("chimpbase_") && referenced !== schema) {
      throw new Error(`module ${moduleName} ${operation} cannot access schema ${referenced}; owned schema is ${schema}`);
    }
    if (referenced === "public" && tokens[index + 1] === ".") {
      throw new Error(`module ${moduleName} ${operation} cannot access schema public; owned schema is ${schema}`);
    }
    if (canMoveObjects && tokens[index]?.toLowerCase() === "set"
      && tokens[index + 1]?.toLowerCase() === "schema" && schemaIdentifier(index + 2) !== schema) {
      throw new Error(`module ${moduleName} migration cannot move objects to schema ${schemaIdentifier(index + 2)}; owned schema is ${schema}`);
    }
  }
}

function assertRelationReferences(
  moduleName: string,
  schema: string,
  sql: string,
  operation: string,
  allowCtes: boolean,
): void {
  const tokens = sqlTokens(sql);
  const word = (index: number) => tokens[index]?.startsWith('"') ? "" : tokens[index]?.toLowerCase();
  const identifier = (index: number) => tokens[index]?.startsWith('"')
    ? tokens[index]?.slice(1, -1).replaceAll('""', '"')
    : word(index);
  const contexts = [{ expression: false, from: false, relation: false, writeTarget: false, ctes: new Set<string>() }];
  const completedCtes = new Map<number, string>();

  for (let index = 0; index < tokens.length; index++) {
    const context = contexts[contexts.length - 1];
    const token = word(index);
    if (token === "with" && allowCtes && !context.expression) {
      const ctes = extractCtes(tokens, index + 1);
      if (word(index + 1) === "recursive") {
        for (const name of ctes.values()) context.ctes.add(name);
      } else {
        // Nonrecursive CTEs become visible only after checking their own query.
        for (const [end, name] of ctes) completedCtes.set(end, name);
      }
    }
    if (token === "(" || token === "[") {
      const query = /^(?:select|with|insert|update|delete|values|table)$/.test(word(index + 1) ?? "");
      contexts.push({
        expression: !query && !context.relation,
        from: context.relation && !query,
        relation: context.relation && !query,
        writeTarget: context.writeTarget && !query,
        ctes: new Set(context.ctes),
      });
      context.relation = false;
      continue;
    }
    if (token === ")" || token === "]") {
      if (contexts.length > 1) contexts.pop();
      const name = completedCtes.get(index);
      if (name !== undefined) contexts[contexts.length - 1].ctes.add(name);
      continue;
    }
    if (context.relation) {
      if (token === "only" || token === "lateral") continue;
      if (context.writeTarget && ["temp", "temporary", "unlogged", "table"].includes(token ?? "")
        && word(index + 1) !== ".") continue;
      if (operation === "migration" && token === "if" && word(index - 1) === "table") {
        const exists = word(index + 1) === "not" ? index + 2 : index + 1;
        if (word(exists) === "exists") {
          index = exists;
          continue;
        }
      }
      const first = identifier(index);
      const second = word(index + 1) === "." ? identifier(index + 2) : undefined;
      if (first === undefined || !/^[a-z_]|^"/i.test(tokens[index] ?? "")) {
        throw new Error(`module ${moduleName} ${operation} contains an unsupported relation reference`);
      }
      if (second === undefined && (context.writeTarget || !context.ctes.has(first))) {
        throw new Error(`module ${moduleName} ${operation} must qualify owned relations with schema ${schema}`);
      }
      if (second !== undefined && first !== schema) {
        throw new Error(`module ${moduleName} ${operation} cannot access schema ${first}; owned schema is ${schema}`);
      }
      if (second !== undefined) index += 2;
      context.relation = false;
      context.writeTarget = false;
      continue;
    }
    if (token === "references" || (!context.expression && (
      token === "from" || token === "join" || token === "table"
      || token === "into"
      || (token === "update" && !["do", "for", "key"].includes(word(index - 1) ?? ""))
      || (token === "using" && word(index + 1) !== "(")
      || (token === "," && context.from)
      || (token === "on" && /^CREATE\s+(?:UNIQUE\s+)?INDEX\b/i.test(sql.trim()))
    ))) {
      context.relation = true;
      context.writeTarget = token === "into" || token === "update" || token === "references"
        || (token === "from" && word(index - 1) === "delete")
        || (token === "table" && operation === "migration");
      if (token === "from" || token === "join" || token === "using") context.from = true;
    }
    if (/^(?:where|group|order|having|limit|offset|window|union|intersect|except|returning|for|set|;)$/.test(token ?? "")) {
      context.from = false;
    }
  }
}

function sqlTokens(sql: string): string[] {
  return sql.match(/"(?:[^"]|"")*"|[a-z_][a-z0-9_$]*|[(),.\[\];]/gi) ?? [];
}

/** Returns statements with literals and comments masked for validation, not execution. */
export function splitSqlStatements(sql: string, dialect: "postgres" | "sqlite" = "postgres"): string[] {
  sql = maskSqlLiteralsAndComments(sql, dialect === "sqlite");
  const statements: string[] = [];
  let start = 0;
  for (const match of sql.matchAll(/"(?:[^"]|"")*"|`(?:[^`]|``)*`|\[[^\]]*\]|;/g)) {
    if (match[0] !== ";") continue;
    statements.push(sql.slice(start, match.index).trim());
    start = match.index + 1;
  }
  statements.push(sql.slice(start).trim());
  return statements.filter(Boolean);
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

function extractCtes(tokens: string[], start: number): Map<number, string> {
  const ctes = new Map<number, string>();
  const skipParentheses = (index: number) => {
    let depth = 0;
    do {
      if (tokens[index] === "(") depth++;
      if (tokens[index] === ")") depth--;
      index++;
    } while (index < tokens.length && depth > 0);
    return index;
  };
  let index = tokens[start]?.toLowerCase() === "recursive" ? start + 1 : start;
  while (index < tokens.length) {
    const name = tokens[index];
    if (name === undefined) break;
    index++;
    if (tokens[index] === "(") index = skipParentheses(index);
    if (tokens[index]?.toLowerCase() !== "as") break;
    index++;
    if (tokens[index]?.toLowerCase() === "not") index++;
    if (tokens[index]?.toLowerCase() === "materialized") index++;
    if (tokens[index] !== "(") break;
    index = skipParentheses(index);
    ctes.set(index - 1, name.startsWith('"') ? name.slice(1, -1).replaceAll('""', '"') : name.toLowerCase());
    if (tokens[index] !== ",") break;
    index++;
  }
  return ctes;
}

function maskSqlLiteralsAndComments(sql: string, sqlite = false): string {
  let result = "";
  for (let index = 0; index < sql.length;) {
    const current = sql[index];
    const next = sql[index + 1];
    if (current === "-" && next === "-") {
      const newline = sql.slice(index).search(sqlite ? /\n/ : /[\r\n]/);
      const length = newline < 0 ? sql.length - index : newline;
      result += " ".repeat(length);
      index += length;
      continue;
    }
    if (current === "/" && next === "*") {
      let end = index + 2;
      let depth = 1;
      while (end < sql.length && depth > 0) {
        if (!sqlite && sql.slice(end, end + 2) === "/*") { depth++; end += 2; }
        else if (sql.slice(end, end + 2) === "*/") { depth--; end += 2; }
        else end++;
      }
      const length = end - index;
      result += " ".repeat(length);
      index += length;
      continue;
    }
    if (current === "'") {
      let end = index + 1;
      const escaped = !sqlite && /[eE]/.test(sql[index - 1] ?? "") && !/[a-z0-9_$]/i.test(sql[index - 2] ?? "");
      while (end < sql.length) {
        if (escaped && sql[end] === "\\") {
          end += 2;
          continue;
        }
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
    const dollar = !sqlite && current === "$" && !/[a-z0-9_$]/i.test(sql[index - 1] ?? "")
      ? sql.slice(index).match(/^\$(?:[a-z_][a-z0-9_]*)?\$/i)?.[0] : undefined;
    if (dollar !== undefined) {
      const closing = sql.indexOf(dollar, index + dollar.length);
      const end = closing < 0 ? sql.length : closing + dollar.length;
      result += " ".repeat(end - index);
      index = end;
      continue;
    }
    if (current === '"' || (sqlite && (current === "`" || current === "["))) {
      const closing = current === "[" ? "]" : current;
      let end = index + 1;
      while (end < sql.length) {
        if (closing !== "]" && sql[end] === closing && sql[end + 1] === closing) { end += 2; continue; }
        if (sql[end++] === closing) break;
      }
      result += sql.slice(index, end);
      index = end;
      continue;
    }
    result += current;
    index += 1;
  }
  return result;
}
