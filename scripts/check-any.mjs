#!/usr/bin/env node
// Reports unsafe `any` in authored TypeScript: explicit `any` annotations plus
// values that flow out of a platform boundary as `any` (or `any[]`). `any`
// erases the type evidence at package and runtime boundaries, so the repository
// gates on zero findings instead of trusting review to catch them.
//
// A value that is immediately given a declared type — `expr as T`,
// `expr satisfies T`, the initializer of an annotated declaration, or the
// `return` of a function with a declared return type — is not reported: the
// `any` stops there instead of spreading, which is what localizing platform
// interop behind a typed adapter looks like. Only the outermost expression of
// an `any` chain is reported, so one leaked value counts once.
import { dirname, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import ts from "typescript";

const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const configPath = resolve(repoRoot, "tsconfig.json");

const configFile = ts.readConfigFile(configPath, ts.sys.readFile);
if (configFile.error) {
  console.error(ts.flattenDiagnosticMessageText(configFile.error.messageText, "\n"));
  process.exit(2);
}

const parsed = ts.parseJsonConfigFileContent(configFile.config, ts.sys, repoRoot);
if (parsed.errors.length > 0) {
  for (const error of parsed.errors) {
    console.error(ts.flattenDiagnosticMessageText(error.messageText, "\n"));
  }
  process.exit(2);
}

const program = ts.createProgram(parsed.fileNames, parsed.options);
const checker = program.getTypeChecker();

const isAuthored = (fileName) =>
  !fileName.includes("/node_modules/")
  && !fileName.includes("/dist/")
  && !fileName.endsWith(".d.ts");

const isAnyType = (type) => Boolean(type.flags & ts.TypeFlags.Any);

const isAnyArrayType = (type) => {
  if (!checker.isArrayType(type) && !checker.isTupleType(type)) {
    return false;
  }

  const args = checker.getTypeArguments(type);
  return args.length > 0 && args.some((arg) => isAnyType(arg));
};

// Positions that name a declaration rather than read a value: reporting them
// would double-count the declaration's own annotation.
const isDeclarationName = (node) => {
  const parent = node.parent;
  if (parent && ts.isTypePredicateNode(parent) && parent.parameterName === node) {
    // The subject of `x is T` names a parameter; it reads no value.
    return true;
  }

  return Boolean(parent && "name" in parent && parent.name === node);
};

const isReadExpression = (node) =>
  ts.isIdentifier(node)
  || ts.isPropertyAccessExpression(node)
  || ts.isElementAccessExpression(node)
  || ts.isCallExpression(node)
  || ts.isNewExpression(node)
  || ts.isAwaitExpression(node)
  || ts.isNonNullExpression(node)
  || ts.isAsExpression(node);

const hasDeclaredType = (node) => Boolean(node && "type" in node && node.type);

// The declared type an expression is immediately handed to, if any.
const isAnnotatedBoundary = (node) => {
  const parent = node.parent;
  if (!parent) {
    return false;
  }

  if (ts.isAsExpression(parent) || ts.isSatisfiesExpression(parent) || ts.isTypeAssertionExpression(parent)) {
    return true;
  }

  if (
    (ts.isVariableDeclaration(parent) || ts.isPropertyDeclaration(parent) || ts.isParameter(parent))
    && parent.initializer === node
  ) {
    return hasDeclaredType(parent);
  }

  if (ts.isReturnStatement(parent)) {
    const fn = ts.findAncestor(parent, (candidate) => ts.isFunctionLike(candidate));
    return hasDeclaredType(fn);
  }

  return false;
};

// Inner nodes of an `any` chain are skipped so the leak is reported once, at
// the point where it either stops or escapes.
const isInsideAnyExpression = (node) => {
  const parent = node.parent;
  if (!parent || !ts.isExpressionNode(parent)) {
    return false;
  }

  const parentType = checker.getTypeAtLocation(parent);
  return isAnyType(parentType) || isAnyArrayType(parentType);
};

const findings = [];

const report = (node, message) => {
  const file = node.getSourceFile();
  const { line, character } = file.getLineAndCharacterOfPosition(node.getStart(file));
  findings.push({
    column: character + 1,
    file: relative(repoRoot, file.fileName),
    line: line + 1,
    message,
    text: node.getText(file).split("\n")[0].slice(0, 80),
  });
};

for (const file of program.getSourceFiles()) {
  if (!isAuthored(file.fileName)) {
    continue;
  }

  // Identifiers inside a type node name types, not values, so only the `any`
  // keyword itself is reported once the walk enters one.
  const visit = (node, inType = false) => {
    if (node.kind === ts.SyntaxKind.AnyKeyword) {
      report(node, "explicit `any` annotation");
    } else if (inType) {
      // nothing else in a type position reads a value
    } else if (isReadExpression(node) && !isDeclarationName(node)) {
      const type = checker.getTypeAtLocation(node);
      const isAny = isAnyType(type);
      if ((isAny || isAnyArrayType(type)) && !isAnnotatedBoundary(node) && !isInsideAnyExpression(node)) {
        report(node, isAny ? "expression is typed `any`" : "expression is typed `any[]`");
      }
    } else if (
      (ts.isParameter(node) || ts.isVariableDeclaration(node) || ts.isPropertyDeclaration(node)
        || ts.isBindingElement(node))
      && !("type" in node && node.type)
      && ts.isIdentifier(node.name)
    ) {
      // Binding patterns are reported through their individual binding elements,
      // so only named declarations are checked here.
      const type = checker.getTypeAtLocation(node.name);
      if (isAnyType(type) || isAnyArrayType(type)) {
        report(node, "declaration is inferred as `any`");
      }
    }

    ts.forEachChild(node, (child) => visit(child, inType || ts.isTypeNode(node)));
  };

  visit(file);
}

const asJson = process.argv.includes("--json");
if (asJson) {
  console.log(JSON.stringify({ count: findings.length, findings }, null, 2));
} else {
  const byFile = new Map();
  for (const finding of findings) {
    byFile.set(finding.file, (byFile.get(finding.file) ?? 0) + 1);
  }

  for (const finding of findings.slice(0, 100)) {
    console.log(`${finding.file}:${finding.line}:${finding.column} ${finding.message} — ${finding.text}`);
  }

  if (findings.length > 100) {
    console.log(`… ${findings.length - 100} more`);
  }

  if (byFile.size > 0) {
    console.log("\nby file:");
    for (const [file, count] of [...byFile].sort((left, right) => right[1] - left[1])) {
      console.log(`  ${count.toString().padStart(4)}  ${file}`);
    }
  }
}

console.error(`unsafe any findings: ${findings.length}`);
process.exit(findings.length === 0 ? 0 : 1);
