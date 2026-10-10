import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { resolve } from "node:path";
import ts from "typescript";

const paths = [
  "packages/bun/src/sqlite_adapter.ts",
  "packages/deno/src/sqlite_adapter.ts",
  "packages/node/src/sqlite_node_adapter.ts",
];
const operations = new Set([
  "collectionUpdate", "kvGet", "blobRecordPart",
  "blobFinalizeUpload", "blobAbortUpload", "blobListUploads",
]);
const printer = ts.createPrinter({ removeComments: true });
const groups = new Map<string, { operation: string; locations: string[]; lines: number }>();
for (const path of paths) {
  let text: string;
  try {
    text = await readFile(resolve(import.meta.dir, "..", path), "utf8");
  } catch (error) {
    if (error instanceof Error && "code" in error && error.code === "ENOENT") continue;
    throw error;
  }
  const source = ts.createSourceFile(path, text, ts.ScriptTarget.Latest, true, ts.ScriptKind.TS);
  function visit(node: ts.Node): void {
    if (ts.isMethodDeclaration(node) && ts.isIdentifier(node.name)
      && operations.has(node.name.text) && node.body !== undefined && node.body.statements.length >= 2) {
      const body = printer.printNode(ts.EmitHint.Unspecified, node.body, source);
      const fingerprint = `${node.name.text}:${createHash("sha256").update(body).digest("hex")}`;
      const group = groups.get(fingerprint) ?? {
        operation: node.name.text, locations: [], lines: body.split("\n").length,
      };
      const { line } = source.getLineAndCharacterOfPosition(node.getStart(source));
      group.locations.push(`${path}:${line + 1}`);
      groups.set(fingerprint, group);
    }
    ts.forEachChild(node, visit);
  }
  visit(source);
}
const duplicates = [...groups.values()].filter((group) => group.locations.length > 1);
console.log(JSON.stringify({ issue: 9, kind: "structural duplication", duplicates }, null, 2));
assert.deepEqual(duplicates, [], "SQLite storage operations must share their implementation across hosts");
