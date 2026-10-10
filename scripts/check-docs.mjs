import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { cp, mkdtemp, readFile, readdir, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const packageJson = JSON.parse(await readFile(join(repoRoot, "package.json"), "utf8"));
const cases = [
  {
    file: "docs/getting-started.md",
    name: "getting-started",
    check: `
import app from "./getting-started.ts";
import { createChimpbase } from "chimpbase/runtime/bun";
const host = await createChimpbase({ ...app, storage: { engine: "memory" } });
try {
  const note = (await host.executeAction("createNote", { body: "From an action" })).result;
  assert.equal(note.body, "From an action");
  assert.equal(typeof note.id, "string");
  const created = await host.executeRoute(new Request("http://test.local/notes", {
    method: "POST", headers: { "content-type": "application/json" },
    body: JSON.stringify({ body: "From HTTP" }),
  }));
  assert.equal(created.response?.status, 201);
  const fromHttp = await created.response.json();
  assert.equal(fromHttp.body, "From HTTP");
  const listed = await host.executeRoute(new Request("http://test.local/notes"));
  assert.equal(listed.response?.status, 200);
  const notes = await listed.response.json();
  assert.equal(notes.length, 2);
  assert.ok(notes.some((item) => item.id === note.id && item.body === note.body));
  assert.ok(notes.some((item) => item.id === fromHttp.id && item.body === fromHttp.body));
  const unmatched = await host.executeRoute(new Request("http://test.local/other"));
  assert.equal(unmatched.response, null);
  const wrongMethod = await host.executeRoute(new Request("http://test.local/notes", { method: "DELETE" }));
  assert.equal(wrongMethod.response, null);
  await assert.rejects(host.executeAction("createNote", { body: 42 }));
} finally {
  await host.close();
}
`,
  },
  { file: "docs/actions.md", name: "validators", check: "" },
  {
    file: "docs/advanced/workflows.md",
    name: "workflow-registration",
    check: `
import app from "./workflow-registration.ts";
import { createChimpbase } from "chimpbase/runtime/bun";
assert.equal(app.registrations[0].kind, "workflow");
assert.equal(app.registrations[0].definition.name, "customer.onboarding");
const host = await createChimpbase({ ...app, storage: { engine: "memory" } });
await host.close();
`,
  },
  {
    file: "docs/advanced/webhooks.md",
    name: "webhook-token",
    check: `
import { verifyCustomWebhook } from "./webhook-token.ts";
const request = (token, source = "partner") => new Request("http://test.local/webhooks/custom", {
  headers: { ...(token === undefined ? {} : { "x-custom-token": token }), "x-source-id": source },
});
const secret = (name) => name === "CUSTOM_WEBHOOK_SECRET" ? "expected" : null;
assert.equal(await verifyCustomWebhook(request("expected"), "{}", secret), true);
assert.equal(await verifyCustomWebhook(request(), "{}", () => null), false);
assert.equal(await verifyCustomWebhook(request("expected"), "{}", () => null), false);
assert.equal(await verifyCustomWebhook(request("expected"), "{}", () => ""), false);
assert.equal(await verifyCustomWebhook(request(), "{}", secret), false);
assert.equal(await verifyCustomWebhook(request(""), "{}", secret), false);
assert.equal(await verifyCustomWebhook(request("wrong"), "{}", secret), false);
assert.equal(await verifyCustomWebhook(request("expected", ""), "{}", secret), false);
`,
  },
  {
    file: "docs/advanced/webhooks.md",
    name: "webhook-signature",
    check: `
import { createHmac } from "node:crypto";
import { verifyWebhookSignature } from "./webhook-signature.ts";
const secret = "receiver-secret";
const body = '{"event":"order.created"}';
const timestamp = String(Math.floor(Date.now() / 1000));
const sign = (time) => "sha256=" + createHmac("sha256", secret).update(time + "." + body).digest("hex");
const signature = sign(timestamp);
assert.equal(verifyWebhookSignature(secret, signature, timestamp, body), true);
assert.equal(verifyWebhookSignature("", signature, timestamp, body), false);
assert.equal(verifyWebhookSignature("wrong", signature, timestamp, body), false);
assert.equal(verifyWebhookSignature(secret, signature, timestamp, body + " "), false);
for (const invalid of ["", "sha256=a", "sha256=" + "z".repeat(64), "x".repeat(64)]) {
  assert.equal(verifyWebhookSignature(secret, invalid, timestamp, body), false);
}
for (const invalid of ["", "NaN", "Infinity", timestamp + "x", "999999999999999999999999", "1.5"]) {
  assert.equal(verifyWebhookSignature(secret, signature, invalid, body), false);
}
const old = String(Number(timestamp) - 301);
assert.equal(verifyWebhookSignature(secret, sign(old), old, body), false);
`,
  },
];

function extractSnippet(markdown, name) {
  const snippets = [...markdown.matchAll(/^```ts chimpbase-check:([a-z-]+)\r?\n([\s\S]*?)^```[ \t]*\r?$/gm)]
    .filter((match) => match[1] === name);
  assert.equal(snippets.length, 1, `expected one complete snippet marked chimpbase-check:${name}`);
  return snippets[0][2];
}

// Guard extraction itself: missing or duplicated blocks must fail the check.
const fixture = "```ts chimpbase-check:example\nconst value = 1;\n```\n";
assert.equal(extractSnippet(fixture, "example"), "const value = 1;\n");
assert.equal(extractSnippet(fixture.replaceAll("\n", "\r\n"), "example"), "const value = 1;\r\n");
assert.throws(() => extractSnippet(fixture, "missing"));
assert.throws(() => extractSnippet(fixture + fixture, "example"));

async function markdownFiles(directory) {
  const files = [];
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    if (entry.name.startsWith(".")) continue;
    const path = join(directory, entry.name);
    if (entry.isDirectory()) files.push(...await markdownFiles(path));
    else if (entry.name.endsWith(".md")) files.push(path);
  }
  return files;
}

const publishedDocs = [
  ...await markdownFiles(join(repoRoot, "docs")),
  join(repoRoot, "README.md"),
  join(repoRoot, "docs/public/llms.txt"),
];
for (const entry of await readdir(join(repoRoot, "packages"), { withFileTypes: true })) {
  if (!entry.isDirectory()) continue;
  const path = join(repoRoot, "packages", entry.name, "README.md");
  try { await readFile(path); publishedDocs.push(path); }
  catch (error) { if (error.code !== "ENOENT") throw error; }
}
for (const file of publishedDocs) {
  const source = await readFile(file, "utf8");
  for (const match of source.matchAll(/\b(?:from\s*|import\s*(?:\(\s*)?)["']((?:@chimpbase\/|chimpbase\b)[^"']*)["']/g)) {
    const specifier = match[1];
    assert.ok(!specifier.startsWith("@chimpbase/"), `${relative(repoRoot, file)}: private workspace import ${specifier}`);
    const subpath = specifier === "chimpbase" ? "." : `.${specifier.slice("chimpbase".length)}`;
    assert.ok(packageJson.exports[subpath], `${relative(repoRoot, file)}: unpublished import ${specifier}`);
  }
}

const stageRoot = await mkdtemp(join(tmpdir(), "chimpbase-docs-check-"));
try {
  // Only published files enter the stage; workspace aliases cannot hide bad imports.
  await cp(join(repoRoot, "package.json"), join(stageRoot, "package.json"));
  for (const path of packageJson.files.filter((path) => path.startsWith("packages/"))) {
    await cp(join(repoRoot, path), join(stageRoot, path), { recursive: true });
  }
  await symlink(join(repoRoot, "node_modules"), join(stageRoot, "node_modules"), "dir");
  const snippetFiles = [];
  for (const testCase of cases) {
    const markdown = await readFile(join(repoRoot, testCase.file), "utf8");
    const source = extractSnippet(markdown, testCase.name);
    const snippetPath = join(stageRoot, `${testCase.name}.ts`);
    await writeFile(snippetPath, source);
    snippetFiles.push(snippetPath);
  }
  const configPath = join(stageRoot, "tsconfig.json");
  await writeFile(configPath, JSON.stringify({
    compilerOptions: {
      strict: true, skipLibCheck: true, noEmit: true,
      target: "ES2022", module: "NodeNext", moduleResolution: "NodeNext", types: ["node"],
    },
    files: snippetFiles,
  }));
  run(process.execPath, [join(repoRoot, "node_modules/typescript/bin/tsc"), "--project", configPath], "complete snippet types");
  for (const testCase of cases) {
    const harnessPath = join(stageRoot, `${testCase.name}.check.mjs`);
    await writeFile(harnessPath, `import assert from "node:assert/strict";\nimport "./${testCase.name}.ts";\n${testCase.check}`);
    run("bun", ["--tsconfig-override", configPath, harnessPath], testCase.name);
    console.log(`docs: ${testCase.name} passed`);
  }
} finally {
  await rm(stageRoot, { recursive: true, force: true });
}

function run(command, args, label) {
  const result = spawnSync(command, args, { cwd: stageRoot, encoding: "utf8", timeout: 20_000 });
  assert.equal(result.status, 0, `${label} failed:\n${result.error ?? ""}\n${result.stdout}\n${result.stderr}`);
}

console.log("Documentation imports and complete examples passed against the published package.");
