import assert from "node:assert/strict";
import { spawn, spawnSync } from "node:child_process";
import { chmod, cp, mkdir, mkdtemp, readFile, readdir, rm, symlink, writeFile } from "node:fs/promises";
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

function extractSnippet(markdown, name, marker = "chimpbase-check") {
  const snippets = [...markdown.matchAll(/^```[a-z]+ ([a-z-]+):([a-z-]+)\r?\n([\s\S]*?)^```[ \t]*\r?$/gm)]
    .filter((match) => match[1] === marker && match[2] === name);
  assert.equal(snippets.length, 1, `expected one complete snippet marked ${marker}:${name}`);
  return snippets[0][3];
}

// Guard extraction itself: missing or duplicated blocks must fail the check.
const fixture = "```ts chimpbase-check:example\nconst value = 1;\n```\n";
assert.equal(extractSnippet(fixture, "example"), "const value = 1;\n");
assert.equal(extractSnippet(fixture.replaceAll("\n", "\r\n"), "example"), "const value = 1;\r\n");
assert.throws(() => extractSnippet(fixture, "missing"));
assert.throws(() => extractSnippet(fixture + fixture, "example"));
assert.equal(extractSnippet("```bash chimpbase-run:bun\nbunx chimpbase dev\n```\n", "bun", "chimpbase-run"), "bunx chimpbase dev\n");

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
  // Consumer code sees only the public package and its declared runtime dependencies.
  const stagedPackage = join(stageRoot, "node_modules/chimpbase");
  await mkdir(stagedPackage, { recursive: true });
  await cp(join(repoRoot, "package.json"), join(stagedPackage, "package.json"));
  for (const path of packageJson.files.filter((path) => path.startsWith("packages/"))) {
    await cp(join(repoRoot, path), join(stagedPackage, path), { recursive: true });
  }
  for (const dependency of [...Object.keys(packageJson.dependencies), "@types/node"]) {
    const target = join(stageRoot, "node_modules", dependency);
    await mkdir(dirname(target), { recursive: true });
    await symlink(join(repoRoot, "node_modules", dependency), target, "dir");
  }
  const binDir = join(stageRoot, "node_modules/.bin");
  await mkdir(binDir);
  for (const [name, path] of Object.entries(packageJson.bin)) {
    await chmod(join(stagedPackage, path), 0o755);
    await symlink(join(stagedPackage, path), join(binDir, name));
  }
  await writeFile(join(stageRoot, "package.json"), JSON.stringify({
    private: true, type: "module", dependencies: { chimpbase: packageJson.version },
  }));
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
  const gettingStarted = await readFile(join(repoRoot, "docs/getting-started.md"), "utf8");
  const configuration = await readFile(join(repoRoot, "docs/configuration.md"), "utf8");
  await writeFile(join(stageRoot, "chimpbase.app.ts"), extractSnippet(gettingStarted, "getting-started"));
  for (const [runtime, markdown] of [["bun", gettingStarted], ["node", configuration], ["deno", configuration]]) {
    await checkLaunch(runtime, extractSnippet(markdown, runtime, "chimpbase-run"), gettingStarted, binDir);
  }
} finally {
  await rm(stageRoot, { recursive: true, force: true });
}

function run(command, args, label) {
  const result = spawnSync(command, args, { cwd: stageRoot, encoding: "utf8", timeout: 20_000 });
  assert.equal(result.status, 0, `${label} failed:\n${result.error ?? ""}\n${result.stdout}\n${result.stderr}`);
}

async function checkLaunch(runtime, command, markdown, binDir) {
  await rm(join(stageRoot, "data"), { recursive: true, force: true });
  const env = { ...process.env, PATH: `${binDir}:${process.env.PATH}`, npm_config_offline: "true" };
  // Ignore caller configuration so this exercises the documented defaults.
  for (const name of Object.keys(env)) {
    if (name.startsWith("CHIMPBASE_") || name === "PORT" || name === "DATABASE_URL") delete env[name];
  }
  const child = spawn("bash", ["-eu", "-c", `exec ${command}`], { cwd: stageRoot, env, detached: true });
  let output = "";
  child.stdout.on("data", (data) => { output += data; });
  child.stderr.on("data", (data) => { output += data; });
  const exited = new Promise((resolve) => child.once("exit", resolve));
  try {
    let ready = false;
    for (let attempt = 0; attempt < 100; attempt++) {
      assert.equal(child.exitCode, null, `${runtime} launch exited before serving HTTP:\n${output}`);
      try {
        const response = await fetch("http://localhost:3000/health", { signal: AbortSignal.timeout(200) });
        if (response.ok && output.includes("listening on http://127.0.0.1:3000")) { ready = true; break; }
      } catch {}
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
    assert.ok(ready, `${runtime} documented launch did not start HTTP:\n${output}`);
    function request(name) {
      const script = extractSnippet(markdown, name, "chimpbase-request");
      const result = spawnSync("bash", ["-eu", "-c", script], { cwd: stageRoot, env, encoding: "utf8", timeout: 10_000 });
      assert.equal(result.status, 0, `${runtime} ${name} request failed:\n${result.stdout}\n${result.stderr}`);
      return result.stdout;
    }
    assert.deepEqual(JSON.parse(request("health")), { ok: true });
    const created = request("create");
    assert.match(created, /^HTTP\/\d(?:\.\d)? 201\b/);
    const note = JSON.parse(created.split(/\r?\n\r?\n/).slice(1).join("\n\n"));
    assert.equal(note.body, "First note");
    assert.equal(typeof note.id, "string");
    assert.deepEqual(JSON.parse(request("list")), [note]);
    console.log(`docs: ${runtime} documented launch and HTTP requests passed`);
  } finally {
    if (child.exitCode === null && child.signalCode === null) process.kill(-child.pid, "SIGTERM");
    await Promise.race([exited, new Promise((resolve) => setTimeout(resolve, 2_000))]);
    if (child.exitCode === null && child.signalCode === null) process.kill(-child.pid, "SIGKILL");
    await exited;
  }
}

console.log("Documentation imports and complete examples passed against the published package.");
