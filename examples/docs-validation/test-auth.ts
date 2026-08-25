import { createChimpbase } from "@chimpbase/bun";
import { chimpbaseAuth } from "@chimpbase/auth";
import { readJsonBody, v } from "@chimpbase/runtime";

const BOOTSTRAP_KEY = "test-bootstrap-key";
const chimpbase = await createChimpbase({
  storage: { engine: "memory" },
  server: { port: 0 },
  secrets: { get: (name: string) => name === "BOOTSTRAP_KEY" ? BOOTSTRAP_KEY : null },
});
chimpbase.register({ auth: chimpbaseAuth({ bootstrapKeySecret: "BOOTSTRAP_KEY" }) });
await chimpbase.start();

// Block without key
const r1 = await chimpbase.executeRoute(new Request("http://test.local/some-path"));
if (r1.response?.status !== 401) throw new Error("should block without key");

// Pass with bootstrap key
const r2 = await chimpbase.executeRoute(new Request("http://test.local/some-path", { headers: { "x-api-key": BOOTSTRAP_KEY } }));
if (r2.response !== null) throw new Error("should pass with bootstrap key");

// Health excluded
const r3 = await chimpbase.executeRoute(new Request("http://test.local/health"));
if (r3.response?.status === 401) throw new Error("health should be excluded");

// Create user
const r4 = await chimpbase.executeRoute(new Request("http://test.local/_auth/users", {
  method: "POST", headers: { "content-type": "application/json", "x-api-key": BOOTSTRAP_KEY },
  body: JSON.stringify({ email: "admin@test.com", name: "Admin" }),
}));
if (r4.response?.status !== 201) throw new Error("should create user");
const user = v.object({ id: v.string() }).parse(await readJsonBody(r4.response), "created user");

// Create API key
const r5 = await chimpbase.executeRoute(new Request(`http://test.local/_auth/users/${user.id}/keys`, {
  method: "POST", headers: { "content-type": "application/json", "x-api-key": BOOTSTRAP_KEY },
  body: JSON.stringify({ label: "test", scopes: ["read", "write"] }),
}));
if (r5.response?.status !== 201) throw new Error("should create key");
const keyData = v.object({
  key: v.string(),
  scopes: v.string().array(),
}).parse(await readJsonBody(r5.response), "created API key");
if (keyData.key.length !== 64) throw new Error("key should be 64 chars");
const scopes = keyData.scopes;
if (scopes[0] !== "read") throw new Error(`scopes should be [read,write], got: ${JSON.stringify(scopes)}`);

// Auth with generated key
const r6 = await chimpbase.executeRoute(new Request("http://test.local/some-path", { headers: { "x-api-key": keyData.key } }));
if (r6.response !== null) throw new Error("should pass with generated key");

// Scope enforcement: read key cannot POST
const r7 = await chimpbase.executeAction("__chimpbase.auth.createApiKey", [{ userId: user.id, scopes: ["read"] }]);
const readKey = v.object({ key: v.string() }).parse(r7.result, "read-only API key").key;
const r8 = await chimpbase.executeRoute(new Request("http://test.local/some-path", {
  method: "POST", headers: { "x-api-key": readKey },
}));
if (r8.response?.status !== 403) throw new Error("read key should not POST");

console.log("auth: OK"); await chimpbase.close(); process.exit(0);
