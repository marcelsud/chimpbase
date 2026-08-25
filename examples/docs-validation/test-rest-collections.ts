import { createChimpbase } from "@chimpbase/bun";
import { restCollections } from "@chimpbase/rest-collections";
import { readJsonBody, v } from "@chimpbase/runtime";

const chimpbase = await createChimpbase({ storage: { engine: "memory" }, server: { port: 0 } });
chimpbase.register({ rest: restCollections({
  basePath: "/api",
  collections: {
    notes: {
      collection: "todo_notes",
      writableFields: ["body", "todoId"],
      filterableFields: { todoId: "number" },
    },
  },
}) });
await chimpbase.start();

// Create
const r1 = await chimpbase.executeRoute(new Request("http://test.local/api/notes", {
  method: "POST", headers: { "content-type": "application/json" },
  body: JSON.stringify({ body: "First note", todoId: 42 }),
}));
if (r1.response?.status !== 201) throw new Error("should create note");
const note = await r1.response!.json() as { id: string; body: string; todoId: number };
if (note.body !== "First note") throw new Error("body should match");

// List
const r2 = await chimpbase.executeRoute(new Request("http://test.local/api/notes"));
if (r2.response?.status !== 200) throw new Error("should list notes");
const notes = v.unknown().array().parse(await readJsonBody(r2.response), "notes");
if (notes.length !== 1) throw new Error("should have 1 note");

// Filter
const r3 = await chimpbase.executeRoute(new Request("http://test.local/api/notes?todoId=42"));
if (r3.response === null) throw new Error("filter should return a response");
const filtered = v.unknown().array().parse(await readJsonBody(r3.response), "filtered notes");
if (filtered.length !== 1) throw new Error("filter should return 1 note");

// Get by ID
const r4 = await chimpbase.executeRoute(new Request(`http://test.local/api/notes/${note.id}`));
if (r4.response?.status !== 200) throw new Error("should get note by id");

// Update
const r5 = await chimpbase.executeRoute(new Request(`http://test.local/api/notes/${note.id}`, {
  method: "PATCH", headers: { "content-type": "application/json" },
  body: JSON.stringify({ body: "Updated note" }),
}));
if (r5.response?.status !== 200) throw new Error("should update note");

// Delete
const r6 = await chimpbase.executeRoute(new Request(`http://test.local/api/notes/${note.id}`, { method: "DELETE" }));
if (r6.response?.status !== 204) throw new Error("should delete note");

console.log("rest-collections: OK"); await chimpbase.close(); process.exit(0);
