import { afterEach, describe, expect, test } from "bun:test";
import { mkdtemp, rm } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";

import { createChimpbase } from "../packages/bun/src/library.ts";
import { restCollections } from "../packages/rest-collections/src/index.ts";
import { readJsonResponse } from "./support/http.ts";

const cleanupDirs: string[] = [];

afterEach(async () => {
  while (cleanupDirs.length > 0) {
    const dir = cleanupDirs.pop();
    if (dir) {
      await rm(dir, { recursive: true, force: true });
    }
  }
});

describe("@chimpbase/rest-collections", () => {
  test("exposes CRUD endpoints for an explicit collection", async () => {
    const projectDir = await mkdtemp(join(tmpdir(), "chimpbase-rest-collections-"));
    cleanupDirs.push(projectDir);

    const host = await createChimpbase({
      project: { name: "rest-collections" },
      projectDir,
      storage: {
        engine: "memory",
      },
    });
    const restPlugin = restCollections({
      basePath: "/api",
      collections: {
        todoNotes: {
          collection: "todo_notes",
          filterableFields: { todoId: "number" },
          path: "/todo-notes",
          writableFields: ["body", "todoId"],
        },
      },
    });

    try {
      host.register({ restPlugin });

      const createOutcome = await host.executeRoute(
        new Request("http://rest.test/api/todo-notes", {
          body: JSON.stringify({
            body: "First note",
            todoId: 42,
          }),
          headers: {
            "content-type": "application/json",
          },
          method: "POST",
        }),
      );
      expect(createOutcome.response?.status).toBe(201);

      const created = await createOutcome.response?.json() as {
        body: string;
        id: string;
        todoId: number;
      };

      expect(created).toEqual({
        body: "First note",
        id: expect.any(String) as string,
        todoId: 42,
      });

      const listOutcome = await host.executeRoute(
        new Request("http://rest.test/api/todo-notes?todoId=42"),
      );
      expect(listOutcome.response?.status).toBe(200);
      expect(await readJsonResponse<Array<typeof created>>(listOutcome.response)).toEqual([created]);

      const getOutcome = await host.executeRoute(
        new Request(`http://rest.test/api/todo-notes/${created.id}`),
      );
      expect(getOutcome.response?.status).toBe(200);
      expect(await readJsonResponse<typeof created>(getOutcome.response)).toEqual(created);

      const updateOutcome = await host.executeRoute(
        new Request(`http://rest.test/api/todo-notes/${created.id}`, {
          body: JSON.stringify({
            body: "Updated note",
          }),
          headers: {
            "content-type": "application/json",
          },
          method: "PATCH",
        }),
      );
      expect(updateOutcome.response?.status).toBe(200);
      expect(await readJsonResponse<typeof created>(updateOutcome.response)).toEqual({
        ...created,
        body: "Updated note",
      });

      const deleteOutcome = await host.executeRoute(
        new Request(`http://rest.test/api/todo-notes/${created.id}`, {
          method: "DELETE",
        }),
      );
      expect(deleteOutcome.response?.status).toBe(204);

      const missingOutcome = await host.executeRoute(
        new Request(`http://rest.test/api/todo-notes/${created.id}`),
      );
      expect(missingOutcome.response?.status).toBe(404);
      expect(await readJsonResponse<{ error: string }>(missingOutcome.response)).toEqual({
        error: `document not found: ${created.id}`,
      });
    } finally {
      await host.close();
    }
  });

  test("returns 400 for invalid writes and unsupported filters", async () => {
    const projectDir = await mkdtemp(join(tmpdir(), "chimpbase-rest-collections-validation-"));
    cleanupDirs.push(projectDir);

    const host = await createChimpbase({
      project: { name: "rest-collections-validation" },
      projectDir,
      storage: {
        engine: "memory",
      },
    });
    const restPlugin = restCollections({
      basePath: "/api",
      collections: {
        todoNotes: {
          collection: "todo_notes",
          filterableFields: { todoId: "number" },
          path: "/todo-notes",
          writableFields: ["body", "todoId"],
        },
      },
    });

    try {
      host.register({ restPlugin });

      const invalidWriteOutcome = await host.executeRoute(
        new Request("http://rest.test/api/todo-notes", {
          body: JSON.stringify({
            body: "First note",
            extra: true,
            todoId: 42,
          }),
          headers: {
            "content-type": "application/json",
          },
          method: "POST",
        }),
      );
      expect(invalidWriteOutcome.response?.status).toBe(400);
      expect(await readJsonResponse<{ error: string }>(invalidWriteOutcome.response)).toEqual({
        error: 'field "extra" is not writable',
      });

      const invalidFilterOutcome = await host.executeRoute(
        new Request("http://rest.test/api/todo-notes?todoId=abc"),
      );
      expect(invalidFilterOutcome.response?.status).toBe(400);
      expect(await readJsonResponse<{ error: string }>(invalidFilterOutcome.response)).toEqual({
        error: "invalid number value: abc",
      });
    } finally {
      await host.close();
    }
  });

  test("supports schemaVersion with onWrite and onRead transforms", async () => {
    const projectDir = await mkdtemp(join(tmpdir(), "chimpbase-rest-collections-schema-"));
    cleanupDirs.push(projectDir);

    const host = await createChimpbase({
      project: { name: "rest-collections-schema" },
      projectDir,
      storage: {
        engine: "memory",
      },
    });
    const restPlugin = restCollections({
      basePath: "/api",
      collections: {
        users: {
          filterableFields: { email: "string" },
          onRead({ document, schemaVersion }) {
            return {
              email: document.email,
              id: document.id,
              name: schemaVersion === 1 ? document.name : document.fullName,
              schemaVersion,
            };
          },
          onWrite({ current, input }) {
            return {
              email: typeof input.email === "string"
                ? input.email.toLowerCase()
                : current?.email,
              fullName: typeof input.name === "string"
                ? input.name.trim()
                : (current?.fullName ?? current?.name),
              name: undefined,
            };
          },
          path: "/users",
          schemaVersion: 2,
          writableFields: ["email", "name"],
        },
      },
    });

    host.registerAction(
      "readStoredUser",
      async (ctx, id) => await ctx.collection.findOne("users", { id: id as string }),
    );
    host.registerAction(
      "readStoredUserMetadata",
      async (ctx, id) =>
        await ctx.collection.findOne("__chimpbase.rest.collection_metadata", {
          collectionName: "users",
          documentId: id as string,
        }),
    );
    host.registerAction(
      "seedLegacyUser",
      async (ctx) => await ctx.collection.insert("users", {
        email: "legacy@chimpbase.dev",
        name: "Legacy User",
        schemaVersion: 1,
      }),
    );

    try {
      host.register({ restPlugin });

      const createOutcome = await host.executeRoute(
        new Request("http://rest.test/api/users", {
          body: JSON.stringify({
            email: "ANA@CHIMPBASE.DEV",
            name: " Ana Silva ",
          }),
          headers: {
            "content-type": "application/json",
          },
          method: "POST",
        }),
      );
      expect(createOutcome.response?.status).toBe(201);

      const created = await createOutcome.response?.json() as {
        email: string;
        id: string;
        name: string;
        schemaVersion: number;
      };
      expect(created).toEqual({
        email: "ana@chimpbase.dev",
        id: expect.any(String) as string,
        name: "Ana Silva",
        schemaVersion: 2,
      });

      const storedAfterCreate = await host.executeAction("readStoredUser", [created.id]);
      expect(storedAfterCreate.result).toEqual({
        email: "ana@chimpbase.dev",
        fullName: "Ana Silva",
        id: created.id,
      });
      const metadataAfterCreate = await host.executeAction("readStoredUserMetadata", [created.id]);
      expect(metadataAfterCreate.result).toEqual({
        collectionName: "users",
        documentId: created.id,
        id: expect.any(String) as string,
        schemaVersion: 2,
      });

      const updateOutcome = await host.executeRoute(
        new Request(`http://rest.test/api/users/${created.id}`, {
          body: JSON.stringify({
            name: "Ana Maria",
          }),
          headers: {
            "content-type": "application/json",
          },
          method: "PATCH",
        }),
      );
      expect(updateOutcome.response?.status).toBe(200);
      expect(await readJsonResponse<typeof created>(updateOutcome.response)).toEqual({
        email: "ana@chimpbase.dev",
        id: created.id,
        name: "Ana Maria",
        schemaVersion: 2,
      });

      const storedAfterUpdate = await host.executeAction("readStoredUser", [created.id]);
      expect(storedAfterUpdate.result).toEqual({
        email: "ana@chimpbase.dev",
        fullName: "Ana Maria",
        id: created.id,
      });
      const metadataAfterUpdate = await host.executeAction("readStoredUserMetadata", [created.id]);
      expect(metadataAfterUpdate.result).toEqual({
        collectionName: "users",
        documentId: created.id,
        id: expect.any(String) as string,
        schemaVersion: 2,
      });

      const legacyUserId = (await host.executeAction("seedLegacyUser")).result as string;
      const legacyOutcome = await host.executeRoute(
        new Request(`http://rest.test/api/users/${legacyUserId}`),
      );
      expect(legacyOutcome.response?.status).toBe(200);
      expect(await readJsonResponse<typeof created>(legacyOutcome.response)).toEqual({
        email: "legacy@chimpbase.dev",
        id: legacyUserId,
        name: "Legacy User",
        schemaVersion: 1,
      });
    } finally {
      await host.close();
    }
  });
});
