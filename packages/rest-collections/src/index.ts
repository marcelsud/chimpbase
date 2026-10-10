import {
  action,
  isJsonObject,
  plugin,
  readJsonBody,
  route,
  v,
  type ChimpbaseCollectionFilter,
  type ChimpbaseCollectionClient,
  type ChimpbasePluginDependency,
  type ChimpbasePluginRegistration,
  type ChimpbaseRegistrationSource,
} from "@chimpbase/runtime";

export type RestCollectionMethod =
  | "create"
  | "delete"
  | "get"
  | "list"
  | "update";

export type RestCollectionFilterValueParser = "boolean" | "number" | "string" | ((value: string) => unknown);

export type RestCollectionFilterFields =
  | readonly string[]
  | Record<string, RestCollectionFilterValueParser>;

export type RestCollectionReadOperation = "create" | "get" | "list" | "update";
export type RestCollectionWriteOperation = "create" | "update";

export interface RestCollectionReadContext<TDocument = Record<string, unknown>> {
  configuredSchemaVersion: number | null;
  document: TDocument;
  operation: RestCollectionReadOperation;
  schemaVersion: number | null;
}

export interface RestCollectionWriteContext<
  TInput = Record<string, unknown>,
  TDocument = Record<string, unknown>,
> {
  configuredSchemaVersion: number | null;
  current: TDocument | null;
  input: TInput;
  operation: RestCollectionWriteOperation;
  schemaVersion: number | null;
}

export interface RestCollectionDefinition<
  TWriteInput = Record<string, unknown>,
  TStoredDocument = Record<string, unknown>,
  TReadOutput = TStoredDocument,
> {
  collection?: string;
  defaultLimit?: number;
  filterableFields?: RestCollectionFilterFields;
  maxLimit?: number;
  methods?: readonly RestCollectionMethod[];
  onRead?: (
    context: RestCollectionReadContext<TStoredDocument>,
  ) => TReadOutput | Promise<TReadOutput>;
  onWrite?: (
    context: RestCollectionWriteContext<TWriteInput, TStoredDocument>,
  ) => Record<string, unknown> | Promise<Record<string, unknown>>;
  path?: string;
  schemaVersion?: number;
  writableFields?: readonly string[];
}

export interface RestCollectionsOptions {
  basePath?: string;
  collections: Record<string, RestCollectionDefinition>;
  defaultLimit?: number;
  dependsOn?: readonly ChimpbasePluginDependency[];
  maxLimit?: number;
  name?: string;
}

interface ResolvedRestCollectionDefinition {
  actionNames: {
    create: string;
    delete: string;
    get: string;
    list: string;
    update: string;
  };
  collectionName: string;
  defaultLimit: number;
  filterParsers: ReadonlyMap<string, (value: string) => unknown>;
  id: string;
  maxLimit: number;
  methods: ReadonlySet<RestCollectionMethod>;
  onRead?: RestCollectionDefinition["onRead"];
  onWrite?: RestCollectionDefinition["onWrite"];
  routePath: string;
  routeSegments: readonly string[];
  schemaVersion: number | null;
  writableFields: ReadonlySet<string> | null;
}

interface RestListInput {
  filter: ChimpbaseCollectionFilter;
  limit: number;
}

interface RestUpdateInput {
  id: string;
  patch: Record<string, unknown>;
}

interface RestCollectionMetadataRecord {
  collectionName: string;
  documentId: string;
  id: string;
  schemaVersion: number;
}

const DEFAULT_LIMIT = 50;
const DEFAULT_MAX_LIMIT = 100;
const REST_METHODS: readonly RestCollectionMethod[] = ["list", "get", "create", "update", "delete"];
const REST_COLLECTION_METADATA_COLLECTION = "__chimpbase.rest.collection_metadata";
const restCollectionMetadataValidator = v.object({
  collectionName: v.string(),
  documentId: v.string(),
  id: v.string(),
  schemaVersion: v.integer(),
});


class RestCollectionsRequestError extends Error {
  readonly status: number;

  constructor(status: number, message: string) {
    super(message);
    this.name = "RestCollectionsRequestError";
    this.status = status;
  }
}

export function restCollections(
  options: RestCollectionsOptions,
): ChimpbasePluginRegistration {
  const definitions = resolveCollections(options);
  const entries = definitions.flatMap((definition) => buildCollectionEntries(definition));
  if (options.dependsOn !== undefined || (options.name ?? "").length > 0) {
    return plugin(
      {
        dependsOn: options.dependsOn,
        name: options.name,
      },
      ...entries,
    );
  }

  return plugin(...entries);
}

function buildCollectionEntries(
  definition: ResolvedRestCollectionDefinition,
): readonly ChimpbaseRegistrationSource[] {
  return [
    action(definition.actionNames.list, async (ctx, input: RestListInput) => {
      const documents = await ctx.collection.find(definition.collectionName, input.filter, { limit: input.limit });
      if (definition.onRead === undefined || documents.length === 0) {
        return documents;
      }
      // shortcut: reads this collection's metadata; add an IN filter if versioned collections outgrow a single batch.
      const metadata = await ctx.collection.find(
        REST_COLLECTION_METADATA_COLLECTION,
        { collectionName: definition.collectionName },
      );
      const documentIds = new Set(documents.map((document) => document.id));
      const schemaVersions = new Map<string, number>();
      for (const record of metadata) {
        if (typeof record.documentId === "string" && documentIds.has(record.documentId) && !schemaVersions.has(record.documentId)) {
          const versioned = restCollectionMetadataValidator.parse(record, "rest collection metadata");
          schemaVersions.set(versioned.documentId, versioned.schemaVersion);
        }
      }
      return await Promise.all(
        documents.map(async (document) => await applyReadTransform(
          ctx.collection,
          definition,
          document,
          "list",
          typeof document.id === "string"
            ? schemaVersions.get(document.id) ?? readLegacyDocumentSchemaVersion(document)
            : null,
        )),
      );
    }),
    action(definition.actionNames.get, async (ctx, id: string) => {
      const document = await ctx.collection.findOne(definition.collectionName, { id });
      return await applyReadTransform(
        ctx.collection,
        definition,
        document,
        "get",
      );
    }),
    action(definition.actionNames.create, async (ctx, document: Record<string, unknown>) => {
      const nextDocument = await applyWriteTransform(definition, document, "create", null, null);
      const id = await ctx.collection.insert(definition.collectionName, nextDocument);
      await persistSchemaVersion(ctx.collection, definition, id);
      const stored = await ctx.collection.findOne(definition.collectionName, { id });
      return await applyReadTransform(
        ctx.collection,
        definition,
        stored,
        "create",
      );
    }),
    action(definition.actionNames.update, async (ctx, input: RestUpdateInput) => {
      const current = await ctx.collection.findOne(definition.collectionName, { id: input.id });
      if (!(current !== null)) {
        return null;
      }

      const patch = await applyWriteTransform(
        definition,
        input.patch,
        "update",
        current,
        definition.onWrite === undefined ? null : await resolveSchemaVersion(ctx.collection, definition, current),
      );
      await ctx.collection.update(definition.collectionName, { id: input.id }, patch);
      await persistSchemaVersion(ctx.collection, definition, input.id);
      const stored = await ctx.collection.findOne(definition.collectionName, { id: input.id });
      return await applyReadTransform(
        ctx.collection,
        definition,
        stored,
        "update",
      );
    }),
    action(definition.actionNames.delete, async (ctx, id: string) => {
      const deleted = await ctx.collection.delete(definition.collectionName, { id });
      if (deleted > 0) {
        await deleteSchemaVersionMetadata(ctx.collection, definition, id);
      }
      return deleted;
    }),
    route(`__chimpbase.rest.route.${definition.id}`, async (request, env) => {
      try {
        const url = new URL(request.url);
        const requestSegments = splitPath(url.pathname);

        if (matchesExactSegments(requestSegments, definition.routeSegments)) {
          return await handleCollectionRequest(definition, request, url, env);
        }

        if (matchesItemSegments(requestSegments, definition.routeSegments)) {
          return await handleDocumentRequest(
            definition,
            request,
            decodeURIComponent(requestSegments[requestSegments.length - 1] ?? ""),
            env,
          );
        }

        return null;
      } catch (error) {
        if (error instanceof RestCollectionsRequestError) {
          return jsonError(error.status, error.message);
        }

        throw error;
      }
    }),
  ];
}

async function handleCollectionRequest(
  definition: ResolvedRestCollectionDefinition,
  request: Request,
  url: URL,
  env: { action(name: string, ...args: unknown[]): Promise<unknown> },
): Promise<Response> {
  switch (request.method) {
    case "GET":
      if (!definition.methods.has("list")) {
        return methodNotAllowed(getAllowedCollectionMethods(definition));
      }
      return Response.json(
        await env.action(definition.actionNames.list, parseListInput(definition, url.searchParams)),
      );
    case "POST":
      if (!definition.methods.has("create")) {
        return methodNotAllowed(getAllowedCollectionMethods(definition));
      }
      return Response.json(
        await env.action(definition.actionNames.create, sanitizeWritableDocument(
          await parseJsonObject(request),
          definition.writableFields,
          definition.schemaVersion === null ? [] : ["schemaVersion"],
        )),
        { status: 201 },
      );
    default:
      return methodNotAllowed(getAllowedCollectionMethods(definition));
  }
}

async function handleDocumentRequest(
  definition: ResolvedRestCollectionDefinition,
  request: Request,
  id: string,
  env: { action(name: string, ...args: unknown[]): Promise<unknown> },
): Promise<Response> {
  switch (request.method) {
    case "GET": {
      if (!definition.methods.has("get")) {
        return methodNotAllowed(getAllowedDocumentMethods(definition));
      }
      const document = await env.action(definition.actionNames.get, id);
      return document === null
        ? jsonError(404, `document not found: ${id}`)
        : Response.json(document);
    }
    case "PATCH": {
      if (!definition.methods.has("update")) {
        return methodNotAllowed(getAllowedDocumentMethods(definition));
      }
      const document = await env.action(definition.actionNames.update, {
        id,
        patch: sanitizeWritableDocument(
          await parseJsonObject(request),
          definition.writableFields,
          definition.schemaVersion === null ? [] : ["schemaVersion"],
        ),
      } satisfies RestUpdateInput);
      return document === null
        ? jsonError(404, `document not found: ${id}`)
        : Response.json(document);
    }
    case "DELETE": {
      if (!definition.methods.has("delete")) {
        return methodNotAllowed(getAllowedDocumentMethods(definition));
      }
      const deletedCount = await env.action(definition.actionNames.delete, id);
      return deletedCount === 0
        ? jsonError(404, `document not found: ${id}`)
        : new Response(null, { status: 204 });
    }
    default:
      return methodNotAllowed(getAllowedDocumentMethods(definition));
  }
}

function resolveCollections(options: RestCollectionsOptions): ResolvedRestCollectionDefinition[] {
  const resolved: ResolvedRestCollectionDefinition[] = [];
  const seenPaths = new Set<string>();

  for (const [key, definition] of Object.entries(options.collections)) {
    const collectionName = definition.collection ?? key;
    const routePath = joinRoutePath(options.basePath, definition.path ?? key);
    if (seenPaths.has(routePath)) {
      throw new Error(`duplicate rest collection route path: ${routePath}`);
    }

    seenPaths.add(routePath);

    const methods = new Set(definition.methods ?? REST_METHODS);
    if (methods.size === 0) {
      throw new Error(`rest collection ${key} must enable at least one method`);
    }

    const maxLimit = normalizePositiveInteger(definition.maxLimit ?? options.maxLimit ?? DEFAULT_MAX_LIMIT, `${key}.maxLimit`);
    const defaultLimit = normalizePositiveInteger(
      definition.defaultLimit ?? options.defaultLimit ?? DEFAULT_LIMIT,
      `${key}.defaultLimit`,
    );
    if (defaultLimit > maxLimit) {
      throw new Error(`rest collection ${key} defaultLimit cannot exceed maxLimit`);
    }

    const id = hashString(`${routePath}:${collectionName}`);
    const actionBase = `__chimpbase.rest.${id}`;

    resolved.push({
      actionNames: {
        create: `${actionBase}.create`,
        delete: `${actionBase}.delete`,
        get: `${actionBase}.get`,
        list: `${actionBase}.list`,
        update: `${actionBase}.update`,
      },
      collectionName,
      defaultLimit,
      filterParsers: normalizeFilterParsers(definition.filterableFields),
      id,
      maxLimit,
      methods,
      onRead: definition.onRead,
      onWrite: definition.onWrite,
      routePath,
      routeSegments: splitPath(routePath),
      schemaVersion: definition.schemaVersion === undefined
        ? null
        : normalizePositiveInteger(definition.schemaVersion, `${key}.schemaVersion`),
      writableFields: normalizeWritableFields(definition.writableFields),
    });
  }

  return resolved;
}

function parseListInput(
  definition: ResolvedRestCollectionDefinition,
  searchParams: URLSearchParams,
): RestListInput {
  const filter: ChimpbaseCollectionFilter = {};
  let limit = definition.defaultLimit;

  for (const [key, value] of searchParams.entries()) {
    if (key === "limit") {
      limit = Math.min(parsePositiveIntegerValue(value, "limit"), definition.maxLimit);
      continue;
    }

    const parser = definition.filterParsers.get(key);
    if (!(parser !== undefined)) {
      throw badRequest(`unsupported filter field: ${key}`);
    }

    filter[key] = parser(value);
  }

  return { filter, limit };
}

function normalizeFilterParsers(
  fields: RestCollectionFilterFields | undefined,
): ReadonlyMap<string, (value: string) => unknown> {
  if (!(fields !== undefined)) {
    return new Map();
  }

  if (isJsonObject(fields)) {
    return new Map(
      Object.entries(fields).map(([field, parser]) => [field, createFilterParser(parser)]),
    );
  }

  return new Map(fields.map((field) => [field, parseStringValue]));
}

function normalizeWritableFields(
  fields: readonly string[] | undefined,
): ReadonlySet<string> | null {
  return (fields !== undefined) ? new Set(fields) : null;
}

function sanitizeWritableDocument(
  value: Record<string, unknown>,
  writableFields: ReadonlySet<string> | null,
  reservedFields: readonly string[] = [],
): Record<string, unknown> {
  if ("id" in value) {
    throw badRequest('field "id" is managed by the runtime');
  }

  for (const field of reservedFields) {
    if (field in value) {
      throw badRequest(`field "${field}" is managed by the plugin`);
    }
  }

  if (!(writableFields !== null)) {
    return value;
  }

  for (const key of Object.keys(value)) {
    if (!writableFields.has(key)) {
      throw badRequest(`field "${key}" is not writable`);
    }
  }

  return value;
}

async function applyReadTransform(
  collection: Pick<ChimpbaseCollectionClient, "findOne">,
  definition: ResolvedRestCollectionDefinition,
  document: Record<string, unknown> | null,
  operation: RestCollectionReadOperation,
  schemaVersion?: number | null,
): Promise<unknown> {
  if (!(document !== null)) {
    return null;
  }

  if (!(definition.onRead !== undefined)) {
    return document;
  }

  return await definition.onRead({
    configuredSchemaVersion: definition.schemaVersion,
    document,
    operation,
    schemaVersion: schemaVersion === undefined ? await resolveSchemaVersion(collection, definition, document) : schemaVersion,
  });
}

async function applyWriteTransform(
  definition: ResolvedRestCollectionDefinition,
  input: Record<string, unknown>,
  operation: RestCollectionWriteOperation,
  current: Record<string, unknown> | null,
  schemaVersion: number | null,
): Promise<Record<string, unknown>> {
  const transformed = (definition.onWrite !== undefined)
    ? await definition.onWrite({
        configuredSchemaVersion: definition.schemaVersion,
        current,
        input,
        operation,
        schemaVersion,
      })
    : input;

  if (!(transformed !== null && transformed !== undefined) || typeof transformed !== "object" || Array.isArray(transformed)) {
    throw new Error(`rest collection onWrite must return a plain object for ${definition.routePath}`);
  }

  if ("id" in transformed) {
    throw new Error(`rest collection onWrite cannot set "id" for ${definition.routePath}`);
  }

  if (definition.schemaVersion !== null && "schemaVersion" in transformed) {
    throw new Error(`rest collection onWrite cannot set "schemaVersion" in document content for ${definition.routePath}`);
  }

  return transformed;
}

async function resolveSchemaVersion(
  collection: Pick<ChimpbaseCollectionClient, "findOne">,
  definition: ResolvedRestCollectionDefinition,
  document: Record<string, unknown> | null,
): Promise<number | null> {
  if (!(document !== null) || typeof document.id !== "string") {
    return null;
  }

  const metadata = await collection.findOne(
    REST_COLLECTION_METADATA_COLLECTION,
    {
      collectionName: definition.collectionName,
      documentId: document.id,
    },
    restCollectionMetadataValidator,
  );

  if (metadata?.schemaVersion !== undefined) {
    return metadata.schemaVersion;
  }

  return readLegacyDocumentSchemaVersion(document);
}

async function persistSchemaVersion(
  collection: Pick<ChimpbaseCollectionClient, "insert" | "update">,
  definition: ResolvedRestCollectionDefinition,
  documentId: string,
): Promise<void> {
  if (definition.schemaVersion === null) {
    return;
  }

  const updated = await collection.update(
    REST_COLLECTION_METADATA_COLLECTION,
    {
      collectionName: definition.collectionName,
      documentId,
    },
    {
      schemaVersion: definition.schemaVersion,
    },
  );

  if (updated > 0) {
    return;
  }

  await collection.insert(REST_COLLECTION_METADATA_COLLECTION, {
    collectionName: definition.collectionName,
    documentId,
    schemaVersion: definition.schemaVersion,
  });
}

async function deleteSchemaVersionMetadata(
  collection: Pick<ChimpbaseCollectionClient, "delete">,
  definition: ResolvedRestCollectionDefinition,
  documentId: string,
): Promise<void> {
  await collection.delete(
    REST_COLLECTION_METADATA_COLLECTION,
    {
      collectionName: definition.collectionName,
      documentId,
    },
  );
}

function getAllowedCollectionMethods(
  definition: ResolvedRestCollectionDefinition,
): readonly string[] {
  const methods: string[] = [];
  if (definition.methods.has("list")) {
    methods.push("GET");
  }
  if (definition.methods.has("create")) {
    methods.push("POST");
  }
  return methods;
}

function getAllowedDocumentMethods(
  definition: ResolvedRestCollectionDefinition,
): readonly string[] {
  const methods: string[] = [];
  if (definition.methods.has("get")) {
    methods.push("GET");
  }
  if (definition.methods.has("update")) {
    methods.push("PATCH");
  }
  if (definition.methods.has("delete")) {
    methods.push("DELETE");
  }
  return methods;
}

async function parseJsonObject(request: Request): Promise<Record<string, unknown>> {
  let body: unknown;
  try {
    body = await readJsonBody(request);
  } catch {
    throw badRequest("request body must be valid JSON");
  }

  if (!isJsonObject(body)) {
    throw badRequest("request body must be a JSON object");
  }

  return body;
}

function methodNotAllowed(allowedMethods: readonly string[]): Response {
  return new Response(
    JSON.stringify({
      error: `method not allowed; allowed methods: ${allowedMethods.join(", ")}`,
    }),
    {
      headers: {
        allow: allowedMethods.join(", "),
        "content-type": "application/json; charset=utf-8",
      },
      status: 405,
    },
  );
}

function jsonError(status: number, error: string): Response {
  return Response.json({ error }, { status });
}

function joinRoutePath(basePath: string | undefined, path: string): string {
  const normalizedBasePath = normalizePath(basePath ?? "");
  const normalizedPath = normalizePath(path);

  if (normalizedBasePath === "/") {
    return normalizedPath;
  }

  if (normalizedPath === "/") {
    return normalizedBasePath;
  }

  return `${normalizedBasePath}${normalizedPath}`;
}

function normalizePath(path: string): string {
  const trimmed = path.trim();
  if (!(trimmed.length > 0) || trimmed === "/") {
    return "/";
  }

  const withLeadingSlash = trimmed.startsWith("/") ? trimmed : `/${trimmed}`;
  const normalized = withLeadingSlash.replace(/\/+$/g, "");
  return normalized.length > 0 ? normalized : "/";
}

function splitPath(path: string): string[] {
  return normalizePath(path)
    .split("/")
    .filter(Boolean);
}

function matchesExactSegments(
  left: readonly string[],
  right: readonly string[],
): boolean {
  if (left.length !== right.length) {
    return false;
  }

  return left.every((segment, index) => segment === right[index]);
}

function matchesItemSegments(
  left: readonly string[],
  prefix: readonly string[],
): boolean {
  if (left.length !== prefix.length + 1) {
    return false;
  }

  return prefix.every((segment, index) => segment === left[index]);
}

function createFilterParser(
  parser: RestCollectionFilterValueParser,
): (value: string) => unknown {
  if (typeof parser === "function") {
    return parser;
  }

  switch (parser) {
    case "boolean":
      return parseBooleanValue;
    case "number":
      return parseNumberValue;
    case "string":
      return parseStringValue;
  }
}

function parseBooleanValue(value: string): boolean {
  if (value === "true") {
    return true;
  }

  if (value === "false") {
    return false;
  }

  throw badRequest(`invalid boolean value: ${value}`);
}

function parseNumberValue(value: string): number {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    throw badRequest(`invalid number value: ${value}`);
  }

  return parsed;
}

function parseStringValue(value: string): string {
  return value;
}

function readLegacyDocumentSchemaVersion(document: Record<string, unknown> | null): number | null {
  const value = document?.schemaVersion;
  return typeof value === "number" && Number.isInteger(value) && value > 0
    ? value
    : null;
}

function normalizePositiveInteger(value: number, label: string): number {
  if (!Number.isInteger(value) || value <= 0) {
    throw new Error(`${label} must be a positive integer`);
  }

  return value;
}

function hashString(value: string): string {
  let hash = 2166136261;
  for (let index = 0; index < value.length; index += 1) {
    hash ^= value.charCodeAt(index);
    hash = Math.imul(hash, 16777619);
  }

  return (hash >>> 0).toString(36);
}

function badRequest(message: string): RestCollectionsRequestError {
  return new RestCollectionsRequestError(400, message);
}

function parsePositiveIntegerValue(value: string, label: string): number {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw badRequest(`${label} must be a positive integer`);
  }

  return parsed;
}
