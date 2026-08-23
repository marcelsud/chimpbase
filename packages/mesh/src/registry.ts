import { isArrayValue, isJsonObject, isStringArray, tryParseJson } from "@chimpbase/runtime";
import type { ChimpbaseContext } from "@chimpbase/runtime";

import type { NodeRecord, NodeServiceEntry } from "./types.ts";

const TABLE_NAME = "_chimpbase_mesh_nodes";
const INDEX_NAME = "idx_chimpbase_mesh_nodes_heartbeat";

export async function ensureRegistrySchema(ctx: ChimpbaseContext): Promise<void> {
  await ctx.db.query(
    `CREATE TABLE IF NOT EXISTS ${TABLE_NAME} (
      node_id            TEXT PRIMARY KEY,
      advertised_url     TEXT,
      metadata_json      TEXT NOT NULL DEFAULT '{}',
      services_json      TEXT NOT NULL DEFAULT '[]',
      started_at_ms      BIGINT NOT NULL,
      last_heartbeat_ms  BIGINT NOT NULL
    )`,
  );
  await ctx.db.query(
    `CREATE INDEX IF NOT EXISTS ${INDEX_NAME} ON ${TABLE_NAME} (last_heartbeat_ms)`,
  );
}

export interface UpsertNodeInput {
  advertisedUrl: string | null;
  metadata: Record<string, unknown>;
  nodeId: string;
  services: readonly NodeServiceEntry[];
  startedAtMs: number;
}

export async function upsertNode(ctx: ChimpbaseContext, input: UpsertNodeInput): Promise<number> {
  const nowMs = Date.now();
  await ctx.db.query(
    `INSERT INTO ${TABLE_NAME} (
        node_id, advertised_url, metadata_json, services_json, started_at_ms, last_heartbeat_ms
      ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
      ON CONFLICT (node_id) DO UPDATE SET
        advertised_url = excluded.advertised_url,
        metadata_json = excluded.metadata_json,
        services_json = excluded.services_json,
        started_at_ms = excluded.started_at_ms,
        last_heartbeat_ms = excluded.last_heartbeat_ms`,
    [
      input.nodeId,
      input.advertisedUrl,
      JSON.stringify(input.metadata),
      JSON.stringify(input.services),
      input.startedAtMs,
      nowMs,
    ],
  );

  return nowMs;
}

export async function touchHeartbeat(
  ctx: ChimpbaseContext,
  nodeId: string,
  metadata: Record<string, unknown>,
): Promise<number> {
  const nowMs = Date.now();
  await ctx.db.query(
    `UPDATE ${TABLE_NAME}
       SET last_heartbeat_ms = ?2,
           metadata_json = ?3
     WHERE node_id = ?1`,
    [nodeId, nowMs, JSON.stringify(metadata)],
  );

  return nowMs;
}

export async function deleteNode(ctx: ChimpbaseContext, nodeId: string): Promise<void> {
  await ctx.db.query(`DELETE FROM ${TABLE_NAME} WHERE node_id = ?1`, [nodeId]);
}

export async function listLiveNodes(
  ctx: ChimpbaseContext,
  minHeartbeatMs: number,
): Promise<NodeRecord[]> {
  const rows = await ctx.db.query<RegistryRow>(
    `SELECT node_id, advertised_url, metadata_json, services_json, started_at_ms, last_heartbeat_ms
       FROM ${TABLE_NAME}
      WHERE last_heartbeat_ms >= ?1`,
    [minHeartbeatMs],
  );

  return rows.map((row) => rowToNodeRecord(row));
}

export async function getNode(ctx: ChimpbaseContext, nodeId: string): Promise<NodeRecord | null> {
  const rows = await ctx.db.query<RegistryRow>(
    `SELECT node_id, advertised_url, metadata_json, services_json, started_at_ms, last_heartbeat_ms
       FROM ${TABLE_NAME} WHERE node_id = ?1`,
    [nodeId],
  );

  if (rows.length === 0) {
    return null;
  }

  return rowToNodeRecord(rows[0]);
}

export async function gcStaleNodes(
  ctx: ChimpbaseContext,
  olderThanMs: number,
): Promise<number> {
  const before = await ctx.db.query<{ node_id: string }>(
    `SELECT node_id FROM ${TABLE_NAME} WHERE last_heartbeat_ms < ?1`,
    [olderThanMs],
  );

  if (before.length === 0) {
    return 0;
  }

  await ctx.db.query(
    `DELETE FROM ${TABLE_NAME} WHERE last_heartbeat_ms < ?1`,
    [olderThanMs],
  );

  return before.length;
}

interface RegistryRow {
  node_id: string;
  advertised_url: string | null;
  metadata_json: unknown;
  services_json: unknown;
  started_at_ms: string | number;
  last_heartbeat_ms: string | number;
}

function rowToNodeRecord(row: RegistryRow): NodeRecord {
  return {
    advertisedUrl: row.advertised_url ?? null,
    lastHeartbeatMs: Number(row.last_heartbeat_ms),
    metadata: parseJsonObject(row.metadata_json),
    nodeId: row.node_id,
    services: parseServices(row.services_json),
    startedAtMs: Number(row.started_at_ms),
  };
}

function parseJsonObject(value: unknown): Record<string, unknown> {
  if (isJsonObject(value)) {
    return value;
  }

  if (typeof value === "string") {
    const parsed = tryParseJson(value);
    if (isJsonObject(parsed)) {
      return parsed;
    }
  }

  return {};
}

function parseServices(value: unknown): NodeServiceEntry[] {
  const list = isArrayValue(value)
    ? value
    : typeof value === "string"
      ? safeParseArray(value)
      : [];

  return list
    .filter(isJsonObject)
    .map((entry) => ({
      actions: isStringArray(entry.actions) ? entry.actions : [],
      events: isStringArray(entry.events) ? entry.events : [],
      name: String(entry.name ?? ""),
      version: Number(entry.version ?? 1),
    }))
    .filter((entry) => entry.name.length > 0);
}

function safeParseArray(value: string): unknown[] {
  const parsed = tryParseJson(value);
  return isArrayValue(parsed) ? parsed : [];
}
