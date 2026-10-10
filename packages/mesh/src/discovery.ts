import type { NodeRecord, NodeServiceEntry } from "./types.ts";

export const INFO_EVENT_ANNOUNCE = "__chimpbase.mesh.info.announce";
export const INFO_EVENT_LEAVE = "__chimpbase.mesh.info.leave";

export interface AnnouncePayload {
  advertisedUrl: string | null;
  metadata: Record<string, unknown>;
  nodeId: string;
  services: readonly NodeServiceEntry[];
  startedAtMs: number;
}

export interface LeavePayload {
  nodeId: string;
}

export class MeshPeerCache {
  private readonly peers = new Map<string, NodeRecord>();

  constructor(private readonly offlineAfterMs: number) {}

  seed(records: readonly NodeRecord[]): void {
    this.peers.clear();
    for (const record of records) {
      this.peers.set(record.nodeId, record);
    }
  }

  upsert(record: NodeRecord): void {
    this.peers.set(record.nodeId, record);
  }

  remove(nodeId: string): void {
    this.peers.delete(nodeId);
  }

  all(now: number = Date.now()): NodeRecord[] {
    const cutoff = now - this.offlineAfterMs;
    const fresh: NodeRecord[] = [];
    for (const record of this.peers.values()) {
      if (record.lastHeartbeatMs < cutoff) {
        this.peers.delete(record.nodeId);
        continue;
      }

      fresh.push(record);
    }
    return fresh;
  }

  get(nodeId: string): NodeRecord | null {
    return this.peers.get(nodeId) ?? null;
  }

  findByAction(actionName: string, now: number = Date.now()): NodeRecord[] {
    return this.all(now).filter((peer) =>
      peer.services.some((service) => service.actions.includes(actionName)),
    );
  }
}
