// src/store/routing-store.ts — Session routing persistence for ISSUE-166
import { getDb, isFeatureEnabled } from './db';
import { runMigrations } from './migration';
import { bridgeLogger } from '../logger';

const DEFAULT_TTL_SECONDS = 24 * 60 * 60; // 24 hours

type RoutingRow = {
  session_id: string;
  chat_id: string;
  adapter_key: string;
  sender_id: string;
  created_at: number;
  updated_at: number;
};

export class RoutingStore {
  private initialized = false;

  private ensureInit(): void {
    if (this.initialized) return;
    runMigrations();
    this.initialized = true;
  }

  private getTtlMs(): number {
    const envVal = process.env.BRIDGE_ROUTING_TTL_SECONDS;
    const seconds = envVal ? parseInt(envVal, 10) : DEFAULT_TTL_SECONDS;
    return (isNaN(seconds) || seconds <= 0 ? DEFAULT_TTL_SECONDS : seconds) * 1000;
  }

  upsert(sessionId: string, chatId: string, adapterKey: string, senderId: string): void {
    try {
      this.ensureInit();
      const db = getDb();
      const now = Date.now();
      db.run(
        `INSERT INTO routing_state (session_id, chat_id, adapter_key, sender_id, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?)
         ON CONFLICT(session_id) DO UPDATE SET
           chat_id = excluded.chat_id,
           adapter_key = excluded.adapter_key,
           sender_id = excluded.sender_id,
           updated_at = excluded.updated_at`,
        [sessionId, chatId, adapterKey, senderId, now, now]
      );
    } catch (e) {
      bridgeLogger.warn(`[RoutingStore] upsert failed sid=${sessionId}`, e);
    }
  }

  tryGet(sessionId: string): { chatId: string; adapterKey: string; senderId: string } | null {
    try {
      this.ensureInit();
      const db = getDb();
      const row = db.query<RoutingRow, [string]>(
        'SELECT * FROM routing_state WHERE session_id = ?'
      ).get(sessionId);

      if (!row) return null;

      // TTL check
      const ttlMs = this.getTtlMs();
      if (Date.now() - row.updated_at > ttlMs) {
        bridgeLogger.debug(`[RoutingStore] ttl-expired sid=${sessionId} age=${Date.now() - row.updated_at}ms`);
        return null;
      }

      return {
        chatId: row.chat_id,
        adapterKey: row.adapter_key,
        senderId: row.sender_id,
      };
    } catch (e) {
      bridgeLogger.warn(`[RoutingStore] tryGet failed sid=${sessionId}`, e);
      return null;
    }
  }

  sweepExpired(): void {
    try {
      this.ensureInit();
      const db = getDb();
      const ttlMs = this.getTtlMs();
      const cutoff = Date.now() - ttlMs;
      const result = db.run(
        'DELETE FROM routing_state WHERE updated_at < ?',
        [cutoff]
      );
      const deleted = result.changes;
      if (deleted > 0) {
        bridgeLogger.info(`[RoutingStore] sweep deleted=${deleted} cutoff=${new Date(cutoff).toISOString()}`);
      }
    } catch (e) {
      bridgeLogger.warn('[RoutingStore] sweep failed', e);
    }
  }
}

export function isRoutingStoreEnabled(): boolean {
  return isFeatureEnabled();
}
