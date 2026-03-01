// src/store/db.ts — SQLite connection manager for ISSUE-166 RoutingStore
// Feature flag: BRIDGE_ENABLE_MVP166=1
import { Database } from 'bun:sqlite';
import { mkdirSync } from 'node:fs';
import { join } from 'node:path';
import { bridgeLogger } from '../logger';

let _db: Database | null = null;
let _closed = false;

const DB_DIR = 'bridge_data';
const DB_FILE = 'bridge.db';

export function isFeatureEnabled(): boolean {
  return process.env.BRIDGE_ENABLE_MVP166 === '1';
}

export function getDb(): Database {
  if (_db) return _db;
  if (_closed) {
    throw new Error('[RoutingStore] DB already closed, cannot reopen');
  }

  const dir = join(process.cwd(), DB_DIR);
  mkdirSync(dir, { recursive: true });

  const dbPath = join(dir, DB_FILE);
  _db = new Database(dbPath);

  // WAL mode for better concurrent read performance
  _db.run('PRAGMA journal_mode = WAL');
  _db.run('PRAGMA busy_timeout = 5000');
  _db.run('PRAGMA synchronous = NORMAL');

  bridgeLogger.info(`[RoutingStore] sqlite opened path=${dbPath} mode=WAL`);
  return _db;
}

export function closeDb(): void {
  if (!_db) return;
  try {
    _db.run('PRAGMA wal_checkpoint(TRUNCATE)');
    _db.close();
    bridgeLogger.info('[RoutingStore] sqlite closed');
  } catch (e) {
    bridgeLogger.warn('[RoutingStore] sqlite close error', e);
  }
  _db = null;
  _closed = true;
}
