// src/store/migration.ts — Minimal schema migration runner for ISSUE-166
import { getDb } from './db';
import { bridgeLogger } from '../logger';

type Migration = { version: number; up: string[] };

const MIGRATIONS: Migration[] = [
  {
    version: 1,
    up: [
      `CREATE TABLE routing_state (
        session_id TEXT PRIMARY KEY,
        chat_id TEXT NOT NULL,
        adapter_key TEXT NOT NULL,
        sender_id TEXT NOT NULL DEFAULT 'system',
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL
      )`,
      `CREATE INDEX idx_routing_updated_at ON routing_state(updated_at)`,
    ],
  },
  {
    version: 2,
    up: [
      `CREATE TABLE quota_ledger (
        app_id TEXT NOT NULL,
        month TEXT NOT NULL,
        day TEXT NOT NULL,
        call_count INTEGER NOT NULL DEFAULT 0,
        last_updated INTEGER NOT NULL,
        PRIMARY KEY (app_id, month, day)
      )`,
      `CREATE INDEX idx_quota_month ON quota_ledger(app_id, month)`,
    ],
  },
];

export function runMigrations(): void {
  const db = getDb();

  // Ensure schema_version table exists
  db.run(
    `CREATE TABLE IF NOT EXISTS schema_version (
      version INTEGER PRIMARY KEY,
      applied_at TEXT NOT NULL
    )`
  );

  const row = db.query<{ v: number | null }, []>(
    'SELECT MAX(version) as v FROM schema_version'
  ).get();
  const currentVersion = row?.v ?? 0;

  for (const m of MIGRATIONS) {
    if (m.version <= currentVersion) continue;

    const tx = db.transaction(() => {
      for (const sql of m.up) {
        db.run(sql);
      }
      db.run(
        'INSERT INTO schema_version(version, applied_at) VALUES(?, datetime(\'now\'))',
        [m.version]
      );
    });
    tx();

    bridgeLogger.info(`[RoutingStore] migration v${m.version} applied`);
  }
}
