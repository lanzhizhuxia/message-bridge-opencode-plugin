// src/store/quota-ledger.ts — Feishu API quota tracking for ISSUE-166 Phase 2
// Tracks daily/monthly API call counts per Feishu app to enable adaptive degradation.
// All timestamps use UTC to avoid timezone drift at month boundaries.
import { getDb, isFeatureEnabled } from './db';
import { runMigrations } from './migration';
import { bridgeLogger } from '../logger';

export type DegradationLevel = 'normal' | 'constrained' | 'critical';

const DEFAULT_MONTHLY_QUOTA = 10_000;
const CONSTRAINED_THRESHOLD = 0.70; // 70%
const CRITICAL_THRESHOLD = 0.85;    // 85%

function getMonthlyBudget(): number {
  const env = process.env.BRIDGE_FEISHU_MONTHLY_QUOTA;
  if (env) {
    const n = parseInt(env, 10);
    if (Number.isFinite(n) && n > 0) return n;
  }
  return DEFAULT_MONTHLY_QUOTA;
}

function utcDay(): string {
  return new Date().toISOString().slice(0, 10); // YYYY-MM-DD
}

function utcMonth(): string {
  return new Date().toISOString().slice(0, 7); // YYYY-MM
}

export class QuotaLedger {
  private initialized = false;
  private appId: string;

  constructor(appId: string) {
    this.appId = appId;
  }

  private ensureInit(): void {
    if (this.initialized) return;
    runMigrations();
    this.initialized = true;
  }

  /**
   * Record one API call. Uses atomic UPSERT for concurrent safety.
   */
  recordCall(count = 1): void {
    try {
      this.ensureInit();
      const db = getDb();
      const month = utcMonth();
      const day = utcDay();
      const now = Date.now();
      db.run(
        `INSERT INTO quota_ledger (app_id, month, day, call_count, last_updated)
         VALUES (?, ?, ?, ?, ?)
         ON CONFLICT(app_id, month, day) DO UPDATE SET
           call_count = call_count + excluded.call_count,
           last_updated = excluded.last_updated`,
        [this.appId, month, day, count, now],
      );
    } catch (e) {
      bridgeLogger.warn(`[QuotaLedger] recordCall failed app=${this.appId}`, e);
    }
  }

  /**
   * Get total API calls for the current UTC month.
   */
  getMonthlyCount(): number {
    try {
      this.ensureInit();
      const db = getDb();
      const month = utcMonth();
      const row = db.query<{ total: number | null }, [string, string]>(
        'SELECT SUM(call_count) as total FROM quota_ledger WHERE app_id = ? AND month = ?',
      ).get(this.appId, month);
      return row?.total ?? 0;
    } catch (e) {
      bridgeLogger.warn(`[QuotaLedger] getMonthlyCount failed app=${this.appId}`, e);
      return 0;
    }
  }

  /**
   * Get API calls for the current UTC day.
   */
  getDailyCount(): number {
    try {
      this.ensureInit();
      const db = getDb();
      const month = utcMonth();
      const day = utcDay();
      const row = db.query<{ total: number | null }, [string, string, string]>(
        'SELECT call_count as total FROM quota_ledger WHERE app_id = ? AND month = ? AND day = ?',
      ).get(this.appId, month, day);
      return row?.total ?? 0;
    } catch (e) {
      bridgeLogger.warn(`[QuotaLedger] getDailyCount failed app=${this.appId}`, e);
      return 0;
    }
  }

  /**
   * Calculate degradation level based on current monthly usage.
   * - normal:      < 70% of budget
   * - constrained: 70% - 85%
   * - critical:    > 85%
   */
  getDegradationLevel(): DegradationLevel {
    const budget = getMonthlyBudget();
    const used = this.getMonthlyCount();
    const ratio = used / budget;

    if (ratio >= CRITICAL_THRESHOLD) return 'critical';
    if (ratio >= CONSTRAINED_THRESHOLD) return 'constrained';
    return 'normal';
  }

  /**
   * Get quota usage summary for metrics/logging.
   */
  getUsageSummary(): {
    appId: string;
    monthlyCount: number;
    dailyCount: number;
    budget: number;
    ratio: number;
    level: DegradationLevel;
  } {
    const budget = getMonthlyBudget();
    const monthlyCount = this.getMonthlyCount();
    const dailyCount = this.getDailyCount();
    return {
      appId: this.appId,
      monthlyCount,
      dailyCount,
      budget,
      ratio: monthlyCount / budget,
      level: this.getDegradationLevel(),
    };
  }

  /**
   * Sweep old monthly data (keep last 3 months).
   */
  sweepOldData(): void {
    try {
      this.ensureInit();
      const db = getDb();
      // Keep current month and 2 previous months
      const now = new Date();
      const cutoffDate = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth() - 2, 1));
      const cutoffMonth = cutoffDate.toISOString().slice(0, 7);
      const result = db.run(
        'DELETE FROM quota_ledger WHERE app_id = ? AND month < ?',
        [this.appId, cutoffMonth],
      );
      const deleted = result.changes;
      if (deleted > 0) {
        bridgeLogger.info(
          `[QuotaLedger] sweep deleted=${deleted} cutoff=${cutoffMonth} app=${this.appId}`,
        );
      }
    } catch (e) {
      bridgeLogger.warn(`[QuotaLedger] sweep failed app=${this.appId}`, e);
    }
  }
}

// ── Singleton management ──

let _ledger: QuotaLedger | null = null;

export function getQuotaLedger(): QuotaLedger | null {
  if (!isFeatureEnabled()) return null;
  return _ledger;
}

export function initQuotaLedger(appId: string): QuotaLedger {
  if (_ledger) return _ledger;
  _ledger = new QuotaLedger(appId);
  bridgeLogger.info(`[QuotaLedger] initialized app=${appId}`);
  return _ledger;
}

export function resetQuotaLedger(): void {
  _ledger = null;
}
