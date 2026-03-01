// src/feishu/rate-limit.ts — Feishu API rate-limit detection & retry for ISSUE-166
// Handles: 99991668 (rate limit) and 99991403 (monthly quota exceeded)
import { bridgeLogger } from '../logger';

// ── Error code detection ──

function extractErrorCode(error: unknown): number | undefined {
  if (!error || typeof error !== 'object') return undefined;
  const rec = error as Record<string, unknown>;

  // Direct code on error object
  if (typeof rec.code === 'number') return rec.code;

  // Nested in response.data.code (lark SDK shape)
  const response = rec.response as Record<string, unknown> | undefined;
  const data =
    (response?.data as Record<string, unknown>) ??
    (rec.data as Record<string, unknown>);
  if (data && typeof data.code === 'number') return data.code;

  // String matching fallback
  const msg = String(rec.message ?? rec.msg ?? '');
  const match = msg.match(/(?:errcode|code)[:\s]*(\d{5,})/i);
  return match ? Number(match[1]) : undefined;
}

export function isRateLimitError(error: unknown): boolean {
  const code = extractErrorCode(error);
  if (code === 99991668) return true;

  // Also check string patterns
  const msg = error instanceof Error ? error.message : String(error ?? '');
  return /rate.?limit|too many requests|99991668/i.test(msg);
}

export function isQuotaExhaustedError(error: unknown): boolean {
  const code = extractErrorCode(error);
  if (code === 99991403) return true;

  const msg = error instanceof Error ? error.message : String(error ?? '');
  return /quota|99991403|api call limit/i.test(msg);
}

export function isRetryableFeishuError(error: unknown): boolean {
  return isRateLimitError(error);
  // Quota exhaustion (99991403) is NOT retryable — it persists until month resets
}

// ── Retry wrapper ──

export type RateLimitRetryOptions = {
  maxRetries?: number;       // default 3
  baseDelayMs?: number;      // default 1000
  maxDelayMs?: number;       // default 8000
  label?: string;            // for logging
};

const DEFAULT_MAX_RETRIES = 3;
const DEFAULT_BASE_DELAY_MS = 1000;
const DEFAULT_MAX_DELAY_MS = 8000;

export async function withRateLimitRetry<T>(
  fn: () => Promise<T>,
  opts?: RateLimitRetryOptions,
): Promise<T> {
  const maxRetries = opts?.maxRetries ?? DEFAULT_MAX_RETRIES;
  const baseDelay = opts?.baseDelayMs ?? DEFAULT_BASE_DELAY_MS;
  const maxDelay = opts?.maxDelayMs ?? DEFAULT_MAX_DELAY_MS;
  const label = opts?.label ?? 'feishu-api';

  let lastError: unknown;

  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      return await fn();
    } catch (e) {
      lastError = e;

      // Quota exhaustion: propagate immediately, retrying won't help
      if (isQuotaExhaustedError(e)) {
        bridgeLogger.error(
          `[RateLimit] ${label} quota-exhausted (99991403), NOT retrying`,
        );
        throw e;
      }

      // Rate limit: retry with exponential backoff
      if (isRetryableFeishuError(e) && attempt < maxRetries) {
        const jitter = Math.random() * 500;
        const delay = Math.min(baseDelay * 2 ** attempt + jitter, maxDelay);
        bridgeLogger.warn(
          `[RateLimit] ${label} rate-limited attempt=${attempt + 1}/${maxRetries} delay=${Math.round(delay)}ms`,
        );
        await new Promise(resolve => setTimeout(resolve, delay));
        continue;
      }

      // Not a rate-limit error, or exhausted retries — propagate
      if (attempt > 0) {
        bridgeLogger.warn(
          `[RateLimit] ${label} exhausted retries=${attempt} for rate-limit error`,
        );
      }
      throw e;
    }
  }

  // Should never reach here, but TypeScript needs this
  throw lastError;
}

// ── Metrics ──

let rateLimitHitCount = 0;
let quotaExhaustedHitCount = 0;

export function recordRateLimitHit(): void {
  rateLimitHitCount++;
}

export function recordQuotaExhaustedHit(): void {
  quotaExhaustedHitCount++;
}

export function getRateLimitMetrics(): { rateLimitHits: number; quotaExhaustedHits: number } {
  return { rateLimitHits: rateLimitHitCount, quotaExhaustedHits: quotaExhaustedHitCount };
}

export function resetRateLimitMetrics(): void {
  rateLimitHitCount = 0;
  quotaExhaustedHitCount = 0;
}
