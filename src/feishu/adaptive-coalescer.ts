// src/feishu/adaptive-coalescer.ts — Adaptive flush coalescing for ISSUE-166 Phase 2
// Reduces Feishu API calls by dynamically adjusting flush intervals based on quota usage.
//
// Three degradation levels:
//   normal:      first flush at 1s, then every 4s or +400 chars
//   constrained: first flush at 2s, then every 8s or +800 chars
//   critical:    first flush only, then only on done/error (skip all intermediate edits)
//
// Max staleness guard: forced flush every 15s in normal, 25s in constrained (prevents
// user seeing stale content on slow streams).

import type { DegradationLevel } from '../store/quota-ledger';
import { bridgeLogger } from '../logger';

// ── Configuration per degradation level ──

type LevelConfig = {
  firstFlushDelayMs: number;   // delay before first flush
  intervalMs: number;          // minimum interval between flushes
  charDelta: number;           // chars changed since last flush to trigger
  maxStalenessMs: number;      // forced flush if this much time passes regardless
  skipIntermediate: boolean;   // if true, only first + final flushes
};

const LEVEL_CONFIGS: Record<DegradationLevel, LevelConfig> = {
  normal: {
    firstFlushDelayMs: 1_000,
    intervalMs: 4_000,
    charDelta: 400,
    maxStalenessMs: 15_000,
    skipIntermediate: false,
  },
  constrained: {
    firstFlushDelayMs: 2_000,
    intervalMs: 8_000,
    charDelta: 800,
    maxStalenessMs: 25_000,
    skipIntermediate: false,
  },
  critical: {
    firstFlushDelayMs: 1_500,
    intervalMs: 0,             // unused when skipIntermediate=true
    charDelta: 0,              // unused
    maxStalenessMs: 0,         // unused
    skipIntermediate: true,    // only first + final flush
  },
};

// ── Per-message coalescer state ──

export class MessageCoalescer {
  private messageId: string;
  private createdAt: number;
  private firstFlushDone = false;
  private lastFlushAt = 0;
  private lastFlushedCharCount = 0;
  private flushCount = 0;

  constructor(messageId: string) {
    this.messageId = messageId;
    this.createdAt = Date.now();
  }

  /**
   * Decide whether the buffer should flush right now.
   *
   * @param level        Current degradation level from QuotaLedger
   * @param currentChars Total chars in buffer (text + reasoning)
   * @param isFinal      True if status is done/error/aborted
   * @returns            true = flush now, false = skip
   */
  shouldFlush(
    level: DegradationLevel,
    currentChars: number,
    isFinal: boolean,
  ): boolean {
    const now = Date.now();
    const config = LEVEL_CONFIGS[level];

    // Always flush on final (done / error / aborted)
    if (isFinal) {
      return true;
    }

    // First flush: wait for firstFlushDelay before sending anything
    if (!this.firstFlushDone) {
      if (now - this.createdAt < config.firstFlushDelayMs) {
        return false;
      }
      return true; // time to do first flush
    }

    // Critical mode: skip all intermediate flushes
    if (config.skipIntermediate) {
      return false;
    }

    // Time-based interval check
    const timeSinceLastFlush = now - this.lastFlushAt;
    if (timeSinceLastFlush >= config.intervalMs) {
      return true;
    }

    // Char-delta threshold: enough new content since last flush
    const charsSinceFlush = currentChars - this.lastFlushedCharCount;
    if (charsSinceFlush >= config.charDelta) {
      return true;
    }

    // Max staleness guard: force flush if we haven't flushed in too long
    if (config.maxStalenessMs > 0 && timeSinceLastFlush >= config.maxStalenessMs) {
      bridgeLogger.debug(
        `[Coalescer] max-staleness-flush mid=${this.messageId} stale=${timeSinceLastFlush}ms`,
      );
      return true;
    }

    return false;
  }

  /**
   * Record that a flush actually happened. Call AFTER successful send/edit.
   */
  recordFlush(currentChars: number): void {
    const now = Date.now();
    if (!this.firstFlushDone) {
      this.firstFlushDone = true;
      bridgeLogger.debug(
        `[Coalescer] first-flush mid=${this.messageId} delay=${now - this.createdAt}ms chars=${currentChars}`,
      );
    }
    this.lastFlushAt = now;
    this.lastFlushedCharCount = currentChars;
    this.flushCount++;
  }

  getFlushCount(): number {
    return this.flushCount;
  }

  getMessageId(): string {
    return this.messageId;
  }
}

// ── Coalescer registry (managed per dispatch lifecycle) ──

import { LRUCache } from 'lru-cache';

const coalescerCache = new LRUCache<string, MessageCoalescer>({
  max: 600,
  ttl: 30 * 60 * 1000, // 30 min TTL
});

export function getOrCreateCoalescer(messageId: string): MessageCoalescer {
  let c = coalescerCache.get(messageId);
  if (!c) {
    c = new MessageCoalescer(messageId);
    coalescerCache.set(messageId, c);
  }
  return c;
}

export function removeCoalescer(messageId: string): void {
  coalescerCache.delete(messageId);
}

export function resetAllCoalescers(): void {
  coalescerCache.clear();
}

/**
 * Get coalescer metrics for logging.
 */
export function getCoalescerMetrics(): { activeCount: number } {
  return { activeCount: coalescerCache.size };
}
