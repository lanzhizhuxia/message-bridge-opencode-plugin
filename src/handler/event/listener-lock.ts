// Cross-process leader election for the SSE event listener.
//
// opencode serve spawns child processes per session, each loading the plugin
// independently. Without coordination, N processes = N SSE listeners = Nx
// duplicate message delivery.
//
// Uses O_EXCL atomic file creation for lock. Stale locks detected via PID
// liveness + mtime heartbeat (covers SIGKILL / crashes).

import * as fs from 'node:fs';
import * as path from 'node:path';
import { bridgeLogger } from '../../logger.js';

const LOG_TAG = '[ListenerLock]';

const LOCK_FILENAME = '.bridge-listener.lock';
const HEARTBEAT_INTERVAL_MS = 30_000;
const STALE_THRESHOLD_MS = 90_000;

let heartbeatTimer: ReturnType<typeof setInterval> | null = null;
let lockFilePath: string | null = null;
let cleanupRegistered = false;

function getLockPath(): string {
  return path.join(process.cwd(), 'logs', LOCK_FILENAME);
}

function isPidAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

function isLockStaleByMtime(filePath: string): boolean {
  try {
    const stat = fs.statSync(filePath);
    return (Date.now() - stat.mtimeMs) > STALE_THRESHOLD_MS;
  } catch {
    return true;
  }
}

function readLockPid(filePath: string): number | null {
  try {
    const content = fs.readFileSync(filePath, 'utf-8').trim();
    const pid = parseInt(content, 10);
    return Number.isFinite(pid) && pid > 0 ? pid : null;
  } catch {
    return null;
  }
}

/**
 * Try to become the singleton event-listener process.
 * Returns true = this process owns the lock and should start the listener.
 * @param depth recursion guard — max 1 retry after stale recovery
 */
export function tryAcquireListenerLock(depth = 0): boolean {
  const target = getLockPath();

  try {
    fs.mkdirSync(path.dirname(target), { recursive: true });
  } catch { /* dir exists */ }

  try {
    // O_EXCL: kernel-level atomic check-and-create — race-free
    const fd = fs.openSync(
      target,
      fs.constants.O_CREAT | fs.constants.O_EXCL | fs.constants.O_WRONLY,
    );
    fs.writeSync(fd, `${process.pid}\n`);
    fs.closeSync(fd);

    lockFilePath = target;
    bridgeLogger.info(`${LOG_TAG} lock acquired pid=${process.pid} path=${target}`);
    registerCleanup();
    startHeartbeat();
    return true;
  } catch (e: unknown) {
    if (!(e instanceof Error && 'code' in e && (e as NodeJS.ErrnoException).code === 'EEXIST')) {
      bridgeLogger.error(`${LOG_TAG} unexpected error acquiring lock`, e);
      return false;
    }

    const ownerPid = readLockPid(target);
    if (ownerPid !== null && isPidAlive(ownerPid) && !isLockStaleByMtime(target)) {
      bridgeLogger.info(
        `${LOG_TAG} lock held by pid=${ownerPid} (alive, fresh) — skipping listener`,
      );
      return false;
    }

    if (depth >= 1) {
      bridgeLogger.warn(`${LOG_TAG} stale recovery already attempted, giving up`);
      return false;
    }

    bridgeLogger.info(
      `${LOG_TAG} stale lock detected owner=${ownerPid} alive=${ownerPid !== null && isPidAlive(ownerPid)} — recovering`,
    );

    // Atomic rename avoids TOCTOU race in delete-then-recreate
    try {
      const stalePath = `${target}.stale.${process.pid}`;
      fs.renameSync(target, stalePath);
      try { fs.unlinkSync(stalePath); } catch { /* best effort */ }
    } catch { /* another process may have already recovered */ }

    return tryAcquireListenerLock(depth + 1);
  }
}

export function releaseListenerLock(): void {
  stopHeartbeat();

  if (!lockFilePath) return;

  try {
    const ownerPid = readLockPid(lockFilePath);
    if (ownerPid === process.pid) {
      fs.unlinkSync(lockFilePath);
      bridgeLogger.info(`${LOG_TAG} lock released pid=${process.pid}`);
    }
  } catch { /* best effort during shutdown */ }
  lockFilePath = null;
}

function startHeartbeat(): void {
  stopHeartbeat();
  heartbeatTimer = setInterval(() => {
    if (!lockFilePath) return;
    try {
      const now = new Date();
      fs.utimesSync(lockFilePath, now, now);
    } catch {
      stopHeartbeat();
    }
  }, HEARTBEAT_INTERVAL_MS);

  if (heartbeatTimer && typeof heartbeatTimer === 'object' && 'unref' in heartbeatTimer) {
    heartbeatTimer.unref();
  }
}

function stopHeartbeat(): void {
  if (heartbeatTimer) {
    clearInterval(heartbeatTimer);
    heartbeatTimer = null;
  }
}

function registerCleanup(): void {
  if (cleanupRegistered) return;
  cleanupRegistered = true;

  process.on('exit', () => releaseListenerLock());
  process.on('SIGTERM', () => {
    releaseListenerLock();
    process.exit(0);
  });
  process.on('SIGINT', () => {
    releaseListenerLock();
    process.exit(0);
  });
}
