import { Pool } from 'pg';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------
interface CacheEntry {
  data: unknown;
  expiry: number;
}

// Cache TTL presets for different query types
export const CACHE_TTLS = {
  COUNTS: 10 * 60 * 1000,       // 10 min — aggregate counts change slowly
  DAILY_SERIES: 15 * 60 * 1000, // 15 min — 90-day charts don't change fast
  RECENT: 2 * 60 * 1000,        // 2 min  — recent transactions/events
  LEADERBOARD: 10 * 60 * 1000,  // 10 min — top N lists
  NETWORK: 5 * 60 * 1000,       // 5 min  — network stats
};

const DEFAULT_CACHE_TTL = 5 * 60 * 1000; // 5 minutes

// ---------------------------------------------------------------------------
// Tier 1 — In-memory cache (fast, lost on cold start)
// ---------------------------------------------------------------------------
const memoryCache = new Map<string, CacheEntry>();

// ---------------------------------------------------------------------------
// Connection pool
// ---------------------------------------------------------------------------
let pool: Pool | null = null;

function getPool(): Pool {
  if (!pool) {
    pool = new Pool({
      connectionString: process.env.NEON_DATABASE_URL,
      max: 10,
      idleTimeoutMillis: 30000,
      connectionTimeoutMillis: 30000,
      statement_timeout: 120000,
      ssl: { rejectUnauthorized: false },
    });

    pool.on('error', (err) => {
      console.error('Unexpected pool error:', err);
    });
  }
  return pool;
}

// ---------------------------------------------------------------------------
// Tier 2 — DB cache table (persists across cold starts)
// ---------------------------------------------------------------------------
let dbCacheReady = false;

async function ensureCacheTable(): Promise<void> {
  if (dbCacheReady) return;
  const p = getPool();
  const client = await p.connect();
  try {
    await client.query(`
      CREATE TABLE IF NOT EXISTS api_cache (
        cache_key  TEXT PRIMARY KEY,
        data       JSONB NOT NULL,
        updated_at TIMESTAMPTZ DEFAULT NOW()
      );
    `);
    dbCacheReady = true;
  } catch (err) {
    console.error('Failed to ensure api_cache table:', err);
  } finally {
    client.release();
  }
}

/** Read a row from the api_cache table. Returns null if not found. */
async function readDbCache<T>(key: string): Promise<{ data: T; updatedAt: Date } | null> {
  try {
    await ensureCacheTable();
    const p = getPool();
    const client = await p.connect();
    try {
      const result = await client.query(
        'SELECT data, updated_at FROM api_cache WHERE cache_key = $1',
        [key],
      );
      if (result.rows.length === 0) return null;
      return {
        data: result.rows[0].data as T,
        updatedAt: new Date(result.rows[0].updated_at),
      };
    } finally {
      client.release();
    }
  } catch (err) {
    console.error('readDbCache error:', err);
    return null;
  }
}

/** Upsert a value into the api_cache table. */
async function writeDbCache(key: string, data: unknown): Promise<void> {
  try {
    await ensureCacheTable();
    const p = getPool();
    const client = await p.connect();
    try {
      await client.query(
        `INSERT INTO api_cache (cache_key, data, updated_at)
         VALUES ($1, $2::jsonb, NOW())
         ON CONFLICT (cache_key)
         DO UPDATE SET data = $2::jsonb, updated_at = NOW()`,
        [key, JSON.stringify(data)],
      );
    } finally {
      client.release();
    }
  } catch (err) {
    console.error('writeDbCache error:', err);
  }
}

// ---------------------------------------------------------------------------
// Background refresh tracking  (consumed by API routes / DataFreshness)
// ---------------------------------------------------------------------------
const refreshingKeys = new Set<string>();

/** Returns true if at least one cache key is currently being refreshed. */
export function isRefreshing(): boolean {
  return refreshingKeys.size > 0;
}

/** Returns the set of keys currently refreshing (for debugging / status). */
export function getRefreshingKeys(): string[] {
  return Array.from(refreshingKeys);
}

// ---------------------------------------------------------------------------
// Core query functions — two-tier cache
// ---------------------------------------------------------------------------

/**
 * Execute a SQL query with a two-tier cache:
 *   1. Check in-memory cache → if fresh, return immediately.
 *   2. Check DB cache table  → if exists (even stale), return it AND
 *      trigger a background refresh.
 *   3. If nothing cached anywhere, run the query synchronously and store
 *      in both caches.
 *
 * The returned object has an extra `_fromCache` flag that API routes can
 * forward to the client so the UI can show "Data updating…".
 */
export async function query<T = Record<string, unknown>>(
  sql: string,
  params: unknown[] = [],
  cacheKey?: string,
  cacheTTL: number = DEFAULT_CACHE_TTL,
): Promise<T[] & { _stale?: boolean }> {
  // ----- Tier 1: in-memory cache (hot path) -----
  if (cacheKey) {
    const mem = memoryCache.get(cacheKey);
    if (mem && mem.expiry > Date.now()) {
      const result = mem.data as T[];
      return result;
    }
  }

  // ----- Tier 2: DB cache table -----
  if (cacheKey) {
    const dbEntry = await readDbCache<T[]>(cacheKey);
    if (dbEntry) {
      // Populate in-memory cache so subsequent calls are fast
      memoryCache.set(cacheKey, {
        data: dbEntry.data,
        expiry: Date.now() + cacheTTL,
      });

      // Check freshness — is the DB cache still within TTL?
      const age = Date.now() - dbEntry.updatedAt.getTime();
      if (age < cacheTTL) {
        // Still fresh, return as-is
        return dbEntry.data;
      }

      // Stale — return immediately but kick off background refresh
      if (!refreshingKeys.has(cacheKey)) {
        refreshingKeys.add(cacheKey);
        // Fire-and-forget background refresh
        executeAndCache<T>(sql, params, cacheKey, cacheTTL)
          .catch((err) => console.error(`Background refresh failed [${cacheKey}]:`, err))
          .finally(() => refreshingKeys.delete(cacheKey));
      }

      const staleResult = dbEntry.data as T[] & { _stale?: boolean };
      staleResult._stale = true;
      return staleResult;
    }
  }

  // ----- Nothing cached — synchronous fetch -----
  return executeAndCache<T>(sql, params, cacheKey, cacheTTL);
}

/**
 * Actually run the SQL, then write to both in-memory and DB caches.
 */
async function executeAndCache<T>(
  sql: string,
  params: unknown[],
  cacheKey: string | undefined,
  cacheTTL: number,
): Promise<T[]> {
  const p = getPool();
  const client = await p.connect();
  try {
    const result = await client.query(sql, params);
    const data = result.rows as T[];

    if (cacheKey) {
      // Tier 1
      memoryCache.set(cacheKey, { data, expiry: Date.now() + cacheTTL });
      // Tier 2 (fire-and-forget — don't block the response)
      writeDbCache(cacheKey, data).catch((err) =>
        console.error(`writeDbCache failed [${cacheKey}]:`, err),
      );
    }

    return data;
  } finally {
    client.release();
  }
}

export async function queryOne<T = Record<string, unknown>>(
  sql: string,
  params: unknown[] = [],
  cacheKey?: string,
  cacheTTL: number = DEFAULT_CACHE_TTL,
): Promise<T | null> {
  const rows = await query<T>(sql, params, cacheKey, cacheTTL);
  return rows[0] || null;
}
