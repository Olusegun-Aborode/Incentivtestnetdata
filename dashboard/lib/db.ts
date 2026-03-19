import { Pool } from 'pg';

// Simple in-memory cache
interface CacheEntry {
  data: unknown;
  expiry: number;
}

const cache = new Map<string, CacheEntry>();
const CACHE_TTL = 5 * 60 * 1000; // 5 minutes

let pool: Pool | null = null;

function getPool(): Pool {
  if (!pool) {
    pool = new Pool({
      connectionString: process.env.NEON_DATABASE_URL,
      max: 10,
      idleTimeoutMillis: 30000,
      connectionTimeoutMillis: 30000,
      statement_timeout: 30000,
      ssl: { rejectUnauthorized: false },
    });

    pool.on('error', (err) => {
      console.error('Unexpected pool error:', err);
    });
  }
  return pool;
}

export async function query<T = Record<string, unknown>>(
  sql: string,
  params: unknown[] = [],
  cacheKey?: string
): Promise<T[]> {
  // Check cache
  if (cacheKey) {
    const cached = cache.get(cacheKey);
    if (cached && cached.expiry > Date.now()) {
      return cached.data as T[];
    }
  }

  const p = getPool();
  const client = await p.connect();
  try {
    const result = await client.query(sql, params);
    const data = result.rows as T[];

    // Store in cache
    if (cacheKey) {
      cache.set(cacheKey, {
        data,
        expiry: Date.now() + CACHE_TTL,
      });
    }

    return data;
  } finally {
    client.release();
  }
}

export async function queryOne<T = Record<string, unknown>>(
  sql: string,
  params: unknown[] = [],
  cacheKey?: string
): Promise<T | null> {
  const rows = await query<T>(sql, params, cacheKey);
  return rows[0] || null;
}
