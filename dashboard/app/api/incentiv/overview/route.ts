import { NextResponse } from 'next/server';
import { query, queryOne, CACHE_TTLS } from '@/lib/db';

export const dynamic = 'force-dynamic';

function cachedResponse(data: unknown) {
  return NextResponse.json(data, {
    headers: {
      'Cache-Control': 'public, s-maxage=300, stale-while-revalidate=600',
    },
  });
}

// Cache for BlockScout stats (total addresses, total blocks)
let explorerStatsCache: { totalAddresses: string; totalBlocks: string; expiry: number } | null = null;
const EXPLORER_CACHE_TTL = 5 * 60 * 1000; // 5 minutes

async function getExplorerStats(): Promise<{ totalAddresses: string; totalBlocks: string } | null> {
  if (explorerStatsCache && explorerStatsCache.expiry > Date.now()) {
    return { totalAddresses: explorerStatsCache.totalAddresses, totalBlocks: explorerStatsCache.totalBlocks };
  }

  try {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 10000);
    const resp = await fetch('https://explorer.incentiv.io/api/v2/stats', {
      signal: controller.signal,
      headers: { Accept: 'application/json' },
    });
    clearTimeout(timeout);

    if (resp.ok) {
      const data = await resp.json();
      const stats = {
        totalAddresses: data.total_addresses || '0',
        totalBlocks: data.total_blocks || '0',
      };
      explorerStatsCache = { ...stats, expiry: Date.now() + EXPLORER_CACHE_TTL };
      return stats;
    }
  } catch (error) {
    console.error('BlockScout stats fetch error:', error);
  }
  return null;
}

export async function GET() {
  try {
    // Fetch explorer stats in parallel with DB queries for accurate total addresses/blocks
    const explorerStatsPromise = getExplorerStats();

    const [
      blockCount,
      txCount,
      contractCount,
      eventCount,
      addressCount,
      dailyTxs,
      dailyActiveAddresses,
      dailyGas,
      networkStats,
    ] = await Promise.all([
      // Use MAX(number) for total blocks since block numbers are sequential from genesis
      queryOne<{ count: string }>(
        `SELECT GREATEST(MAX(number), COUNT(*))::text as count FROM blocks`,
        [],
        'overview_blocks',
        CACHE_TTLS.COUNTS
      ),
      queryOne<{ count: string }>('SELECT COUNT(*)::text as count FROM transactions', [], 'overview_txs', CACHE_TTLS.COUNTS),
      queryOne<{ count: string }>('SELECT COUNT(*)::text as count FROM contracts', [], 'overview_contracts', CACHE_TTLS.COUNTS),
      queryOne<{ count: string }>('SELECT COUNT(*)::text as count FROM decoded_events', [], 'overview_events', CACHE_TTLS.COUNTS),
      // Our own indexed address count from the addresses table
      queryOne<{ count: string }>('SELECT COUNT(*)::text as count FROM addresses', [], 'overview_addresses', CACHE_TTLS.COUNTS),

      query(
        `SELECT DATE(block_timestamp::timestamp) as date, COUNT(*)::int as value
         FROM transactions
         WHERE block_timestamp::timestamp > NOW() - INTERVAL '90 days'
         GROUP BY DATE(block_timestamp::timestamp)
         ORDER BY date`,
        [],
        'overview_daily_txs',
        CACHE_TTLS.DAILY_SERIES
      ),

      query(
        `SELECT date, COUNT(DISTINCT addr)::int as value FROM (
          SELECT DATE(block_timestamp::timestamp) as date, from_address as addr
            FROM transactions
            WHERE block_timestamp::timestamp > NOW() - INTERVAL '90 days'
          UNION ALL
          SELECT DATE(block_timestamp::timestamp) as date, to_address as addr
            FROM transactions
            WHERE block_timestamp::timestamp > NOW() - INTERVAL '90 days'
            AND to_address IS NOT NULL
        ) t
        GROUP BY date
        ORDER BY date`,
        [],
        'overview_daily_active',
        CACHE_TTLS.DAILY_SERIES
      ),

      query(
        `SELECT DATE("timestamp"::timestamp) as date, AVG(gas_used)::bigint as value
         FROM blocks
         WHERE "timestamp"::timestamp > NOW() - INTERVAL '90 days'
         GROUP BY DATE("timestamp"::timestamp)
         ORDER BY date`,
        [],
        'overview_daily_gas',
        CACHE_TTLS.DAILY_SERIES
      ),

      queryOne<{ avg_block_time: string; avg_txs_per_block: string; total_gas: string; avg_gas_per_block: string }>(
        `SELECT
          EXTRACT(EPOCH FROM (MAX("timestamp"::timestamp) - MIN("timestamp"::timestamp))) / NULLIF(COUNT(*) - 1, 0) as avg_block_time,
          AVG(transaction_count) as avg_txs_per_block,
          SUM(gas_used)::text as total_gas,
          AVG(gas_used)::bigint::text as avg_gas_per_block
        FROM blocks
        WHERE "timestamp"::timestamp > NOW() - INTERVAL '7 days'`,
        [],
        'overview_network_stats',
        CACHE_TTLS.NETWORK
      ),
    ]);

    // Use BlockScout explorer stats for total addresses and total blocks (most accurate)
    const explorerStats = await explorerStatsPromise;

    // For total blocks: prefer explorer stats, fall back to our DB MAX(number)
    const totalBlocks = explorerStats?.totalBlocks || blockCount?.count || '0';

    // For unique addresses: use explorer stats (most complete), fall back to our indexed addresses table
    const uniqueAddresses = explorerStats?.totalAddresses || addressCount?.count || '0';

    return cachedResponse({
      metrics: {
        totalBlocks,
        totalTransactions: txCount?.count || '0',
        uniqueAddresses,
        totalContracts: contractCount?.count || '0',
        totalEvents: eventCount?.count || '0',
      },
      dailyTransactions: dailyTxs.map((r: Record<string, unknown>) => ({
        date: new Date(r.date as string).toLocaleDateString('en-US', { month: 'short', day: 'numeric' }),
        value: r.value,
      })),
      dailyActiveAddresses: dailyActiveAddresses.map((r: Record<string, unknown>) => ({
        date: new Date(r.date as string).toLocaleDateString('en-US', { month: 'short', day: 'numeric' }),
        value: r.value,
      })),
      dailyGas: dailyGas.map((r: Record<string, unknown>) => ({
        date: new Date(r.date as string).toLocaleDateString('en-US', { month: 'short', day: 'numeric' }),
        value: Number(r.value),
      })),
      networkStats: {
        avgBlockTime: networkStats?.avg_block_time ? parseFloat(networkStats.avg_block_time).toFixed(2) : '0',
        avgTxsPerBlock: networkStats?.avg_txs_per_block ? parseFloat(networkStats.avg_txs_per_block).toFixed(1) : '0',
        totalGas: networkStats?.total_gas || '0',
        avgGasPerBlock: networkStats?.avg_gas_per_block || '0',
      },
    });
  } catch (error) {
    console.error('Overview API error:', error);
    return NextResponse.json({ error: 'Failed to fetch overview data' }, { status: 500 });
  }
}
