import { NextResponse } from 'next/server';
import { query, CACHE_TTLS } from '@/lib/db';

export const dynamic = 'force-dynamic';

function cachedResponse(data: unknown) {
  return NextResponse.json(data, {
    headers: { 'Cache-Control': 'public, s-maxage=300, stale-while-revalidate=600' },
  });
}

const POOL_ADDRESSES = [
  '0xf9884c2a1749b0a02ce780ade437cbadfa3a961d', // WCENT/USDC
  '0xd1da5c73eb5b498dea4224267feea3a3de82ba4e', // WCENT/USDT
];

export async function GET() {
  try {
    const [dailySwaps, poolActivity, recentSwaps, liquidityEvents] = await Promise.all([
      // Daily swap count
      query(
        `SELECT DATE("timestamp") as date, COUNT(*)::int as value
         FROM decoded_events
         WHERE event_name = 'Swap'
           AND "timestamp" > NOW() - INTERVAL '90 days'
         GROUP BY DATE("timestamp")
         ORDER BY date`,
        [],
        'dex_daily_swaps',
        CACHE_TTLS.DAILY_SERIES
      ),

      // Pool activity breakdown
      query(
        `SELECT
          contract_address,
          COUNT(*)::int as swap_count,
          MIN("timestamp") as first_swap,
          MAX("timestamp") as last_swap
        FROM decoded_events
        WHERE event_name = 'Swap'
          AND contract_address = ANY($1)
        GROUP BY contract_address
        ORDER BY swap_count DESC`,
        [POOL_ADDRESSES],
        'dex_pool_activity',
        CACHE_TTLS.LEADERBOARD
      ),

      // Recent swaps
      query(
        `SELECT
          contract_address,
          params->>'sender' as sender,
          params->>'recipient' as recipient,
          params->>'amount0' as amount0,
          params->>'amount1' as amount1,
          params->>'tick' as tick,
          "timestamp" as ts,
          transaction_hash
        FROM decoded_events
        WHERE event_name = 'Swap'
        ORDER BY "timestamp" DESC
        LIMIT 50`,
        [],
        'dex_recent_swaps',
        CACHE_TTLS.RECENT
      ),

      // Liquidity events (Mint/Burn)
      query(
        `SELECT event_name, COUNT(*)::int as count
         FROM decoded_events
         WHERE event_name IN ('Mint', 'Burn')
           AND contract_address = ANY($1)
         GROUP BY event_name`,
        [POOL_ADDRESSES],
        'dex_liquidity',
        CACHE_TTLS.COUNTS
      ),
    ]);

    return cachedResponse({
      dailySwaps: dailySwaps.map((r: Record<string, unknown>) => ({
        date: new Date(r.date as string).toLocaleDateString('en-US', { month: 'short', day: 'numeric' }),
        value: r.value,
      })),
      poolActivity,
      recentSwaps: recentSwaps.map((r: Record<string, unknown>) => ({
        ...r,
        timestamp: r.ts ? new Date(r.ts as string).toISOString() : null,
      })),
      liquidityEvents,
    });
  } catch (error) {
    console.error('DEX API error:', error);
    return NextResponse.json({ error: 'Failed to fetch DEX data' }, { status: 500 });
  }
}
