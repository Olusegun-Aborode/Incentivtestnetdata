import { NextResponse } from 'next/server';
import { query, CACHE_TTLS } from '@/lib/db';

export const dynamic = 'force-dynamic';

function cachedResponse(data: unknown) {
  return NextResponse.json(data, {
    headers: { 'Cache-Control': 'public, s-maxage=300, stale-while-revalidate=600' },
  });
}

const TOKEN_ADDRESSES = [
  '0x16e43840d8d79896a389a3de85ab0b0210c05685', // USDC
  '0x39b076b5d23f588690d480af3bf820edad31a4bb', // USDT
  '0xfac24134dbc4b00ee11114ecdfe6397f389203e3', // SOL
  '0xb0f0a14a50f14dc9e6476d61c00cf0375dd4eb04', // WCENT
];

export async function GET() {
  try {
    const [dailyVolume, topHolders, recentTransfers] = await Promise.all([
      // Daily transfer volume by token (last 90 days)
      query(
        `SELECT
          DATE("timestamp") as date,
          contract_address,
          COUNT(*)::int as transfer_count,
          SUM(CASE
            WHEN params->>'value' ~ '^[0-9]+$' THEN (params->>'value')::numeric
            ELSE 0
          END) as raw_volume
        FROM decoded_events
        WHERE event_name = 'Transfer'
          AND contract_address = ANY($1)
          AND "timestamp" > NOW() - INTERVAL '90 days'
        GROUP BY DATE("timestamp"), contract_address
        ORDER BY date`,
        [TOKEN_ADDRESSES],
        'tokens_daily_volume',
        CACHE_TTLS.DAILY_SERIES
      ),

      // Top addresses by transfer activity
      query(
        `SELECT
          contract_address,
          params->>'to' as address,
          COUNT(*)::int as transfer_count
        FROM decoded_events
        WHERE event_name = 'Transfer'
          AND contract_address = ANY($1)
          AND params->>'to' IS NOT NULL
        GROUP BY contract_address, params->>'to'
        ORDER BY transfer_count DESC
        LIMIT 50`,
        [TOKEN_ADDRESSES],
        'tokens_top_holders',
        CACHE_TTLS.LEADERBOARD
      ),

      // Recent token transfers
      query(
        `SELECT
          contract_address,
          params->>'from' as from_addr,
          params->>'to' as to_addr,
          params->>'value' as value,
          "timestamp" as ts,
          transaction_hash
        FROM decoded_events
        WHERE event_name = 'Transfer'
          AND contract_address = ANY($1)
        ORDER BY "timestamp" DESC NULLS LAST
        LIMIT 50`,
        [TOKEN_ADDRESSES],
        'tokens_recent_transfers_v2',
        CACHE_TTLS.RECENT
      ),
    ]);

    return cachedResponse({
      dailyVolume: dailyVolume.map((r: Record<string, unknown>) => ({
        date: new Date(r.date as string).toLocaleDateString('en-US', { month: 'short', day: 'numeric' }),
        contract_address: r.contract_address,
        transfer_count: r.transfer_count,
        raw_volume: r.raw_volume?.toString() || '0',
      })),
      topHolders,
      recentTransfers: recentTransfers.map((r: Record<string, unknown>) => ({
        ...r,
        timestamp: r.ts ? new Date(r.ts as string).toISOString() : null,
        value: r.value?.toString() || '0',
      })),
    });
  } catch (error) {
    console.error('Tokens API error:', error);
    return NextResponse.json({ error: 'Failed to fetch token data' }, { status: 500 });
  }
}
