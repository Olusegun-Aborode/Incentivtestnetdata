import { NextResponse } from 'next/server';
import { query, queryOne } from '@/lib/db';

export const dynamic = 'force-dynamic';

export async function GET() {
  try {
    const [
      blockCount,
      txCount,
      uniqueAddresses,
      contractCount,
      eventCount,
      dailyTxs,
      dailyActiveAddresses,
      dailyGas,
      networkStats,
    ] = await Promise.all([
      queryOne<{ count: string }>('SELECT COUNT(*)::text as count FROM blocks', [], 'overview_blocks'),
      queryOne<{ count: string }>('SELECT COUNT(*)::text as count FROM transactions', [], 'overview_txs'),
      queryOne<{ count: string }>(
        `SELECT COUNT(DISTINCT addr)::text as count FROM (
          SELECT from_address as addr FROM transactions
          UNION
          SELECT to_address as addr FROM transactions WHERE to_address IS NOT NULL
        ) t`,
        [],
        'overview_addrs'
      ),
      queryOne<{ count: string }>('SELECT COUNT(*)::text as count FROM contracts', [], 'overview_contracts'),
      queryOne<{ count: string }>('SELECT COUNT(*)::text as count FROM decoded_events', [], 'overview_events'),

      query(
        `SELECT DATE(block_timestamp::timestamp) as date, COUNT(*)::int as value
         FROM transactions
         WHERE block_timestamp::timestamp > NOW() - INTERVAL '90 days'
         GROUP BY DATE(block_timestamp::timestamp)
         ORDER BY date`,
        [],
        'overview_daily_txs'
      ),

      query(
        `SELECT DATE(block_timestamp::timestamp) as date, COUNT(DISTINCT from_address)::int as value
         FROM transactions
         WHERE block_timestamp::timestamp > NOW() - INTERVAL '90 days'
         GROUP BY DATE(block_timestamp::timestamp)
         ORDER BY date`,
        [],
        'overview_daily_active'
      ),

      query(
        `SELECT DATE("timestamp"::timestamp) as date, AVG(gas_used)::bigint as value
         FROM blocks
         WHERE "timestamp"::timestamp > NOW() - INTERVAL '90 days'
         GROUP BY DATE("timestamp"::timestamp)
         ORDER BY date`,
        [],
        'overview_daily_gas'
      ),

      queryOne<{ avg_block_time: string; avg_txs_per_block: string; total_gas: string }>(
        `SELECT
          EXTRACT(EPOCH FROM (MAX("timestamp"::timestamp) - MIN("timestamp"::timestamp))) / NULLIF(COUNT(*) - 1, 0) as avg_block_time,
          AVG(transaction_count) as avg_txs_per_block,
          SUM(gas_used)::text as total_gas
        FROM blocks
        WHERE "timestamp"::timestamp > NOW() - INTERVAL '7 days'`,
        [],
        'overview_network_stats'
      ),
    ]);

    return NextResponse.json({
      metrics: {
        totalBlocks: blockCount?.count || '0',
        totalTransactions: txCount?.count || '0',
        uniqueAddresses: uniqueAddresses?.count || '0',
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
      },
    });
  } catch (error) {
    console.error('Overview API error:', error);
    return NextResponse.json({ error: 'Failed to fetch overview data' }, { status: 500 });
  }
}
