import { NextResponse } from 'next/server';
import { query, queryOne } from '@/lib/db';

export const dynamic = 'force-dynamic';

const ENTRYPOINT_ADDRESSES = [
  '0x3ec61c5633bbd7afa9144c6610930489736a72d4',
  '0x79fe1f70bdc764cf4fe83c6823d81dd676c7c2a1',
];

export async function GET() {
  try {
    const [totalOps, successRate, totalGasCost, newAccounts, dailyOps, paymasterUsage, recentOps] = await Promise.all([
      queryOne<{ count: string }>(
        `SELECT COUNT(*)::text as count FROM decoded_events
         WHERE event_name = 'UserOperationEvent'`,
        [],
        'aa_total_ops'
      ),

      queryOne<{ total: string; successful: string }>(
        `SELECT
          COUNT(*)::text as total,
          COUNT(*) FILTER (WHERE params->>'success' = 'true')::text as successful
        FROM decoded_events
        WHERE event_name = 'UserOperationEvent'`,
        [],
        'aa_success_rate'
      ),

      queryOne<{ total_gas: string }>(
        `SELECT COALESCE(SUM(
          CASE WHEN params->>'actualGasCost' ~ '^[0-9]+$'
            THEN (params->>'actualGasCost')::numeric
            ELSE 0
          END
        ), 0)::text as total_gas
        FROM decoded_events
        WHERE event_name = 'UserOperationEvent'`,
        [],
        'aa_total_gas'
      ),

      queryOne<{ count: string }>(
        `SELECT COUNT(*)::text as count FROM decoded_events
         WHERE event_name = 'AccountDeployed'`,
        [],
        'aa_new_accounts'
      ),

      query(
        `SELECT DATE("timestamp") as date, COUNT(*)::int as value
         FROM decoded_events
         WHERE event_name = 'UserOperationEvent'
           AND "timestamp" > NOW() - INTERVAL '90 days'
         GROUP BY DATE("timestamp")
         ORDER BY date`,
        [],
        'aa_daily_ops'
      ),

      query(
        `SELECT
          params->>'paymaster' as paymaster,
          COUNT(*)::int as count
        FROM decoded_events
        WHERE event_name = 'UserOperationEvent'
          AND params->>'paymaster' IS NOT NULL
        GROUP BY params->>'paymaster'
        ORDER BY count DESC
        LIMIT 20`,
        [],
        'aa_paymaster_usage'
      ),

      query(
        `SELECT
          params->>'sender' as sender,
          params->>'paymaster' as paymaster,
          params->>'actualGasCost' as gas_cost,
          params->>'success' as success,
          params->>'userOpHash' as user_op_hash,
          "timestamp" as ts,
          transaction_hash
        FROM decoded_events
        WHERE event_name = 'UserOperationEvent'
        ORDER BY "timestamp" DESC
        LIMIT 50`,
        [],
        'aa_recent_ops'
      ),
    ]);

    const total = parseInt(successRate?.total || '0');
    const successful = parseInt(successRate?.successful || '0');

    return NextResponse.json({
      metrics: {
        totalOps: totalOps?.count || '0',
        successRate: total > 0 ? ((successful / total) * 100).toFixed(1) : '0',
        totalGasCost: totalGasCost?.total_gas || '0',
        newAccounts: newAccounts?.count || '0',
      },
      dailyOps: dailyOps.map((r: Record<string, unknown>) => ({
        date: new Date(r.date as string).toLocaleDateString('en-US', { month: 'short', day: 'numeric' }),
        value: r.value,
      })),
      paymasterUsage,
      recentOps: recentOps.map((r: Record<string, unknown>) => ({
        ...r,
        timestamp: r.ts ? new Date(r.ts as string).toISOString() : null,
      })),
    });
  } catch (error) {
    console.error('AA API error:', error);
    return NextResponse.json({ error: 'Failed to fetch AA data' }, { status: 500 });
  }
}
