import { NextResponse } from 'next/server';
import { query, queryOne } from '@/lib/db';

export const dynamic = 'force-dynamic';

export async function GET() {
  try {
    const [inbound, outbound, dailyBridge, bridgeByToken, recentBridge] = await Promise.all([
      queryOne<{ count: string }>(
        `SELECT COUNT(*)::text as count FROM decoded_events
         WHERE event_name = 'ReceivedTransferRemote'`,
        [],
        'bridge_inbound_count'
      ),

      queryOne<{ count: string }>(
        `SELECT COUNT(*)::text as count FROM decoded_events
         WHERE event_name = 'SentTransferRemote'`,
        [],
        'bridge_outbound_count'
      ),

      query(
        `SELECT
          DATE("timestamp") as date,
          event_name,
          COUNT(*)::int as count
        FROM decoded_events
        WHERE event_name IN ('ReceivedTransferRemote', 'SentTransferRemote')
          AND "timestamp" > NOW() - INTERVAL '90 days'
        GROUP BY DATE("timestamp"), event_name
        ORDER BY date`,
        [],
        'bridge_daily'
      ),

      query(
        `SELECT
          contract_address,
          event_name,
          COUNT(*)::int as count
        FROM decoded_events
        WHERE event_name IN ('ReceivedTransferRemote', 'SentTransferRemote')
        GROUP BY contract_address, event_name
        ORDER BY count DESC`,
        [],
        'bridge_by_token'
      ),

      query(
        `SELECT
          event_name,
          contract_address,
          params->>'amount' as amount,
          COALESCE(params->>'origin', params->>'destination') as chain_id,
          params->>'recipient' as recipient,
          "timestamp" as ts,
          transaction_hash
        FROM decoded_events
        WHERE event_name IN ('ReceivedTransferRemote', 'SentTransferRemote')
        ORDER BY "timestamp" DESC
        LIMIT 50`,
        [],
        'bridge_recent'
      ),
    ]);

    // Transform daily bridge data into paired inbound/outbound
    const dateMap = new Map<string, { date: string; inbound: number; outbound: number }>();
    for (const r of dailyBridge) {
      const dateStr = new Date(r.date as string).toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
      if (!dateMap.has(dateStr)) {
        dateMap.set(dateStr, { date: dateStr, inbound: 0, outbound: 0 });
      }
      const entry = dateMap.get(dateStr)!;
      if (r.event_name === 'ReceivedTransferRemote') entry.inbound = r.count as number;
      else entry.outbound = r.count as number;
    }

    return NextResponse.json({
      metrics: {
        inbound: inbound?.count || '0',
        outbound: outbound?.count || '0',
      },
      dailyBridge: Array.from(dateMap.values()),
      bridgeByToken,
      recentBridge: recentBridge.map((r: Record<string, unknown>) => ({
        ...r,
        timestamp: r.ts ? new Date(r.ts as string).toISOString() : null,
      })),
    });
  } catch (error) {
    console.error('Bridge API error:', error);
    return NextResponse.json({ error: 'Failed to fetch bridge data' }, { status: 500 });
  }
}
