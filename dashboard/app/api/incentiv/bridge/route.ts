import { NextResponse } from 'next/server';
import { query, queryOne, CACHE_TTLS } from '@/lib/db';

export const dynamic = 'force-dynamic';

function cachedResponse(data: unknown) {
  return NextResponse.json(data, {
    headers: { 'Cache-Control': 'public, s-maxage=300, stale-while-revalidate=600' },
  });
}

export async function GET() {
  try {
    const [inbound, outbound, inboundVolume, dailyBridge, bridgeByToken, recentBridge, chainFlows] = await Promise.all([
      queryOne<{ count: string }>(
        `SELECT COUNT(*)::text as count FROM decoded_events
         WHERE event_name = 'ReceivedTransferRemote'`,
        [],
        'bridge_inbound_count',
        CACHE_TTLS.COUNTS
      ),

      queryOne<{ count: string }>(
        `SELECT COUNT(*)::text as count FROM decoded_events
         WHERE event_name = 'SentTransferRemote'`,
        [],
        'bridge_outbound_count',
        CACHE_TTLS.COUNTS
      ),

      // Total bridge-in volume by token (sum amounts)
      query(
        `SELECT
          contract_address,
          SUM(CASE
            WHEN params->>'amount' ~ '^[0-9]+$' THEN (params->>'amount')::numeric
            ELSE 0
          END) as total_volume,
          COUNT(*)::int as tx_count
        FROM decoded_events
        WHERE event_name = 'ReceivedTransferRemote'
        GROUP BY contract_address
        ORDER BY tx_count DESC`,
        [],
        'bridge_inbound_volume',
        CACHE_TTLS.COUNTS
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
        'bridge_daily',
        CACHE_TTLS.DAILY_SERIES
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
        'bridge_by_token',
        CACHE_TTLS.LEADERBOARD
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
        'bridge_recent',
        CACHE_TTLS.RECENT
      ),

      // Chain flow breakdown: which chains users bridge FROM and TO
      query(
        `SELECT
          event_name,
          COALESCE(params->>'origin', params->>'destination') as chain_id,
          COUNT(*)::int as count,
          COUNT(DISTINCT LOWER(params->>'recipient'))::int as unique_users
        FROM decoded_events
        WHERE event_name IN ('ReceivedTransferRemote', 'SentTransferRemote')
          AND COALESCE(params->>'origin', params->>'destination') IS NOT NULL
        GROUP BY event_name, chain_id
        ORDER BY count DESC`,
        [],
        'bridge_chain_flows',
        CACHE_TTLS.LEADERBOARD
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

    // Build chain flow summary: inbound sources and outbound destinations
    const inboundChains: { chain: string; chain_id: string; count: number; unique_users: number }[] = [];
    const outboundChains: { chain: string; chain_id: string; count: number; unique_users: number }[] = [];
    for (const r of chainFlows) {
      const chainId = r.chain_id as string;
      const id = parseInt(chainId);
      const CHAIN_NAMES: Record<number, string> = {
        1: 'Ethereum', 42161: 'Arbitrum', 1399811149: 'Solana',
        10: 'Optimism', 137: 'Polygon', 8453: 'Base', 56: 'BSC',
      };
      const chainName = CHAIN_NAMES[id] || `Chain ${chainId}`;
      const entry = {
        chain: chainName,
        chain_id: chainId,
        count: r.count as number,
        unique_users: r.unique_users as number,
      };
      if (r.event_name === 'ReceivedTransferRemote') {
        inboundChains.push(entry); // These chains are sending TO Incentiv
      } else {
        outboundChains.push(entry); // These chains are receiving FROM Incentiv
      }
    }

    return cachedResponse({
      metrics: {
        inbound: inbound?.count || '0',
        outbound: outbound?.count || '0',
      },
      inboundVolume: inboundVolume.map((r: Record<string, unknown>) => ({
        contract_address: r.contract_address,
        total_volume: r.total_volume?.toString() || '0',
        tx_count: r.tx_count,
      })),
      dailyBridge: Array.from(dateMap.values()),
      bridgeByToken,
      chainFlows: {
        inboundFrom: inboundChains,  // Chains sending assets TO Incentiv
        outboundTo: outboundChains,  // Chains receiving assets FROM Incentiv
      },
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
