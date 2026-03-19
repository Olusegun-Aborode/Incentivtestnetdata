import { NextResponse } from 'next/server';
import { isRefreshing, getRefreshingKeys } from '@/lib/db';

export const dynamic = 'force-dynamic';

export async function GET() {
  return NextResponse.json(
    {
      refreshing: isRefreshing(),
      keys: getRefreshingKeys(),
    },
    {
      headers: { 'Cache-Control': 'no-store' },
    },
  );
}
