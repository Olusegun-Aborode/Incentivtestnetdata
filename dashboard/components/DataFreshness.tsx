'use client';

import React, { useEffect, useState } from 'react';
import { apiUrl } from '@/lib/helpers';

/**
 * A subtle status indicator that shows when dashboard data is being
 * refreshed in the background (stale-while-revalidate pattern).
 *
 * It polls a lightweight API endpoint every 10 seconds to check whether
 * any cache keys are currently refreshing on the server.
 */
export default function DataFreshness() {
  const [refreshing, setRefreshing] = useState(false);

  useEffect(() => {
    let mounted = true;

    async function check() {
      try {
        const res = await fetch(apiUrl('/api/incentiv/cache-status'));
        if (res.ok) {
          const json = await res.json();
          if (mounted) setRefreshing(json.refreshing === true);
        }
      } catch {
        // Silently ignore — the indicator just won't show
      }
    }

    // Initial check
    check();
    const interval = setInterval(check, 10_000);

    return () => {
      mounted = false;
      clearInterval(interval);
    };
  }, []);

  if (!refreshing) return null;

  return (
    <span className="inline-flex items-center gap-1.5 text-[10px] text-accent-orange animate-pulse">
      <span
        className="inline-block w-1.5 h-1.5 rounded-full bg-accent-orange"
        aria-hidden="true"
      />
      Data updating&hellip;
    </span>
  );
}
