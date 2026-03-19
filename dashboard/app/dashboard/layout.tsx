'use client';

import React from 'react';
import Link from 'next/link';
import { usePathname } from 'next/navigation';

const NAV_ITEMS = [
  { href: '/dashboard/overview', label: 'Overview' },
  { href: '/dashboard/tokens', label: 'Tokens' },
  { href: '/dashboard/dex', label: 'DEX' },
  { href: '/dashboard/account-abstraction', label: 'Account Abstraction' },
  { href: '/dashboard/bridge', label: 'Bridge' },
  { href: '/dashboard/activity', label: 'Activity' },
];

export default function DashboardLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  const pathname = usePathname();

  return (
    <div className="min-h-screen flex flex-col">
      {/* Top Navigation Bar */}
      <header className="h-12 border-b border-[rgba(255,255,255,0.08)] bg-panel-header flex items-center px-4 sticky top-0 z-40">
        <div className="flex items-center gap-3 mr-8">
          <img src="/branding/icon.png" alt="Datum Labs" className="w-6 h-6" onError={(e) => { (e.target as HTMLImageElement).style.display = 'none'; }} />
          <span className="text-accent-orange font-semibold text-sm tracking-wider uppercase">Datum Labs</span>
          <span className="text-text-muted text-[10px] tracking-widest uppercase ml-1">Incentiv Explorer</span>
        </div>

        <nav className="flex items-center gap-1 overflow-x-auto flex-1">
          {NAV_ITEMS.map((item) => (
            <Link
              key={item.href}
              href={item.href}
              className={`nav-link whitespace-nowrap ${pathname === item.href ? 'active' : ''}`}
            >
              {item.label}
            </Link>
          ))}
        </nav>

        <div className="flex items-center gap-2 ml-4">
          <span className="status-dot" />
          <span className="text-accent-orange text-[11px] font-semibold uppercase tracking-wider">Live</span>
        </div>
      </header>

      {/* Main Content */}
      <main className="flex-1 p-4 md:p-6 pb-12 overflow-auto">
        {children}
      </main>

      {/* Status Bar */}
      <div className="status-bar">
        <div className="flex items-center gap-2">
          <span className="chain-badge">Incentiv Testnet</span>
        </div>
        <div className="flex-1" />
        <span className="text-[10px]">Chain ID: 15557</span>
        <span className="text-[10px]">|</span>
        <span className="text-[10px]">RPC: incentiv-testnet.rpc.caldera.xyz</span>
        <span className="text-[10px]">|</span>
        <span className="text-[10px]">Powered by Datum Labs</span>
      </div>
    </div>
  );
}
