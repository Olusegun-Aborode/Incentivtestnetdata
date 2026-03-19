'use client';

import { useState, useEffect } from 'react';
import { apiUrl } from '@/lib/helpers';

const STORAGE_KEY = 'datumlabs_incentiv_unlocked';

export default function EmailGate({ children }: { children: React.ReactNode }) {
  const [unlocked, setUnlocked] = useState(false);
  const [mounted, setMounted] = useState(false);
  const [email, setEmail] = useState('');
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    setMounted(true);
    if (localStorage.getItem(STORAGE_KEY) === 'true') {
      setUnlocked(true);
    }
  }, []);

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    setError(null);

    if (!email || !email.includes('@')) {
      setError('Please enter a valid email address.');
      return;
    }

    setSubmitting(true);
    try {
      const res = await fetch(apiUrl('/api/subscribe'), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ email }),
      });

      if (!res.ok) {
        const body = await res.json().catch(() => null);
        throw new Error(body?.error ?? 'Subscription failed. Please try again.');
      }

      localStorage.setItem(STORAGE_KEY, 'true');
      setUnlocked(true);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Something went wrong.');
    } finally {
      setSubmitting(false);
    }
  }

  if (!mounted) return null;
  if (unlocked) return <>{children}</>;

  return (
    <div className="relative">
      {/* Blurred content preview */}
      <div
        className="select-none max-h-[500px] overflow-hidden"
        style={{ filter: 'blur(6px)', pointerEvents: 'none' }}
      >
        {children}
      </div>

      {/* Gate overlay */}
      <div className="fixed inset-0 z-50 flex items-center justify-center bg-white/70 backdrop-blur-sm">
        <div className="bg-white rounded-xl shadow-2xl border border-gray-200 max-w-md w-full mx-4">
          {/* Header */}
          <div className="flex items-center justify-between px-6 py-4 border-b border-gray-100">
            <div className="flex items-center gap-2">
              <img
                src={apiUrl('/branding/icon.png')}
                alt="Datum Labs"
                className="w-5 h-5"
                onError={(e) => { (e.target as HTMLImageElement).style.display = 'none'; }}
              />
              <span className="text-sm font-bold text-gray-900 tracking-wide uppercase">Datum Labs</span>
            </div>
            <span className="text-[10px] font-semibold text-orange-600 bg-orange-50 px-2 py-0.5 rounded uppercase tracking-wider">
              Locked
            </span>
          </div>

          {/* Body */}
          <div className="p-6 space-y-5">
            <div className="text-sm text-gray-500 space-y-1.5">
              <p>
                <span className="text-orange-600 font-semibold">&gt;</span> Full dashboard access includes:
              </p>
              <p className="pl-4">• Real-time on-chain analytics</p>
              <p className="pl-4">• Bridge & DEX activity tracking</p>
              <p className="pl-4">• Account Abstraction insights</p>
              <p className="pl-4">• Token & address metrics</p>
              <p className="pl-4">• Cross-chain flow analysis</p>
              <p className="mt-3">
                <span className="text-orange-600 font-semibold">&gt;</span> Enter email to unlock
              </p>
            </div>

            <form onSubmit={handleSubmit} className="space-y-3">
              <div className="flex items-center gap-2 rounded-lg px-3 py-2.5 bg-gray-50 border border-gray-200 focus-within:border-orange-400 transition-colors">
                <span className="text-xs text-orange-600">&gt;</span>
                <input
                  type="email"
                  placeholder="you@email.com"
                  value={email}
                  onChange={(e) => setEmail(e.target.value)}
                  className="flex-1 bg-transparent text-sm text-gray-900 placeholder:text-gray-400 focus:outline-none"
                />
              </div>

              <button
                type="submit"
                disabled={submitting}
                className="w-full font-bold rounded-lg py-2.5 text-xs uppercase tracking-wider transition-colors bg-orange-600 text-white hover:bg-orange-700 disabled:opacity-50 disabled:cursor-not-allowed"
              >
                {submitting ? 'Authenticating...' : 'Unlock Dashboard'}
              </button>
            </form>

            {error && (
              <p className="text-xs text-red-600">
                <span className="font-semibold">[ERR]</span> {error}
              </p>
            )}

            <p className="text-[10px] text-center text-gray-400">
              Join the Datum Labs newsletter for full access
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}
