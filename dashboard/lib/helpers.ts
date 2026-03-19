export function formatNumber(n: number | string | null | undefined): string {
  if (n === null || n === undefined) return '0';
  const num = typeof n === 'string' ? parseFloat(n) : n;
  if (isNaN(num)) return '0';
  if (num >= 1_000_000_000) return (num / 1_000_000_000).toFixed(2) + 'B';
  if (num >= 1_000_000) return (num / 1_000_000).toFixed(2) + 'M';
  if (num >= 1_000) return num.toLocaleString('en-US', { maximumFractionDigits: 0 });
  if (num < 0.01 && num > 0) return num.toFixed(6);
  if (num % 1 !== 0) return num.toLocaleString('en-US', { maximumFractionDigits: 2 });
  return num.toLocaleString('en-US');
}

export function formatCompact(n: number | string | null | undefined): string {
  if (n === null || n === undefined) return '0';
  const num = typeof n === 'string' ? parseFloat(n) : n;
  if (isNaN(num)) return '0';
  if (num >= 1_000_000_000) return (num / 1_000_000_000).toFixed(1) + 'B';
  if (num >= 1_000_000) return (num / 1_000_000).toFixed(1) + 'M';
  if (num >= 1_000) return (num / 1_000).toFixed(1) + 'K';
  return num.toString();
}

export function truncateAddress(address: string): string {
  if (!address) return '';
  if (address.length < 12) return address;
  return `${address.slice(0, 6)}...${address.slice(-4)}`;
}

export function truncateHash(hash: string): string {
  if (!hash) return '';
  if (hash.length < 14) return hash;
  return `${hash.slice(0, 8)}...${hash.slice(-6)}`;
}

export function formatRelativeTime(timestamp: string | Date | null | undefined): string {
  if (!timestamp) return '—';
  const now = new Date();
  const date = typeof timestamp === 'string' ? new Date(timestamp) : timestamp;
  if (isNaN(date.getTime())) return '—';
  const diffMs = now.getTime() - date.getTime();
  const diffSec = Math.floor(diffMs / 1000);
  const diffMin = Math.floor(diffSec / 60);
  const diffHour = Math.floor(diffMin / 60);
  const diffDay = Math.floor(diffHour / 24);

  if (diffSec < 60) return `${diffSec}s ago`;
  if (diffMin < 60) return `${diffMin}m ago`;
  if (diffHour < 24) return `${diffHour}h ago`;
  if (diffDay < 7) return `${diffDay}d ago`;

  return date.toLocaleDateString('en-US', {
    month: 'short',
    day: 'numeric',
    hour: 'numeric',
    minute: '2-digit',
  });
}

export function formatTimestamp(timestamp: string | Date | null | undefined): string {
  if (!timestamp) return '—';
  const date = typeof timestamp === 'string' ? new Date(timestamp) : timestamp;
  if (isNaN(date.getTime())) return '—';
  return date.toLocaleDateString('en-US', {
    month: 'short',
    day: 'numeric',
    hour: 'numeric',
    minute: '2-digit',
  });
}

export function formatTokenValue(value: string | number, decimals: number): string {
  if (!value) return '0';
  const raw = typeof value === 'string' ? BigInt(value) : BigInt(Math.floor(value));
  const divisor = BigInt(10 ** decimals);
  const whole = raw / divisor;
  const fraction = raw % divisor;
  const fractionStr = fraction.toString().padStart(decimals, '0').slice(0, 2);
  return `${whole.toLocaleString('en-US')}.${fractionStr}`;
}

export function formatGwei(wei: string | number): string {
  const n = typeof wei === 'string' ? parseFloat(wei) : wei;
  return (n / 1e9).toFixed(2) + ' Gwei';
}

export function formatEther(wei: string | number): string {
  const n = typeof wei === 'string' ? parseFloat(wei) : wei;
  return (n / 1e18).toFixed(4) + ' ETH';
}
