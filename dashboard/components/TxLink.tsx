'use client';

import React from 'react';
import { getExplorerTxUrl } from '@/lib/contracts';
import { truncateHash } from '@/lib/helpers';

interface TxLinkProps {
  hash: string;
  className?: string;
}

export default function TxLink({ hash, className = '' }: TxLinkProps) {
  if (!hash) return <span className="text-text-muted">-</span>;

  return (
    <a
      href={getExplorerTxUrl(hash)}
      target="_blank"
      rel="noopener noreferrer"
      className={`explorer-link ${className}`}
      title={hash}
    >
      {truncateHash(hash)}
    </a>
  );
}
