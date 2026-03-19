'use client';

import React from 'react';
import { CONTRACT_REGISTRY, getExplorerAddressUrl } from '@/lib/contracts';
import { truncateAddress } from '@/lib/helpers';

interface AddressLinkProps {
  address: string;
  className?: string;
}

export default function AddressLink({ address, className = '' }: AddressLinkProps) {
  if (!address) return <span className="text-text-muted">-</span>;

  const lower = address.toLowerCase();
  const entry = CONTRACT_REGISTRY[lower];
  const display = entry ? entry.name : truncateAddress(address);
  const url = getExplorerAddressUrl(address);

  return (
    <a
      href={url}
      target="_blank"
      rel="noopener noreferrer"
      className={`explorer-link ${entry ? 'font-semibold' : ''} ${className}`}
      title={address}
    >
      {display}
    </a>
  );
}
