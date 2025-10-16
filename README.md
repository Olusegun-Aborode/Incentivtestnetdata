# Incentiv Testnet RPC → Dune Sync

This repository now focuses on directly syncing Incentiv Testnet logs from the JSON-RPC endpoint into a Dune table.

## Overview
- Source: Incentiv Testnet RPC (eth_getLogs)
- Destination: Dune table (default: incentiv_testnet_raw_logs_rpc)
- Script: fetch_sync_to_dune.py
- Checkpointing: last_block.txt to continue from the next block on subsequent runs
- Reliability: retries with exponential backoff and a small reorg overlap window to avoid missing data
- Scheduling: hourly system cron job (optional GitHub Actions available)

## Prerequisites
- Python 3.9+
- pip install:
  - requests
  - python-dotenv
- .env file in the project root with:
  - DUNE_API_KEY=<your_dune_api_key>
  - INCENTIVE_RPC_URL=https://rpc1.testnet.incentiv.io/
  - Optional tuning:
    - BLOCK_BATCH_SIZE=100
    - REORG_OVERLAP_BLOCKS=5
    - MAX_RPC_RETRIES=5
    - DUNE_UPLOAD_RETRIES=3
    - BACKOFF_BASE_SECONDS=1
    - BACKOFF_MAX_SECONDS=16

## Run a one-off sync
From the project root:

```bash
python3 fetch_sync_to_dune.py
```

This will:
- Read last_block.txt if present and re-fetch the last N blocks (reorg overlap)
- Pull logs with eth_getLogs for the computed range
- Upload to Dune as CSV
- Advance last_block.txt for the next run

## Continuous sync (cron)
A local cron entry can run the sync hourly and append logs to cron_sync.log.

Example entry:
```
0 * * * * cd /Users/olusegunaborode/Documents/trae_projects/goldskycli && /usr/bin/env python3 fetch_sync_to_dune.py >> /Users/olusegunaborode/Documents/trae_projects/goldskycli/cron_sync.log 2>&1
```

Logs: /Users/olusegunaborode/Documents/trae_projects/goldskycli/cron_sync.log

If you prefer GitHub Actions instead of local cron, we can set up a workflow that uses repository secrets and runs on a schedule.

## Dune query: de-duplicate (reorg-safe)
Use a window function to keep one row per (block_number, transaction_hash, log_index):

```sql
WITH ranked AS (
  SELECT
    block_number,
    block_hash,
    transaction_hash,
    log_index,
    address,
    data,
    topics,
    row_number() OVER (
      PARTITION BY block_number, transaction_hash, log_index
      ORDER BY block_hash DESC
    ) AS rn
  FROM incentiv_testnet_raw_logs_rpc
)
SELECT
  block_number,
  block_hash,
  transaction_hash,
  log_index,
  address,
  data,
  topics
FROM ranked
WHERE rn = 1;
```

Tip: If you want stronger ordering, add an ingested_at timestamp in uploads and ORDER BY ingested_at DESC instead.

## Repo hygiene
- .env is ignored (do not commit secrets)
- last_block.txt and cron_sync.log are ignored (ephemeral state/logs)

## Legacy (Goldsky/Neon)
The previous Goldsky/Neon pipeline is retired for this repo. Legacy examples remain under examples/ and historical files like incentiv-testnet-raw-logs.yaml and sync_neon_to_dune.py are kept for reference only.