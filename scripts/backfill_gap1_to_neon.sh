#!/bin/bash
# Gap 1 Backfill: Blocks 0 → 938,364
# Extract all data types and upload to Neon

set -e

echo "🚀 Starting Gap 1 Backfill (0 → 938,364)"
echo "Estimated time: ~10 hours"
echo ""

# Run pipeline with all data types
python3 -m src.pipeline \
  --chain incentiv \
  --logs \
  --decoded-logs \
  --blocks \
  --transactions \
  --skip-dune \
  --from-block 0 \
  --to-block 938364 \
  --batch-size 2000

echo ""
echo "✅ Gap 1 Backfill Complete!"
echo "Next: Run Gap 2 backfill (1,143,795 → 2,290,000)"
