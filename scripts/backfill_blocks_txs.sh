#!/bin/bash
# Backfill blocks and transactions metadata
# Strategy: Process in chunks, using existing block numbers as guide

set -e

MIN_BLOCK=938365
MAX_BLOCK=2293167
CHUNK_SIZE=10000

LOG_DIR="logs/backfill_blocks_txs"
mkdir -p $LOG_DIR

echo "🚀 Starting Block & Transaction Metadata Backfill..."
echo "Range: $MIN_BLOCK to $MAX_BLOCK"
echo "Chunk size: $CHUNK_SIZE"
echo ""

for start in $(seq $MIN_BLOCK $CHUNK_SIZE $MAX_BLOCK); do
    end=$((start + CHUNK_SIZE - 1))
    if [ $end -gt $MAX_BLOCK ]; then
        end=$MAX_BLOCK
    fi
    
    echo "📦 Processing blocks $start to $end..."
    
    # Extract blocks and transactions
    python3 -m src.pipeline \
        --chain incentiv \
        --blocks \
        --transactions \
        --skip-dune \
        --from-block $start \
        --to-block $end \
        --batch-size 500 2>&1 | tee -a "$LOG_DIR/extract_${start}_${end}.log"
    
    # Upload to Supabase
    echo "📤 Uploading to Supabase..."
    python3 scripts/upload_blocks_txs.py 2>&1 | tee -a "$LOG_DIR/upload_${start}_${end}.log"
    
    echo "✅ Completed chunk $start-$end"
    echo "----------------------------------------"
done

echo ""
echo "🎉 Backfill Complete!"
echo "Run 'python3 scripts/audit_data.py' to verify."
