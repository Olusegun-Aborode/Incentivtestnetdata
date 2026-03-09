#!/bin/bash
# Complete remaining backfill from block 1,738,000 to current
# This script continues where the previous backfill left off

set -e

CHAIN="incentiv"
CHUNK_SIZE=10000  # Smaller chunks for better progress tracking
START_BLOCK=1738000  # Where backfill left off
LOG_FILE="backfill_completion.log"

echo "🚀 Starting backfill completion from block $START_BLOCK" | tee -a $LOG_FILE
echo "Started at: $(date)" | tee -a $LOG_FILE
echo "" | tee -a $LOG_FILE

# Get current chain head
echo "Fetching current blockchain height..." | tee -a $LOG_FILE
HEAD=$(python3 -c "
import os, requests
from dotenv import load_dotenv
load_dotenv()
RPC_URL = os.getenv('INCENTIV_BLOCKSCOUT_RPC_URL')
resp = requests.post(RPC_URL, json={'id':1,'jsonrpc':'2.0','method':'eth_blockNumber','params':[]}).json()
print(int(resp['result'], 16))
")

echo "✅ Current blockchain height: $HEAD" | tee -a $LOG_FILE
TOTAL_BLOCKS=$((HEAD - START_BLOCK))
echo "📊 Total blocks to process: $TOTAL_BLOCKS" | tee -a $LOG_FILE
echo "" | tee -a $LOG_FILE

# Calculate chunks
START=$START_BLOCK
CHUNK_NUM=1
TOTAL_CHUNKS=$(( (TOTAL_BLOCKS + CHUNK_SIZE - 1) / CHUNK_SIZE ))

echo "Processing in $TOTAL_CHUNKS chunks of $CHUNK_SIZE blocks each" | tee -a $LOG_FILE
echo "========================================" | tee -a $LOG_FILE
echo "" | tee -a $LOG_FILE

while [ $START -le $HEAD ]; do
    END=$((START + CHUNK_SIZE - 1))
    if [ $END -gt $HEAD ]; then
        END=$HEAD
    fi
    
    PROGRESS=$(awk "BEGIN {printf \"%.2f\", ($CHUNK_NUM / $TOTAL_CHUNKS) * 100}")
    
    echo "========================================" | tee -a $LOG_FILE
    echo "Chunk $CHUNK_NUM/$TOTAL_CHUNKS ($PROGRESS%)" | tee -a $LOG_FILE
    echo "Blocks: $START - $END" | tee -a $LOG_FILE
    echo "Started: $(date)" | tee -a $LOG_FILE
    echo "========================================" | tee -a $LOG_FILE
    
    # Run pipeline with logs and decoded-logs, skipping Dune uploads
    python3 -m src.pipeline \
        --chain $CHAIN \
        --logs \
        --decoded-logs \
        --skip-dune \
        --from-block $START \
        --to-block $END 2>&1 | tee -a $LOG_FILE | grep -E "(Found|Saved|Successfully|✅|❌|⚠️|Error)"
    
    EXIT_CODE=${PIPESTATUS[0]}
    if [ $EXIT_CODE -ne 0 ]; then
        echo "❌ Chunk $CHUNK_NUM failed with exit code $EXIT_CODE" | tee -a $LOG_FILE
        echo "Failed at block range: $START - $END" | tee -a $LOG_FILE
        exit $EXIT_CODE
    fi
    
    # Update backfill state
    echo "{\"last_block\": $END}" > state_backfill.json
    
    echo "✅ Chunk $CHUNK_NUM completed at $(date)" | tee -a $LOG_FILE
    echo "Progress: $PROGRESS% complete" | tee -a $LOG_FILE
    echo "" | tee -a $LOG_FILE
    
    START=$((END + 1))
    CHUNK_NUM=$((CHUNK_NUM + 1))
done

echo "========================================" | tee -a $LOG_FILE
echo "🎉 BACKFILL COMPLETED!" | tee -a $LOG_FILE
echo "Completed at: $(date)" | tee -a $LOG_FILE
echo "Total chunks processed: $((CHUNK_NUM - 1))" | tee -a $LOG_FILE
echo "Final block: $HEAD" | tee -a $LOG_FILE
echo "========================================" | tee -a $LOG_FILE
