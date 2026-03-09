#!/bin/bash
# Historical backfill script - runs all chunks sequentially
# Usage: ./backfill_all_chunks.sh

set -e

CHAIN="incentiv"
CHUNK_SIZE=100000
LOG_FILE="/tmp/backfill_progress.log"

# Get current chain head
HEAD=$(python3 -c "
import os, requests
from dotenv import load_dotenv
load_dotenv()
RPC_URL = os.getenv('INCENTIV_BLOCKSCOUT_RPC_URL')
resp = requests.post(RPC_URL, json={'id':1,'jsonrpc':'2.0','method':'eth_blockNumber','params':[]}).json()
print(int(resp['result'], 16))
")

echo "Chain head: $HEAD"
echo "Starting backfill at $(date)" | tee -a $LOG_FILE

# Calculate chunks
START=1
CHUNK_NUM=1

while [ $START -le $HEAD ]; do
    END=$((START + CHUNK_SIZE - 1))
    if [ $END -gt $HEAD ]; then
        END=$HEAD
    fi
    
    echo "" | tee -a $LOG_FILE
    echo "========================================" | tee -a $LOG_FILE
    echo "Chunk $CHUNK_NUM: Blocks $START - $END" | tee -a $LOG_FILE
    echo "Started at: $(date)" | tee -a $LOG_FILE
    echo "========================================" | tee -a $LOG_FILE
    
    python3 -m src.pipeline \
        --chain $CHAIN \
        --logs \
        --decoded-logs \
        --from-block $START \
        --to-block $END 2>&1 | tee -a $LOG_FILE | grep -E "(Found|Uploading|Successfully|✅|❌|⚠️)"
    
    EXIT_CODE=$?
    if [ $EXIT_CODE -ne 0 ]; then
        echo "❌ Chunk $CHUNK_NUM failed with exit code $EXIT_CODE" | tee -a $LOG_FILE
        exit $EXIT_CODE
    fi
    
    echo "✅ Chunk $CHUNK_NUM completed at $(date)" | tee -a $LOG_FILE
    
    START=$((END + 1))
    CHUNK_NUM=$((CHUNK_NUM + 1))
done

echo "" | tee -a $LOG_FILE
echo "🎉 Backfill completed at $(date)" | tee -a $LOG_FILE
echo "Total chunks processed: $((CHUNK_NUM - 1))" | tee -a $LOG_FILE
