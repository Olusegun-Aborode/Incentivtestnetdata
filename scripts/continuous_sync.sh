#!/bin/bash
# Continuous sync script - runs the pipeline to sync new blocks
# Usage: ./continuous_sync.sh

set -e

CHAIN="incentiv"
LOG_FILE="/tmp/continuous_sync.log"

echo "Starting continuous sync at $(date)" | tee -a $LOG_FILE

# Run the pipeline from current state
python3 -m src.pipeline \
    --chain $CHAIN \
    --logs \
    --decoded-logs 2>&1 | tee -a $LOG_FILE

echo "Sync completed at $(date)" | tee -a $LOG_FILE
