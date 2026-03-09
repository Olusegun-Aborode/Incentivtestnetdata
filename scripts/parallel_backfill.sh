#!/bin/bash
# Parallel backfill script
# Usage: ./parallel_backfill.sh

set -e

# Define ranges
# Current head is approx 2,291,096
# Last synced is 1,970,204
# Gap is ~320k blocks

# 4 Parallel Jobs
# Range 1: 1,970,205 - 2,050,000 (79,795 blocks)
# Range 2: 2,050,001 - 2,130,000 (80,000 blocks)
# Range 3: 2,130,001 - 2,210,000 (80,000 blocks)
# Range 4: 2,210,001 - 2,291,096+ (81,096+ blocks) - will likely hit head

LOG_DIR="logs/backfill"
mkdir -p $LOG_DIR

echo "🚀 Starting Parallel Backfill..."

# Function to run a backfill chunk in background
run_chunk() {
    local job_id=$1
    local start_block=$2
    local end_block=$3
    local log_file="$LOG_DIR/job_${job_id}.log"
    
    echo "  Job $job_id: $start_block - $end_block (Logging to $log_file)"
    
    # Using src.pipeline directly
    # Note: --batch-size 2000 for speed
    nohup python3 -u -m src.pipeline \
        --chain incentiv \
        --logs \
        --decoded-logs \
        --skip-dune \
        --batch-size 2000 \
        --from-block $start_block \
        --to-block $end_block > $log_file 2>&1 &
        
    echo $! > "$LOG_DIR/job_${job_id}.pid"
}

run_chunk 1 1970205 2050000
run_chunk 2 2050001 2130000
run_chunk 3 2130001 2210000
run_chunk 4 2210001 2300000 # Go slightly past current head to be safe

echo ""
echo "✅ All jobs started."
echo "Monitor progress with: tail -f logs/backfill/*.log"
