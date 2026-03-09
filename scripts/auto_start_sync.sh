#!/bin/bash
# Auto-start continuous sync after backfill completes
# This script waits for backfill to finish, then starts continuous sync

set -e

BACKFILL_PID=$1
PROJECT_DIR="/Users/olusegunaborode/Incentiveanti/Incentivtestnetdata"

if [ -z "$BACKFILL_PID" ]; then
    echo "Usage: $0 <backfill_pid>"
    echo "Example: $0 12345"
    exit 1
fi

echo "Waiting for backfill process (PID: $BACKFILL_PID) to complete..."
echo "Started at: $(date)"
echo ""

# Wait for backfill process to finish
while kill -0 $BACKFILL_PID 2>/dev/null; do
    sleep 60
    echo "[$(date)] Backfill still running..."
done

echo ""
echo "========================================="
echo "Backfill completed at: $(date)"
echo "========================================="
echo ""

# Verify data state
echo "Verifying data state..."
cd "$PROJECT_DIR"
python3 scripts/audit_data_state.py

echo ""
echo "========================================="
echo "Starting continuous sync..."
echo "========================================="
echo ""

# Start continuous sync
python3 scripts/run_continuous.py
