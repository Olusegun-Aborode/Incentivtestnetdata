#!/bin/bash
# Master script to complete backfill and start continuous sync
# This orchestrates the entire data update process

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "========================================="
echo "INCENTIV DATA UPDATE - MASTER SCRIPT"
echo "========================================="
echo ""
echo "This script will:"
echo "  1. Complete backfill (blocks 1,738,000 → current)"
echo "  2. Start continuous sync to stay current"
echo ""
echo "Project directory: $PROJECT_DIR"
echo ""

# Step 1: Complete backfill
echo "========================================="
echo "STEP 1: COMPLETING BACKFILL"
echo "========================================="
echo ""

if [ -f "$SCRIPT_DIR/complete_backfill.sh" ]; then
    echo "Starting backfill completion..."
    bash "$SCRIPT_DIR/complete_backfill.sh"
    
    if [ $? -eq 0 ]; then
        echo ""
        echo "✅ Backfill completed successfully!"
        echo ""
    else
        echo ""
        echo "❌ Backfill failed. Please check logs."
        exit 1
    fi
else
    echo "❌ complete_backfill.sh not found!"
    exit 1
fi

# Step 2: Verify data
echo "========================================="
echo "STEP 2: VERIFYING DATA"
echo "========================================="
echo ""

cd "$PROJECT_DIR"
python3 scripts/audit_data_state.py

echo ""
echo "Press Enter to continue to continuous sync, or Ctrl+C to stop..."
read

# Step 3: Start continuous sync
echo ""
echo "========================================="
echo "STEP 3: STARTING CONTINUOUS SYNC"
echo "========================================="
echo ""
echo "Starting continuous sync..."
echo "This will run indefinitely. Press Ctrl+C to stop."
echo ""
echo "Logs will be written to: continuous_sync.log"
echo ""

sleep 3

python3 scripts/run_continuous.py

echo ""
echo "========================================="
echo "DATA UPDATE COMPLETE"
echo "========================================="
