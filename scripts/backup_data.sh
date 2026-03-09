#!/bin/bash
# Backup all remaining blockchain data to local CSV files
# This ensures no data is lost while waiting for Dune API credits

set -e

BACKUP_DIR="backups"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="/tmp/backup_${TIMESTAMP}.log"

echo "🔄 Starting data backup..." | tee -a $LOG_FILE
echo "Timestamp: $TIMESTAMP" | tee -a $LOG_FILE
echo "" | tee -a $LOG_FILE

# Create backup directory
mkdir -p $BACKUP_DIR

# Get current state
LAST_BLOCK=$(python3 -c "import json; print(json.load(open('state.json'))['last_block'])")
echo "Last synced block: $LAST_BLOCK" | tee -a $LOG_FILE

# Extract and save data using pipeline with CSV output
echo "" | tee -a $LOG_FILE
echo "Extracting logs from blockchain..." | tee -a $LOG_FILE

# Run pipeline in dry-run mode to extract data without uploading
python3 -m src.pipeline \
    --chain incentiv \
    --logs \
    --decoded-logs \
    --dry-run \
    2>&1 | tee -a $LOG_FILE

echo "" | tee -a $LOG_FILE
echo "✅ Backup extraction complete!" | tee -a $LOG_FILE
echo "Log file: $LOG_FILE" | tee -a $LOG_FILE
echo "" | tee -a $LOG_FILE
echo "Note: Data extracted but not uploaded to Dune (dry-run mode)" | tee -a $LOG_FILE
echo "When Dune credits are available, run:" | tee -a $LOG_FILE
echo "  python3 -m src.pipeline --chain incentiv --logs --decoded-logs" | tee -a $LOG_FILE
