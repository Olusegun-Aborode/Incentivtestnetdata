#!/bin/bash
# Monitor the fetch_missing_txs.out log until it completes,
# then automatically run the decoder and dashboard generation.

LOG_FILE="logs/fetch_missing_txs.out"

echo "Monitoring $LOG_FILE for completion..."

tail -f "$LOG_FILE" | while read -r line; do
    if [[ "$line" == *"DONE in"* ]]; then
        echo "Detected completion string: $line"
        echo "Starting redecode_all.py..."
        python3 scripts/redecode_all.py > logs/redecode_all_auto.out 2>&1
        
        echo "Starting generate_dashboard.py..."
        python3 scripts/generate_dashboard.py > logs/generate_dashboard_auto.out 2>&1
        
        echo "All automated steps completed successfully."
        
        # Kill the tail process to cleanly exit the monitor script
        pkill -P $$ tail
        break
    fi
done
