#!/bin/bash
# Continuous upload script for parallel backfill
# Usage: ./continuous_upload.sh

set -e

echo "🚀 Starting Continuous Upload Loop..."

while true; do
  echo "📤 Running upload_recent.py..."
  python3 scripts/upload_recent.py
  
  EXIT_CODE=$?
  if [ $EXIT_CODE -ne 0 ]; then
    echo "❌ Upload script failed with exit code $EXIT_CODE"
    sleep 5
  else
    echo "✅ Upload cycle complete."
    sleep 2  # Short sleep to be aggressive
  fi
  
  echo "----------------------------------------"
done
