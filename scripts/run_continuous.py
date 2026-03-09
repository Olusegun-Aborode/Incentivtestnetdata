#!/usr/bin/env python3
"""
Continuous ETL sync for Incentiv blockchain.
Runs continuously to keep data up-to-date with the latest blocks.
"""
import subprocess
import time
import sys
import json
from datetime import datetime
from pathlib import Path

STATE_FILE = Path(__file__).parent.parent / "state.json"
LOG_FILE = Path(__file__).parent.parent / "continuous_sync.log"

def log(message):
    """Log message to both console and file."""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_msg = f"[{timestamp}] {message}"
    print(log_msg)
    with open(LOG_FILE, 'a') as f:
        f.write(log_msg + '\n')

def get_last_processed_block():
    """Get the last processed block from state file."""
    if STATE_FILE.exists():
        with open(STATE_FILE) as f:
            state = json.load(f)
            return state.get('last_block', 0)
    return 0

def main():
    log("🚀 Starting Continuous Sync for Incentiv ETL")
    log("This will run indefinitely. Press Ctrl+C to stop.")
    log("=" * 80)
    
    # Configuration
    BATCH_SIZE = 2000  # Increased for faster catch-up
    SLEEP_TIME = 60    # Sleep time between runs (seconds)
    ERROR_THRESHOLD = 10 # Increase tolerance
    consecutive_errors = 0
    # Configuration
    BATCH_SIZE = 2000  # Increased for faster catch-up
    SLEEP_SECONDS = 30 # Reduced sleep for faster catch-up
    ERROR_THRESHOLD = 10 
    consecutive_errors = 0
    MAX_CONSECUTIVE_ERRORS = 5
    
    while True:
        try:
            start_time = datetime.now()
            last_block = get_last_processed_block()
            
            log(f"Starting sync run (last processed block: {last_block:,})")
            
            # Run the pipeline module with batch size
            result = subprocess.run(
                [
                    sys.executable, "-u", "-m", "src.pipeline",
                    "--chain", "incentiv",
                    "--logs",
                    "--decoded-logs",
                    "--skip-dune",
                    "--batch-size", str(BATCH_SIZE)
                ],
                capture_output=True,
                text=True
            )
            
            if result.returncode == 0:
                new_block = get_last_processed_block()
                blocks_processed = new_block - last_block
                
                if blocks_processed > 0:
                    log(f"✅ Processed {blocks_processed} new blocks (now at block {new_block:,})")
                    
                    # Trigger upload to Neon DB
                    log("Uploading recent data to Neon DB...")
                    upload_res = subprocess.run([sys.executable, "scripts/upload_recent.py"], capture_output=True, text=True)
                    if upload_res.returncode == 0:
                        log(f"Upload success: {upload_res.stdout.strip()}")
                    else:
                        log(f"Upload failed: {upload_res.stderr}")

                else:
                    log(f"✅ No new blocks (still at block {new_block:,})")
                
                consecutive_errors = 0  # Reset error counter on success
            else:
                consecutive_errors += 1
                log(f"❌ Run failed with return code {result.returncode}")
                log(f"Error output: {result.stderr[:500]}")  # Log first 500 chars of error
                
                if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                    log(f"⚠️ {MAX_CONSECUTIVE_ERRORS} consecutive errors. Stopping continuous sync.")
                    log("Please check logs and fix issues before restarting.")
                    break
            
            # Sleep before next run
            elapsed = (datetime.now() - start_time).total_seconds()
            log(f"Run completed in {elapsed:.1f}s. Sleeping for {SLEEP_SECONDS}s...")
            log("-" * 80)
            time.sleep(SLEEP_SECONDS)
            
        except KeyboardInterrupt:
            log("")
            log("🛑 Stopping continuous sync (user interrupted)")
            break
        except Exception as e:
            consecutive_errors += 1
            log(f"⚠️ Unexpected error: {e}")
            
            if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                log(f"⚠️ {MAX_CONSECUTIVE_ERRORS} consecutive errors. Stopping.")
                break
            
            log("Sleeping for 30s before retry...")
            time.sleep(30)
    
    log("=" * 80)
    log("Continuous sync stopped.")

if __name__ == "__main__":
    main()
