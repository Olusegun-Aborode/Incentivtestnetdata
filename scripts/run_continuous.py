import subprocess
import time
import sys
from datetime import datetime

def main():
    print("🚀 Starting Continuous Sync for Incentiv ETL")
    print("Press Ctrl+C to stop.")
    
    # Configuration
    SLEEP_SECONDS = 60  # Run every minute
    
    while True:
        try:
            start_time = datetime.now()
            print(f"\n[sync] Starting run at {start_time.strftime('%Y-%m-%d %H:%M:%S')}...")
            
            # Run the pipeline module
            # -u for unbuffered output to see logs immediately
            result = subprocess.run(
                [sys.executable, "-u", "-m", "src.pipeline"],
                capture_output=True,
                text=True
            )
            
            if result.returncode == 0:
                print("[sync] ✅ Run completed successfully.")
                # If there was output (e.g. from dry run or future logs), print it
                if result.stdout.strip():
                    print(f"[output] {result.stdout.strip()}")
            else:
                print(f"[sync] ❌ Run failed with return code {result.returncode}")
                if result.stderr:
                    print(f"[error] {result.stderr}")
            
            # Sleep
            print(f"[sync] Sleeping for {SLEEP_SECONDS} seconds...")
            time.sleep(SLEEP_SECONDS)
            
        except KeyboardInterrupt:
            print("\n[sync] 🛑 Stopping continuous sync.")
            break
        except Exception as e:
            print(f"\n[sync] ⚠️ Unexpected error: {e}")
            time.sleep(10) # Short sleep on error to avoid rapid looping

if __name__ == "__main__":
    main()
