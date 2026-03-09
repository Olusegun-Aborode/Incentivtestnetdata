# Incentiv Data Update Guide

## Quick Start

### Option 1: Run Everything Automatically (Recommended)
```bash
cd /Users/olusegunaborode/Incentiveanti/Incentivtestnetdata
./scripts/update_all_data.sh
```

This will:
1. Complete backfill from block 1,738,000 to current
2. Verify data integrity
3. Start continuous sync

### Option 2: Run Steps Manually

#### Step 1: Complete Backfill
```bash
cd /Users/olusegunaborode/Incentiveanti/Incentivtestnetdata
./scripts/complete_backfill.sh
```

**What it does:**
- Processes blocks 1,738,000 → current (~223,463 blocks)
- Uses 10,000 block chunks for better progress tracking
- Updates `state_backfill.json` after each chunk
- Logs to `backfill_completion.log`

**Estimated time:** 2-4 hours (depending on network speed)

#### Step 2: Start Continuous Sync
```bash
cd /Users/olusegunaborode/Incentiveanti/Incentivtestnetdata
python3 scripts/run_continuous.py
```

**What it does:**
- Runs every 60 seconds
- Processes new blocks automatically
- Updates `state.json` with latest block
- Logs to `continuous_sync.log`
- Stops after 5 consecutive errors

**To run in background:**
```bash
nohup python3 scripts/run_continuous.py > /dev/null 2>&1 &
```

**To stop background process:**
```bash
# Find the process
ps aux | grep run_continuous.py

# Kill it (replace PID with actual process ID)
kill <PID>
```

## Monitoring Progress

### Check Backfill Progress
```bash
# Watch live progress
tail -f backfill_completion.log

# Check current state
cat state_backfill.json
```

### Check Continuous Sync Status
```bash
# Watch live sync
tail -f continuous_sync.log

# Check current state
cat state.json

# Run audit to see overall status
python3 scripts/audit_data_state.py
```

## Automation Setup (macOS)

### Using launchd (Recommended for macOS)

1. **Create launchd plist file:**
```bash
cat > ~/Library/LaunchAgents/com.incentiv.continuous-sync.plist << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.incentiv.continuous-sync</string>
    <key>ProgramArguments</key>
    <array>
        <string>/usr/local/bin/python3</string>
        <string>/Users/olusegunaborode/Incentiveanti/Incentivtestnetdata/scripts/run_continuous.py</string>
    </array>
    <key>WorkingDirectory</key>
    <string>/Users/olusegunaborode/Incentiveanti/Incentivtestnetdata</string>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>/Users/olusegunaborode/Incentiveanti/Incentivtestnetdata/continuous_sync.log</string>
    <key>StandardErrorPath</key>
    <string>/Users/olusegunaborode/Incentiveanti/Incentivtestnetdata/continuous_sync_error.log</string>
</dict>
</plist>
EOF
```

2. **Load the service:**
```bash
launchctl load ~/Library/LaunchAgents/com.incentiv.continuous-sync.plist
```

3. **Manage the service:**
```bash
# Start
launchctl start com.incentiv.continuous-sync

# Stop
launchctl stop com.incentiv.continuous-sync

# Unload (disable)
launchctl unload ~/Library/LaunchAgents/com.incentiv.continuous-sync.plist

# Check status
launchctl list | grep incentiv
```

## Troubleshooting

### Backfill Fails
```bash
# Check the log
tail -100 backfill_completion.log

# Check last successful block
cat state_backfill.json

# Resume from last successful block (edit START_BLOCK in script)
nano scripts/complete_backfill.sh
```

### Continuous Sync Stops
```bash
# Check error log
tail -100 continuous_sync.log

# Check for rate limiting
grep "429" continuous_sync.log

# Restart manually
python3 scripts/run_continuous.py
```

### Out of Sync
```bash
# Run audit to check gap
python3 scripts/audit_data_state.py

# If gap is large, run continuous sync manually
python3 scripts/run_continuous.py
```

## File Locations

| File | Purpose |
|------|---------|
| `scripts/complete_backfill.sh` | Complete remaining backfill |
| `scripts/run_continuous.py` | Continuous sync script |
| `scripts/update_all_data.sh` | Master orchestration script |
| `scripts/audit_data_state.py` | Check current data state |
| `state.json` | Main ETL state (last processed block) |
| `state_backfill.json` | Backfill state |
| `backfill_completion.log` | Backfill progress log |
| `continuous_sync.log` | Continuous sync log |

## Next Steps After Data is Current

Once backfill is complete and continuous sync is running:

1. ✅ Verify data is up-to-date: `python3 scripts/audit_data_state.py`
2. ✅ Create fresh CSV exports (if needed)
3. ✅ Backup decoded logs
4. ✅ Proceed with PostgreSQL + Streamlit dashboard implementation
