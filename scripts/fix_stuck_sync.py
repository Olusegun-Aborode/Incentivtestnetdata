#!/usr/bin/env python3
"""
FIX #1: Skip stuck block 2695207 and resume sync.

The cron sync has been stuck for a week because block 2695207 returns a
500 error from the Incentiv RPC.  This script:

  1. Advances state.json past the bad block range
  2. Patches pipeline.py so future 500 errors skip the batch instead of aborting
  3. Verifies the fix

Run: python3 scripts/fix_stuck_sync.py
"""
import json
from pathlib import Path

PROJECT = Path(__file__).resolve().parent.parent
STATE_FILE = PROJECT / "state.json"
PIPELINE_FILE = PROJECT / "src" / "pipeline.py"

# ── Step 1: Advance state past the stuck block ─────────────────
print("=" * 60)
print("FIX #1: Unstick the sync (block 2695207 → skip ahead)")
print("=" * 60)

state = json.loads(STATE_FILE.read_text())
old_block = state.get("last_all_activity_block", 0)
print(f"\nCurrent last_all_activity_block: {old_block}")

# Skip ahead past the problematic block range.
# We advance by a small batch (50 blocks) so we don't miss too much.
# If the next block also 500s, the patched pipeline will skip it.
SKIP_TO = old_block + 50  # 2695206 + 50 = 2695256
state["last_all_activity_block"] = SKIP_TO
STATE_FILE.write_text(json.dumps(state))
print(f"Advanced to: {SKIP_TO}")

# ── Step 2: Patch pipeline.py to skip-on-error instead of abort ─
print("\nPatching pipeline.py to skip bad batches instead of aborting...")

code = PIPELINE_FILE.read_text()

OLD_ABORT = '''\
            if not args.dry_run:
                print("    Aborting. State saved — resume with same command.")
                import sys
                sys.exit(1)'''

NEW_SKIP = '''\
            if not args.dry_run:
                print(f"    ⚠️  Skipping bad batch {start}-{end} and continuing...")
                # Advance state past this batch so we don't loop forever
                state["last_all_activity_block"] = end
                save_state(state_path, state)
                if args.neon:
                    try:
                        neon.update_extraction_state(
                            "all_activity", end, status="running"
                        )
                    except Exception:
                        pass
                continue'''

if OLD_ABORT in code:
    code = code.replace(OLD_ABORT, NEW_SKIP)
    PIPELINE_FILE.write_text(code)
    print("  ✓ Patched: pipeline now skips bad batches instead of sys.exit(1)")
else:
    # Check if already patched
    if "Skipping bad batch" in code:
        print("  ✓ Already patched (skip-on-error is present)")
    else:
        print("  ⚠  Could not find the abort pattern to patch. Manual edit needed.")
        print("     Look for 'sys.exit(1)' in run_all_activity_etl() and replace with 'continue'")

# ── Step 3: Verify ──────────────────────────────────────────────
print("\nVerification:")
new_state = json.loads(STATE_FILE.read_text())
print(f"  state.json last_all_activity_block: {new_state['last_all_activity_block']}")

code2 = PIPELINE_FILE.read_text()
if "Skipping bad batch" in code2:
    print("  pipeline.py: skip-on-error ✓")
else:
    print("  pipeline.py: ⚠ NOT patched — check manually")

print("\n✅ Done. The next cron run will start at block", SKIP_TO + 1,
      "and skip any 500 errors.")
print("   To catch up faster, run manually:")
print(f"   python3 -m src.pipeline --all-activity --neon --batch-size 100")
