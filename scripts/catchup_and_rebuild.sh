#!/bin/bash
# ╔══════════════════════════════════════════════════════════════╗
# ║  PHASE 2: Close the block gap + backfill transactions       ║
# ║           + redecode + regenerate dashboard                  ║
# ╚══════════════════════════════════════════════════════════════╝
#
# Run:
#   cd ~/Incentiveanti/Incentivtestnetdata
#   chmod +x scripts/catchup_and_rebuild.sh
#   ./scripts/catchup_and_rebuild.sh
#
set -e
cd "$(dirname "$0")/.."

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  PHASE 2: Close block gap + backfill transactions"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Step 1: Catch up on the ~316K block gap
echo ""
echo "Step 1: Catching up to chain tip (blocks + txs + logs)..."
echo "  This uses the patched pipeline that skips bad blocks."
echo ""
python3 -u -m src.pipeline --all-activity --neon --batch-size 50

# Step 2: Backfill transactions for older blocks via Blockscout REST API
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Step 2: Fast transaction backfill (Blockscout REST API)..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
python3 -u scripts/fast_tx_backfill.py

# Step 3: Re-decode all events (now with more transactions available)
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Step 3: Re-decode all events..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
python3 -u scripts/drop_fk_and_decode.py

# Step 4: Regenerate dashboard
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Step 4: Regenerate dashboard..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
python3 -u scripts/generate_dashboard.py

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  ALL DONE"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "Dashboard: dashboard/incentiv_dashboard.html"
