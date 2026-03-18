#!/bin/bash
# ╔══════════════════════════════════════════════════════════════╗
# ║  RUN ALL FIXES — Execute in order                           ║
# ║                                                             ║
# ║  1. Fix stuck sync (skip block 2695207)                     ║
# ║  2. Drop FK constraint + decode all events                  ║
# ║  3. Regenerate dashboard                                    ║
# ╚══════════════════════════════════════════════════════════════╝
#
# Usage:
#   cd ~/Incentiveanti/Incentivtestnetdata
#   chmod +x scripts/run_all_fixes.sh
#   ./scripts/run_all_fixes.sh
#
set -e

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_DIR"

echo "Working directory: $PROJECT_DIR"
echo ""

# ── FIX 1: Unstick the sync ────────────────────────────────────
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  FIX 1: Unstick the sync"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
python3 scripts/fix_stuck_sync.py
echo ""

# ── FIX 2: Drop FK + Decode all events ─────────────────────────
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  FIX 2: Drop FK constraint + Full decode"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
python3 scripts/drop_fk_and_decode.py
echo ""

# ── FIX 3: Regenerate dashboard ────────────────────────────────
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  FIX 3: Regenerate dashboard"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
python3 scripts/generate_dashboard.py
echo ""

# ── Summary ────────────────────────────────────────────────────
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  ALL FIXES COMPLETE"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "Dashboard should be at: dashboard/incentiv_dashboard.html"
echo "Open it in your browser to verify."
echo ""
echo "The cron sync will now skip bad blocks automatically."
echo "To catch up on the ~160K block gap, run:"
echo "  python3 -m src.pipeline --all-activity --neon --batch-size 100"
