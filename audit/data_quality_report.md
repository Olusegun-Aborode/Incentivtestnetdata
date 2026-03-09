# Data Quality Report

Generated at: 2026-03-05T17:36:19.318227+00:00

## ✅ PASSED CHECKS
- [x] Blocks: No NULLs in ['hash', 'timestamp', 'parent_hash']
- [x] Transactions: No NULLs in ['hash', 'from_address', 'block_number']
- [x] Logs: No NULLs in ['contract_address', 'transaction_hash', 'block_number']
- [x] Duplicate checks skipped (Relational Integrity assumed via Primary Keys)

## ⚠️ WARNINGS
No warnings.

## ❌ FAILED CHECKS
- [ ] Count mismatch: Expected 2602547 (from 10 to 2602556), Found 981393. Missing 1621154 blocks.

## SUMMARY
- Current Chain Height: 2676582
- Database Max Block: 2602556
- Lag: 74026 blocks
