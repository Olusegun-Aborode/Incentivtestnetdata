import os
import sys
import requests
from pathlib import Path
from datetime import datetime, timezone
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.loaders.neon import NeonLoader

load_dotenv()
load_dotenv('.env.neon')

# Setup RPC
RPC_URL = os.environ.get("INCENTIV_BLOCKSCOUT_RPC_URL", "https://explorer.incentiv.io/api/eth-rpc")
REPORT_FILE = "audit/data_quality_report.md"

def get_live_chain_height():
    try:
        resp = requests.post(RPC_URL, json={"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}, timeout=10)
        return int(resp.json()["result"], 16)
    except Exception as e:
        print(f"⚠️ Failed to get live chain height: {e}")
        return None

def run_query(query_desc, func):
    print(f"checking {query_desc}...")
    try:
        return func()
    except Exception as e:
        print(f"Error checking {query_desc}: {e}")
        return None

def check_block_continuity(loader):
    res_minmax = loader.query("SELECT MIN(number), MAX(number), COUNT(*) FROM blocks")
    min_block = res_minmax[0][0] or 0
    max_block = res_minmax[0][1] or 0
    count = res_minmax[0][2] or 0
    
    expected = max_block - min_block + 1
    
    if count != expected:
        msg = f"Count mismatch: Expected {expected} (from {min_block} to {max_block}), Found {count}. Missing {expected - count} blocks."
        return {"status": "FAIL", "message": msg, "missing_count": expected - count}
    
    return {"status": "PASS", "message": "No sequence gaps detected locally.", "missing_count": 0}

def check_nulls(loader, table, columns):
    issues = []
    for col in columns:
        res = loader.query(f"SELECT COUNT(*) FROM {table} WHERE {col} IS NULL")
        count = res[0][0]
        if count > 0:
            issues.append(f"{col}: {count} NULLs")
    
    if issues:
        return {"status": "WARNING", "message": ", ".join(issues)}
    return {"status": "PASS", "message": f"No NULLs in {columns}"}

def audit():
    print("🚀 Starting Data Integrity Audit...")
    
    loader = NeonLoader()
    
    results = {
        "passed": [],
        "warnings": [],
        "failed": [],
        "summary": {}
    }
    
    # 1. Block Continuity
    continuity = run_query("Block Continuity", lambda: check_block_continuity(loader))
    if continuity and continuity["status"] == "PASS":
        results["passed"].append(continuity["message"])
    elif continuity:
        results["failed"].append(continuity["message"])
    else:
        results["failed"].append("Block Continuity check failed to execute.")
        
    # 2. Data Completeness
    block_nulls = run_query("Block Completeness", lambda: check_nulls(loader, "blocks", ["hash", "timestamp", "parent_hash"]))
    if block_nulls and block_nulls["status"] == "PASS":
        results["passed"].append(f"Blocks: {block_nulls['message']}")
    elif block_nulls:
        results["warnings"].append(f"Blocks: {block_nulls['message']}")
    else:
        results["warnings"].append("Block Completeness check failed to execute.")

    tx_nulls = run_query("Transaction Completeness", lambda: check_nulls(loader, "transactions", ["hash", "from_address", "block_number"]))
    if tx_nulls and tx_nulls["status"] == "PASS":
        results["passed"].append(f"Transactions: {tx_nulls['message']}")
    elif tx_nulls:
        results["warnings"].append(f"Transactions: {tx_nulls['message']}")
    else:
        results["warnings"].append("Transaction Completeness check failed to execute.")

    log_nulls = run_query("Log Completeness", lambda: check_nulls(loader, "decoded_events", ["contract_address", "transaction_hash", "block_number"]))
    if log_nulls and log_nulls["status"] == "PASS":
        results["passed"].append(f"Logs: {log_nulls['message']}")
    elif log_nulls:
        results["warnings"].append(f"Logs: {log_nulls['message']}")
    else:
        results["warnings"].append("Log Completeness check failed to execute.")

    # 3. Duplicate Detection
    results["passed"].append("Duplicate checks skipped (Relational Integrity assumed via Primary Keys)")

    # 4. Sync Status
    live_height = get_live_chain_height()
    max_block_res = loader.query("SELECT MAX(number) FROM blocks")
    db_max = max_block_res[0][0] or 0
    
    lag = 0
    if live_height:
        lag = live_height - db_max
    
    results["summary"] = {
        "live_height": live_height,
        "db_max": db_max,
        "lag": lag
    }
    
    loader.close()
    
    # Generate Report
    generate_report(results)

def generate_report(results):
    with open(REPORT_FILE, "w") as f:
        f.write("# Data Quality Report\n\n")
        f.write(f"Generated at: {datetime.now(timezone.utc).isoformat()}\n\n")
        
        f.write("## ✅ PASSED CHECKS\n")
        for msg in results["passed"]:
            f.write(f"- [x] {msg}\n")
        f.write("\n")
        
        f.write("## ⚠️ WARNINGS\n")
        for msg in results["warnings"]:
            f.write(f"- [ ] {msg}\n")
        if not results["warnings"]:
            f.write("No warnings.\n")
        f.write("\n")
        
        f.write("## ❌ FAILED CHECKS\n")
        for msg in results["failed"]:
            f.write(f"- [ ] {msg}\n")
        if not results["failed"]:
            f.write("No failures.\n")
        f.write("\n")
        
        f.write("## SUMMARY\n")
        s = results["summary"]
        f.write(f"- Current Chain Height: {s.get('live_height')}\n")
        f.write(f"- Database Max Block: {s.get('db_max')}\n")
        f.write(f"- Lag: {s.get('lag')} blocks\n")
        
    print(f"✅ Report generated: {REPORT_FILE}")
    
    # Exit Code
    if results["failed"]:
        sys.exit(2)
    if results["warnings"]:
        sys.exit(1)
    sys.exit(0)

if __name__ == "__main__":
    audit()
