
import csv
import requests
import io

DUNE_API_KEY = "3nKpTZrrziBToMPOY7z2nybU8c6L3Our"
DUNE_API_BASE = "https://api.dune.com/api/v1"

def debug_upload():
    # Create a tiny CSV in memory
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['col1', 'col2'])
    writer.writerow(['val1', 'val2'])
    csv_content = output.getvalue()
    
    print(f"CSV Content:\n{csv_content}")

    url = f"{DUNE_API_BASE}/table/upload/csv"
    
    files = {"data": ("test_table.csv", csv_content, "text/csv")}
    form_data = {
        "table_name": "incentiv_debug_test",
        "description": "Debug upload",
        "is_private": "false"
    }
    
    headers = {
        "X-Dune-Api-Key": DUNE_API_KEY,
        # "Accept-Encoding": "identity" # Try without first, then with
    }

    print("Sending request...")
    resp = requests.post(url, files=files, data=form_data, headers=headers)
    
    print(f"Status: {resp.status_code}")
    print(f"Response: {resp.text}")

if __name__ == "__main__":
    debug_upload()
