
import requests
import json

BLOCKSCOUT_URL = "https://explorer.incentiv.io"
ADDRESS = "0x16e43840d8D79896A389a3De85aB0B0210C05685"

def get_token_info():
    url = f"{BLOCKSCOUT_URL}/api/v2/tokens/{ADDRESS}"
    print(f"Fetching info for {ADDRESS}...")
    try:
        resp = requests.get(url)
        print(f"Status: {resp.status_code}")
        if resp.status_code == 200:
            print(json.dumps(resp.json(), indent=2))
        else:
            print(resp.text)
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    get_token_info()
