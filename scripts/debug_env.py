from dotenv import load_dotenv
import os

load_dotenv()
print(f"RPC_URL: {os.environ.get('INCENTIV_BLOCKSCOUT_RPC_URL')}")
