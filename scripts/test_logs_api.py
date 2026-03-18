import asyncio
import httpx

async def test():
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/122.0.0.0 Safari/537.36'
    }
    url = "https://explorer.incentiv.io/api?module=logs&action=getLogs&fromBlock=318411&toBlock=latest"
    async with httpx.AsyncClient(http2=False, headers=headers) as c:
        try:
            r = await c.get(url)
            print(f"Status: {r.status_code}")
            data = r.json()
            print(f"Response: {str(data)[:300]}")
        except Exception as e:
            print(f"Error: {e}")

asyncio.run(test())
