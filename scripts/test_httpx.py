import asyncio
import httpx

async def test():
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Accept": "application/json"
    }
    async with httpx.AsyncClient(http2=True) as client:
        r = await client.get('https://explorer.incentiv.io/api/v2/blocks/318411', headers=headers)
        print(f"Status: {r.status_code}")
        print(f"Body: {r.text[:200]}")

asyncio.run(test())
