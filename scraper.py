import aiohttp
import asyncio
import json
from typing import List

API_BASE_URL = "http://172.16.26.12:3029/api/displayticket/staff-report"
BEARER_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IjY4ZWE3NTkzZTBlMGNiNWQ2ZWY5NTBjZCIsImVtYWlsIjoiYWRkaXNtZXNvYjJAZXQuY29tIiwidXNlcm5hbWUiOiJhZGRpcy0wMi1zeXMtc3lzIiwicm9sZSI6ImFkbWluIiwiaWF0IjoxNzY3MDE1NDc4LCJleHAiOjE3NjcwMTkwNzh9.vW1nlmrdmYBDthzQpnF2_JvIBPCDNE3cg6a33BEgWes"  # shorten for safety

HEADERS = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {BEARER_TOKEN}",
}

STAFF_USERNAMES = [
    "AMINFO-01-KIDIST-K",
    "AMINFO-02-Selam-T",
    "AMINFO-03-Bizuye-B",
    "AMINFO-04-Birhanu-F",
    "AMINFO-05-Birhanu-G",
]


def headers() -> dict:
    return {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {BEARER_TOKEN}",
    }


async def fetch_staff_report(
    session: aiohttp.ClientSession, username: str
) -> dict | None:
    url = f"{API_BASE_URL}/{username}"

    async with session.get(url, headers=headers()) as resp:
        if resp.status == 401:
            raise RuntimeError("Token expired or invalid (401)")

        if resp.status != 200:
            print(f" {username}: HTTP {resp.status}")
            return None

        return await resp.json()


async def scrape_all_staff(usernames: List[str]) -> List[dict]:
    timeout = aiohttp.ClientTimeout(total=15)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        tasks = [fetch_staff_report(session, username) for username in usernames]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        clean = []
        for r in results:
            if isinstance(r, Exception):
                print(f"❌ {r}")
            elif r:
                clean.append(r)

        return clean


async def main():
    data = await scrape_all_staff(STAFF_USERNAMES)

    print(json.dumps(data, indent=2, ensure_ascii=False))

    with open("staff_reports.json", "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    print("✅ Saved to staff_reports.json")


if __name__ == "__main__":
    asyncio.run(main())
