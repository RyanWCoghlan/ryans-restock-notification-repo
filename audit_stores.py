"""
Audits a list of store URLs to determine:
  1. Is the site reachable?
  2. Is it running Shopify (has a working /products.json feed)?
  3. Does the homepage mention any of our target TCGs?

Outputs a CSV report: domain,url,status,is_shopify,mentions_tcgs,notes

Run with:  python audit_stores.py stores.csv
"""

import sys
import csv
import asyncio
import httpx

TARGET_TCG_KEYWORDS = ["pokemon", "pokémon", "one piece", "magic the gathering", "mtg", "riftbound"]

CONCURRENCY = 20
TIMEOUT = 12.0


async def check_site(client: httpx.AsyncClient, domain: str, url: str) -> dict:
    result = {
        "domain": domain,
        "url": url,
        "status": "unreachable",
        "is_shopify": False,
        "mentions_tcgs": False,
        "notes": "",
    }
    try:
        resp = await client.get(url, timeout=TIMEOUT, follow_redirects=True)
        result["status"] = f"http_{resp.status_code}"
        if resp.status_code >= 400:
            return result

        body = resp.text.lower()
        result["mentions_tcgs"] = any(kw in body for kw in TARGET_TCG_KEYWORDS)

        # Shopify detection: look for common signatures
        is_shopify = (
            "cdn.shopify.com" in body
            or "shopify.com" in body
            or 'name="shopify-checkout-api-token"' in body
            or "Shopify.theme" in resp.text
        )
        result["is_shopify"] = is_shopify

        # Confirm by trying the products.json feed (only if it looked like Shopify,
        # to avoid wasting requests on obviously non-Shopify sites)
        if is_shopify:
            try:
                pj_url = url.rstrip("/") + "/products.json?limit=1"
                pj_resp = await client.get(pj_url, timeout=TIMEOUT, follow_redirects=True)
                if pj_resp.status_code == 200 and '"products"' in pj_resp.text:
                    result["notes"] = "products.json confirmed working"
                else:
                    result["notes"] = "shopify signature found but products.json check failed"
            except Exception as e:
                result["notes"] = f"products.json check error: {e}"

    except httpx.TimeoutException:
        result["status"] = "timeout"
    except Exception as e:
        result["status"] = f"error: {type(e).__name__}"

    return result


async def main(input_csv: str, output_csv: str):
    with open(input_csv, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = [(row["domain"], row["url"]) for row in reader]

    sem = asyncio.Semaphore(CONCURRENCY)
    results = []

    async with httpx.AsyncClient(
        headers={"User-Agent": "Mozilla/5.0 (compatible; TCGStoreAudit/1.0)"}
    ) as client:

        async def bounded(domain, url):
            async with sem:
                return await check_site(client, domain, url)

        tasks = [bounded(domain, url) for domain, url in rows]
        for i, coro in enumerate(asyncio.as_completed(tasks), 1):
            res = await coro
            results.append(res)
            print(f"[{i}/{len(rows)}] {res['domain']}: {res['status']} shopify={res['is_shopify']} tcg_mentions={res['mentions_tcgs']}")

    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["domain", "url", "status", "is_shopify", "mentions_tcgs", "notes"])
        writer.writeheader()
        for r in sorted(results, key=lambda r: r["domain"]):
            writer.writerow(r)

    print(f"\nDone. Wrote {output_csv}")
    reachable = sum(1 for r in results if r["status"].startswith("http_2"))
    shopify = sum(1 for r in results if r["is_shopify"])
    relevant = sum(1 for r in results if r["mentions_tcgs"])
    print(f"Reachable: {reachable}/{len(results)}")
    print(f"Shopify: {shopify}/{len(results)}")
    print(f"Mentions target TCGs: {relevant}/{len(results)}")


if __name__ == "__main__":
    input_csv = sys.argv[1] if len(sys.argv) > 1 else "stores.csv"
    output_csv = sys.argv[2] if len(sys.argv) > 2 else "audit_report.csv"
    asyncio.run(main(input_csv, output_csv))
