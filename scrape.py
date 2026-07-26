"""
TCG restock scraper.

Checks each Shopify store's /products.json feed for products matching our
target games + product lines. Only products that are currently purchasable
(in stock, or preorder-enabled) are considered. State (availability per
product per store) persists in state/seen_products.json, which this script
updates and the GitHub Actions workflow commits back to the repo after each
run.

Notification rule: a Discord ping fires only on a genuine restock event -
either a brand new product listing that's already in stock, or an existing
product transitioning from out-of-stock to in-stock. A product that's just
sitting in stock continuously across many 20-minute checks will NOT ping
again and again - only the moment it (re)appears in stock triggers a ping.

First run behaviour: if state/seen_products.json doesn't exist yet, this is
treated as a baseline pass - every currently-available matching product gets
recorded as the starting point, but NO Discord notifications are sent
(otherwise you'd get hundreds of messages for products that have simply been
sitting in stock for months).
"""

import asyncio
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor

import httpx

from config import STORES, GAME_KEYWORDS, PRODUCT_LINES, WEBHOOK_ENV_VARS

STATE_FILE = "state/seen_products.json"

CONCURRENCY = 3
# A flat timeout isn't the right tool here: the earlier run showed most
# failures are connections that just hang (consistent with Cloudflare-style
# bot challenges holding the connection open) rather than fast rejections.
# A short, explicit connect timeout lets us bail out quickly instead of
# burning the whole per-store budget on one stuck request.
REQUEST_TIMEOUT = httpx.Timeout(connect=8.0, read=10.0, write=10.0, pool=10.0)
MAX_PAGES_PER_STORE = 3
PAGE_SIZE = 250
MAX_RETRIES = 2
BASE_BACKOFF = 1.0
PAGE_DELAY = 0.3  # politeness delay between pages of the same store
PER_STORE_TIMEOUT = 30.0  # hard cap so one slow/blocked store can't stall the whole run

PREORDER_KEYWORDS = ["pre-order", "preorder", "pre order"]

GAME_DISPLAY_NAMES = {
    "pokemon": "Pokémon",
    "one_piece": "One Piece",
    "mtg": "Magic: The Gathering",
    "riftbound": "Riftbound",
}

REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-AU,en;q=0.9",
}


# ---------------------------------------------------------------------------
# State persistence
# ---------------------------------------------------------------------------

def load_state() -> dict:
    if not os.path.exists(STATE_FILE):
        return {"initialized": False, "seen": {}}
    with open(STATE_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def save_state(state: dict) -> None:
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, ensure_ascii=False)


# ---------------------------------------------------------------------------
# Matching logic
# ---------------------------------------------------------------------------

def detect_game(text: str) -> str | None:
    for game, keywords in GAME_KEYWORDS.items():
        if any(kw in text for kw in keywords):
            return game
    return None


def detect_product_line(game: str, text: str) -> str | None:
    for phrase in PRODUCT_LINES.get(game, []):
        if phrase in text:
            return phrase
    return None


# ---------------------------------------------------------------------------
# Networking
# ---------------------------------------------------------------------------

HARD_REQUEST_DEADLINE = 10.0  # independent backstop - see comment below


async def get_with_retries(client: httpx.AsyncClient, url: str) -> httpx.Response | None:
    for attempt in range(MAX_RETRIES):
        start = time.monotonic()
        try:
            resp = await asyncio.wait_for(
                client.get(url, timeout=REQUEST_TIMEOUT, follow_redirects=True, headers=REQUEST_HEADERS),
                timeout=HARD_REQUEST_DEADLINE,
            )
            elapsed = time.monotonic() - start
            print(f"  {url} -> http_{resp.status_code} in {elapsed:.1f}s")
            if resp.status_code in (429, 503) and attempt < MAX_RETRIES - 1:
                retry_after = resp.headers.get("Retry-After")
                wait = float(retry_after) if retry_after and retry_after.isdigit() else BASE_BACKOFF * (2 ** attempt)
                await asyncio.sleep(wait)
                continue
            return resp
        except Exception as e:
            elapsed = time.monotonic() - start
            print(f"  request error on {url}: {type(e).__name__}: {e} (after {elapsed:.1f}s)")
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(BASE_BACKOFF * (2 ** attempt))
            else:
                return None
    return None


async def fetch_all_products(client: httpx.AsyncClient, store_url: str) -> list[dict]:
    products = []
    for page in range(1, MAX_PAGES_PER_STORE + 1):
        url = f"{store_url.rstrip('/')}/products.json?limit={PAGE_SIZE}&page={page}"
        resp = await get_with_retries(client, url)
        if resp is None:
            print(f"  [{store_url}] page {page}: no response after retries")
            break
        if resp.status_code != 200:
            print(f"  [{store_url}] page {page}: http_{resp.status_code}")
            break
        try:
            data = resp.json()
        except Exception as e:
            print(f"  [{store_url}] page {page}: JSON parse failed: {e}")
            break
        batch = data.get("products", [])
        if not batch:
            break
        products.extend(batch)
        if len(batch) < PAGE_SIZE:
            break
        await asyncio.sleep(PAGE_DELAY)
    return products


# ---------------------------------------------------------------------------
# Per-store processing
# ---------------------------------------------------------------------------

async def process_store(client: httpx.AsyncClient, store: dict, state: dict, sem: asyncio.Semaphore) -> list[dict]:
    domain = store["domain"]
    async with sem:
        await asyncio.sleep(0.2)  # jitter so requests don't fire in lockstep
        store_start = time.monotonic()
        print(f"[{domain}] starting fetch")
        try:
            products = await asyncio.wait_for(
                fetch_all_products(client, store["url"]), timeout=PER_STORE_TIMEOUT
            )
        except asyncio.TimeoutError:
            print(f"[{domain}] TIMEOUT - exceeded {PER_STORE_TIMEOUT}s budget after {time.monotonic()-store_start:.1f}s, skipping this run")
            return []
        except Exception as e:
            print(f"[{domain}] ERROR fetching products: {e}")
            return []

    if not products:
        print(f"[{domain}] no products returned (site may be down or blocking us)")
        return []

    seen = state["seen"].setdefault(domain, {})
    findings = []
    now = time.time()

    for p in products:
        pid = str(p.get("id"))
        title = p.get("title", "") or ""
        product_type = p.get("product_type", "") or ""
        tags = p.get("tags", [])
        if isinstance(tags, str):
            tags = [t.strip() for t in tags.split(",")]
        combined = " ".join([title, product_type, " ".join(tags)]).lower()

        game = detect_game(combined)
        if not game:
            continue
        line = detect_product_line(game, combined)
        if not line:
            continue

        # Only care about products that can actually be bought right now -
        # in stock, or in stock as a preorder. Shopify's "available" flag on
        # a variant already covers both cases (it's true for normal stock,
        # and for preorder listings where "continue selling when out of
        # stock" is enabled).
        variants = p.get("variants", []) or []
        available_variants = [v for v in variants if v.get("available")]
        record = seen.get(pid)
        now_available = bool(available_variants)

        if not now_available:
            # Not purchasable right now. Keep the existing record (marked
            # unavailable) so that if it comes back in stock later we can
            # detect that transition and notify. Don't create a brand new
            # record for something we've never seen in stock.
            if record is not None:
                record["available"] = False
                record["last_checked"] = now
            continue

        prices = [float(v["price"]) for v in available_variants if v.get("price") not in (None, "")]
        price = min(prices) if prices else None
        is_preorder = any(kw in combined for kw in PREORDER_KEYWORDS)
        status_label = "Pre-order" if is_preorder else "In Stock"

        if not state.get("initialized"):
            # Baseline pass: record current availability as the starting
            # point, but don't notify - otherwise every in-stock product
            # across all 85 stores would ping at once.
            seen[pid] = {
                "title": title, "line": line, "game": game,
                "first_seen": now, "available": True, "last_checked": now,
            }
            continue

        # Notify only on a genuine transition: either this product has never
        # been seen before (brand new listing, already in stock), or it was
        # previously out of stock and has now come back - i.e. a restock.
        was_available = record.get("available") if record else None
        should_notify = (record is None) or (was_available is False)

        seen[pid] = {
            "title": title, "line": line, "game": game,
            "first_seen": (record.get("first_seen", now) if record else now),
            "available": True, "last_checked": now,
        }

        if should_notify:
            handle = p.get("handle", "")
            product_url = f"{store['url'].rstrip('/')}/products/{handle}"
            findings.append({
                "game": game,
                "title": title,
                "line": line,
                "store": domain,
                "url": product_url,
                "price": price,
                "status": status_label,
            })

    print(f"[{domain}] checked {len(products)} products, {len(findings)} notifications due")
    return findings


# ---------------------------------------------------------------------------
# Discord notification
# ---------------------------------------------------------------------------

LINE_COLOURS = {
    "pokemon": 0xFFCB05,
    "one_piece": 0xD4AF37,
    "mtg": 0x1E90FF,
    "riftbound": 0x8A2BE2,
}


async def send_discord_notifications(client: httpx.AsyncClient, game: str, findings: list[dict]) -> None:
    env_var = WEBHOOK_ENV_VARS.get(game)
    webhook_url = os.environ.get(env_var, "")
    if not webhook_url:
        print(f"No webhook configured for '{game}' (expected env var {env_var}) - skipping {len(findings)} notifications")
        return

    for f in findings:
        price_str = f"${f['price']:.2f} AUD" if f["price"] is not None else "Price unavailable"
        embed = {
            "title": f["title"][:256],
            "url": f["url"],
            "description": f"**{GAME_DISPLAY_NAMES.get(game, game)}** \u2014 {f['line'].title()}",
            "color": LINE_COLOURS.get(game, 0x2ECC71),
            "fields": [
                {"name": "Price", "value": price_str, "inline": True},
                {"name": "Status", "value": f["status"], "inline": True},
                {"name": "Store", "value": f["store"], "inline": True},
            ],
        }
        payload = {"embeds": [embed]}
        try:
            resp = await client.post(webhook_url, json=payload, timeout=15)
            if resp.status_code not in (200, 204):
                print(f"Discord post failed ({resp.status_code}): {resp.text[:200]}")
        except Exception as e:
            print(f"Discord post error: {e}")
        await asyncio.sleep(1.2)  # stay well under Discord's rate limits


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main() -> None:
    # DNS resolution happens on asyncio's default thread pool executor.
    # Cancelling an asyncio.wait_for() does NOT kill the underlying OS
    # thread doing the actual getaddrinfo() call - it just abandons it while
    # it keeps running. If the pool is small, those abandoned lookups can
    # pile up and starve every subsequent store's DNS resolution, which
    # would explain widespread hangs regardless of per-request timeouts.
    # Widening the pool removes that as a possible cause.
    loop = asyncio.get_event_loop()
    loop.set_default_executor(ThreadPoolExecutor(max_workers=64))

    state = load_state()
    state.setdefault("seen", {})

    sem = asyncio.Semaphore(CONCURRENCY)
    all_findings: list[dict] = []

    async with httpx.AsyncClient(limits=httpx.Limits(max_connections=20, max_keepalive_connections=10)) as client:
        tasks = [process_store(client, store, state, sem) for store in STORES]
        results = await asyncio.gather(*tasks)
        for r in results:
            all_findings.extend(r)

        if not state.get("initialized"):
            total_seen = sum(len(v) for v in state["seen"].values())
            if total_seen == 0:
                print(
                    "\nWARNING: baseline pass recorded 0 products across all stores. "
                    "This almost always means every store failed to fetch (network/blocking "
                    "issue), not that there's genuinely nothing in stock. NOT marking baseline "
                    "as complete, so the next run will retry the baseline pass instead of "
                    "treating everything as a brand new restock."
                )
            else:
                print(f"\nFirst run: baseline recorded for {total_seen} existing products across {len(STORES)} stores.")
                print("No Discord notifications sent on this run (that's expected).")
                state["initialized"] = True
        else:
            print(f"\nFound {len(all_findings)} new product(s) since last check.")
            by_game: dict[str, list[dict]] = {}
            for f in all_findings:
                by_game.setdefault(f["game"], []).append(f)
            for game, findings in by_game.items():
                await send_discord_notifications(client, game, findings)

    save_state(state)
    print("State saved.")


if __name__ == "__main__":
    asyncio.run(main())
