#!/usr/bin/env python3
"""
scan_page.py — weighted-slot scanner (optimized + min-coverage) with AdTech instrumentation

New features (selective & opt-in):
- Distinguish Bids vs Wins (win rate)
- Optional headful mode (--headful)
- Optional human simulation (scroll / random mouse) (--simulate-human)
- AdTech-only share calculation (filter out images/fonts/etc) and ability to block non-ad resources (--block-non-ad-resources)
- Ads.txt auditing (--ads-txt-check)
- Floor price heuristics, format detection (video/display), dealid, schain, bidder-count, ID-solution detection
- Latency measurements (avg / p95) for PubMatic bid responses
- Extended wait for refresh detection (--extended-wait)
- Keeps same proxy per page/run; proxy rotation only between pages/runs (no mid-page change)
- All new features are optional and toggleable via CLI flags; defaults preserve previous behavior
"""

import os
import sys
import json
import time
import argparse
import logging
import threading
import random
import re
from datetime import datetime
from pathlib import Path
from math import floor
from collections import defaultdict, Counter
from contextlib import suppress

try:
    from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError, Route, Request
except Exception as e:
    print("FATAL: playwright not available. Run inside Playwright-enabled environment (docker or preinstalled). Error:", e, file=sys.stderr)
    sys.exit(3)

# ---------------- CONFIG (tunable via env) ----------------
TOTAL_DAILY_SLOTS = int(os.environ.get("TOTAL_DAILY_SLOTS", "6"))
BASE_N_RUNS_PER_PAGE = int(os.environ.get("BASE_N_RUNS_PER_PAGE", "1"))
TARGET_ITERATION_MINUTES = int(os.environ.get("TARGET_ITERATION_MINUTES", "60"))
FLEX_MINUTES = int(os.environ.get("FLEX_MINUTES", "15"))

NAV_TIMEOUT_MIN = float(os.environ.get("NAV_TIMEOUT_MIN", "3"))
NAV_TIMEOUT_MAX = float(os.environ.get("NAV_TIMEOUT_MAX", "120"))
WAIT_AFTER_LOAD_MIN = float(os.environ.get("WAIT_AFTER_LOAD_MIN", "1"))
WAIT_AFTER_LOAD_MAX = float(os.environ.get("WAIT_AFTER_LOAD_MAX", "60"))
GLOBAL_PAGE_RUN_TIMEOUT_MIN = float(os.environ.get("GLOBAL_PAGE_RUN_TIMEOUT_MIN", "8"))
GLOBAL_PAGE_RUN_TIMEOUT_MAX = float(os.environ.get("GLOBAL_PAGE_RUN_TIMEOUT_MAX", "600"))

DEFAULT_OUTDIR = os.environ.get("OUTDIR", "output")
DEFAULT_PROXY_FILE = os.environ.get("PROXIES_FILE", "country_proxies.json")

# detection markers (expanded)
PUBMATIC_STRONG = ["pubmatic", "ads.pubmatic", "pubmatic.com", "hb.pubmatic", "pwt"]
PREBID_MARKERS = ["pbjs", "prebid", "prebid.js"]
# heuristics for wins (common patterns)
PUBMATIC_WIN_MARKERS = ["type=win", "/wt", "event=win", "type=bidwon", "win=true", "win=1"]

# default ad-tech domain substrings (can be extended via --ad-domains-file)
DEFAULT_ADTECH_STRINGS = [
    "pubmatic", "doubleclick", "googlesyndication", "googleads", "gpt", "adservice",
    "rubicon", "openx", "appnexus", "adx", "indexexchange", "index-adserver", "magnite",
    "adsafeprotected", "triplelift", "spotx", "sovrn", "adnxs", "adzerk", "serving-sys"
]

# fields truncation
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
MAX_HEADER_VALUE_LEN = int(os.environ.get("MAX_HEADER_VALUE_LEN", "200"))
MAX_POSTDATA_LEN = int(os.environ.get("MAX_POSTDATA_LEN", "200"))

logging.basicConfig(level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
                    format="%(asctime)s %(levelname)s %(message)s")

# ---------------- helpers ----------------
def timestamp_str():
    return datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

def load_json(p):
    if not os.path.exists(p):
        return {}
    with open(p, "r", encoding="utf-8") as f:
        return json.load(f)

def ensure_dir(d):
    Path(d).mkdir(parents=True, exist_ok=True)

def sanitize(s):
    return "".join(c if (c.isalnum() or c in "-._") else "_" for c in s)[:200]

def _truncate_str(s, length):
    if s is None:
        return None
    s = str(s)
    if len(s) > length:
        return s[:length] + "...(truncated)"
    return s

def _filter_headers(headers):
    keep = {}
    if not headers:
        return keep
    for k, v in headers.items():
        kl = k.lower()
        if kl in ("content-type", "user-agent", "referer", "origin", "accept", "x-forwarded-for"):
            keep[k] = _truncate_str(v, MAX_HEADER_VALUE_LEN)
    return keep

def is_adtech_url(url, ad_strings):
    low = (url or "").lower()
    return any(s in low for s in ad_strings)

def extract_query_param(url, keynames):
    """Return first matched query param value heuristically (pubId, dealid, schain, etc)."""
    if not url:
        return None
    for key in keynames:
        # simple regex search key=VALUE
        m = re.search(r"[?&]" + re.escape(key) + r"=([^&\s]+)", url, flags=re.IGNORECASE)
        if m:
            return m.group(1)
    return None

def safe_json_load(s):
    try:
        return json.loads(s)
    except Exception:
        return None

def parse_ads_txt(domain):
    """Fetch and parse ads.txt for domain (best-effort). Returns list of tuples (domain, seller_account, type)."""
    import requests
    try:
        url = f"https://{domain}/ads.txt"
        r = requests.get(url, timeout=6)
        if r.status_code != 200:
            return []
        out = []
        for ln in r.text.splitlines():
            ln = ln.strip()
            if not ln or ln.startswith("#"):
                continue
            parts = [p.strip() for p in ln.split(",")]
            if len(parts) >= 3:
                out.append((parts[0], parts[1], parts[2].upper()))
        return out
    except Exception:
        return []

# ---------- slot allocation (same as before) ----------
def allocate_slots(publishers, total_slots):
    rows = []
    for p in publishers:
        w = float(p.get("weight_pct", 0.0))
        raw = w / 100.0 * total_slots
        desired = int(floor(raw))
        if desired < 1:
            desired = 1
        rows.append({"name": p["name"], "desired": desired, "raw": raw, "weight": w})
    copies = []
    for r in rows:
        for _ in range(r["desired"]):
            copies.append({"name": r["name"], "weight": r["weight"]})
    if not copies and rows:
        for r in rows:
            copies.append({"name": r["name"], "weight": r["weight"]})
    import heapq
    slot_buckets = [[] for _ in range(total_slots)]
    heap = [(0, idx) for idx in range(total_slots)]
    heapq.heapify(heap)
    for c in copies:
        size, slot = heapq.heappop(heap)
        slot_buckets[slot].append(c["name"])
        heapq.heappush(heap, (size + 1, slot))
    pub_to_slots = {}
    for slot_idx, bucket in enumerate(slot_buckets):
        for name in bucket:
            pub_to_slots.setdefault(name, []).append(slot_idx)
    return pub_to_slots

# ---------- dynamic timing ----------
def compute_timeouts_and_runs(num_pages, target_minutes=TARGET_ITERATION_MINUTES, flex_minutes=FLEX_MINUTES):
    if num_pages <= 0:
        num_pages = 1
    budget_seconds = target_minutes * 60
    per_page_budget = budget_seconds / num_pages
    if per_page_budget < 20:
        n_runs = 1
    elif per_page_budget < 90:
        n_runs = 1
    elif per_page_budget < 240:
        n_runs = 2
    else:
        n_runs = max(1, BASE_N_RUNS_PER_PAGE)
    nav = per_page_budget * 0.45
    wait = per_page_budget * 0.30
    safety = per_page_budget * 0.20
    nav = max(NAV_TIMEOUT_MIN, min(NAV_TIMEOUT_MAX, nav))
    wait = max(WAIT_AFTER_LOAD_MIN, min(WAIT_AFTER_LOAD_MAX, wait))
    global_run_timeout = max(GLOBAL_PAGE_RUN_TIMEOUT_MIN, min(GLOBAL_PAGE_RUN_TIMEOUT_MAX, safety))
    return int(n_runs), int(nav * 1000), int(wait * 1000), int(global_run_timeout)

# ---------- capture logic (mini-har + instrumentation) ----------
def capture_single_run(playwright, url, outdir, domain, page_label, geo, proxy_url, mobile, iteration, run_idx,
                       NAV_TIMEOUT_MS, WAIT_AFTER_LOAD_MS, GLOBAL_PAGE_RUN_TIMEOUT_SEC,
                       opts):
    """
    opts: dict containing flags:
      headful, simulate_human, block_non_ad, ad_strings, ads_txt_check, extended_wait
    """
    run_ts = timestamp_str()
    safe = sanitize(f"{domain}_{page_label}_{geo}_iter{iteration}_run{run_idx}_{run_ts}")
    mini_har_path = os.path.join(outdir, f"{safe}.minihar.json")
    summary_file = os.path.join(outdir, f"{safe}.json")

    req_entries = {}
    seq = []
    flags = {"prebid": False, "pubmatic": False}

    # instrumentation counters
    counters = {
        "pub_bids": 0,
        "pub_wins": 0,
        "pub_adtech_requests": 0,
        "adtech_requests": 0,
        "direct_wins": 0,
        "reseller_wins": 0,
        "dealids": Counter(),
        "formats": Counter(),  # display/video/native
        "latencies_ms": [],  # pubmatic response latencies
        "bidder_counts": [],  # per-auction bidder counts
        "id_solutions": Counter(),  # id types seen
        "schain_hops": [],  # number of hops per schain
        "refresh_wins": 0,
    }

    # storage for auction grouping heuristics (auction_id -> bidders list)
    auction_bidders = defaultdict(lambda: set())

    # ads.txt cache
    ads_txt_entries = []
    if opts.get("ads_txt_check"):
        try:
            ads_txt_entries = parse_ads_txt(domain)
            logging.debug("ads.txt entries for %s: %s", domain, ads_txt_entries)
        except Exception:
            ads_txt_entries = []

    launch_args = {"headless": not opts.get("headful", False), "args": ["--no-sandbox", "--disable-setuid-sandbox"]}
    ctx_args = {"ignore_https_errors": True}
    if proxy_url:
        launch_args["proxy"] = {"server": proxy_url}
    user_agent = "Mozilla/5.0 (Linux; Android 11; Pixel 5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117 Mobile Safari/537.36" if mobile else "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117 Safari/537.36"
    ctx_args["user_agent"] = user_agent

    browser = None
    context = None
    page = None
    start_time = time.time()
    timer = None  # watchdog timer

    try:
        browser = playwright.chromium.launch(**launch_args)

        def _kill_browser():
            try:
                logging.warning("Watchdog triggered: forcing browser/context close for %s", safe)
                with suppress(Exception):
                    if context:
                        context.close()
                with suppress(Exception):
                    if browser:
                        browser.close()
            except Exception:
                pass

        try:
            if GLOBAL_PAGE_RUN_TIMEOUT_SEC and GLOBAL_PAGE_RUN_TIMEOUT_SEC > 0:
                timer = threading.Timer(GLOBAL_PAGE_RUN_TIMEOUT_SEC, _kill_browser)
                timer.daemon = True
                timer.start()
        except Exception:
            logging.exception("Failed to start watchdog timer", exc_info=True)

        # device-aware context
        device = None
        try:
            if hasattr(playwright, "devices"):
                try:
                    device = playwright.devices.get("Pixel 5") if isinstance(playwright.devices, dict) else playwright.devices.get("Pixel 5")
                except Exception:
                    try:
                        device = playwright.devices["Pixel 5"]
                    except Exception:
                        device = None
        except Exception:
            device = None

        if mobile:
            if device:
                device_opts = dict(device)
                device_opts.pop("name", None)
                device_opts["user_agent"] = user_agent
                device_opts["ignore_https_errors"] = True
                context = browser.new_context(**device_opts)
            else:
                context = browser.new_context(user_agent=user_agent,
                                              viewport={"width":412, "height":915},
                                              is_mobile=True, has_touch=True,
                                              ignore_https_errors=True)
        else:
            context = browser.new_context(user_agent=user_agent,
                                          viewport={"width":1366, "height":768},
                                          ignore_https_errors=True)

        page = context.new_page()

        # route handler to optionally block non-ad resources to save proxy bandwidth
        ad_strings = opts.get("ad_strings", DEFAULT_ADTECH_STRINGS)
        if opts.get("block_non_ad_resources"):
            def route_handler(route: Route, request: Request):
                try:
                    url = request.url
                    resource = request.resource_type
                    # allow navigation & XHR/fetch & script & document
                    if resource in ("document", "xhr", "fetch", "script", "stylesheet"):
                        return route.continue_()
                    # allow if url appears to be adtech
                    if is_adtech_url(url, ad_strings):
                        return route.continue_()
                    # otherwise abort to save bandwidth
                    return route.abort()
                except Exception:
                    return route.continue_()
            page.route("**/*", route_handler)

        # instrumentation helpers
        def record_pubmatic_request(req_obj, post_data, ts):
            """Detect bid vs win heuristics from URL/post_data and record counters and latency keys later on response."""
            url = req_obj.url
            low = (url or "").lower()
            is_pub = is_adtech_url(url, ["pubmatic", "pwt", "hb.pubmatic"])
            if not is_pub:
                return False
            # classify as bid if contains typical bid host or param
            is_bid = any("hb.pubmatic" in low or "hb.pubmatic" in low for _ in [0])
            # wins are more likely to appear as beacons containing win indicators
            is_win = any(m in low for m in PUBMATIC_WIN_MARKERS)
            # best-effort inspect post_data for 'type' flags or seatbid info
            body = post_data or ""
            if body and isinstance(body, str):
                if "type\":\"win" in body or '"event":"win"' in body:
                    is_win = True
                # prebid responses often include 'ad' with creative or vast urls
            return is_pub, is_bid, is_win

        def on_request(r):
            try:
                key = id(r)
                ts = time.time()
                post = None
                try:
                    if callable(getattr(r, "post_data", None)):
                        post = r.post_data()
                    else:
                        post = getattr(r, "post_data", None)
                except Exception:
                    post = None
                headers_val = {}
                try:
                    if callable(getattr(r, "headers", None)):
                        headers_val = r.headers()
                    else:
                        headers_val = r.headers if hasattr(r, "headers") else {}
                except Exception:
                    headers_val = {}
                entry = {
                    "request_ts": ts,
                    "method": getattr(r, "method", None) or (r._impl_obj.method if hasattr(r, "_impl_obj") else None),
                    "url": r.url,
                    "headers": _filter_headers(headers_val),
                    "post_data": _truncate_str(post, MAX_POSTDATA_LEN),
                    "resource_type": r.resource_type
                }
                req_entries[key] = entry
                seq.append(key)
                low = (r.url or "").lower()

                # detect prebid marker (client side)
                if not flags["prebid"] and any(k in low for k in PREBID_MARKERS):
                    flags["prebid"] = True

                # adtech count
                if is_adtech_url(r.url, ad_strings):
                    counters["adtech_requests"] += 1
                    # pubmatic adtech count
                    if any(s in low for s in ["pubmatic", "pwt", "hb.pubmatic"]):
                        counters["pub_adtech_requests"] += 1

                # detect bids/wins heuristics
                is_pub_detection = False
                try:
                    is_pub, is_bid, is_win = record_pubmatic_request(r, post, ts)
                    is_pub_detection = is_pub
                except Exception:
                    is_pub = is_bid = is_win = False

                if is_pub_detection:
                    # tag in entry for later matching on response
                    req_entries[key]["is_pubmatic"] = True
                    req_entries[key]["is_pubmatic_bid_like"] = bool(is_bid)
                    req_entries[key]["is_pubmatic_win_like"] = bool(is_win)
                    if is_bid:
                        counters["pub_bids"] += 1
                    if is_win:
                        counters["pub_wins"] += 1  # early increment (will be re-evaluated on response)
                # Auction grouping heuristics: attempt to extract auction id
                aid = extract_query_param(r.url, ["auctionId", "auction_id", "auctionIdEncoded", "auid", "tid", "requestId"])
                if not aid and post:
                    maybe = safe_json_load(post)
                    if isinstance(maybe, dict):
                        aid = maybe.get("auctionId") or maybe.get("tid") or maybe.get("id")
                if aid:
                    # attempt to find bidders list
                    bidders = []
                    try:
                        if post:
                            body_json = safe_json_load(post)
                            if isinstance(body_json, dict):
                                # Prebid: find bidderCodes or bidders
                                if "bidders" in body_json:
                                    bidders = list(body_json.get("bidders") or [])
                                elif "bidderCodes" in body_json:
                                    bidders = body_json.get("bidderCodes") or []
                                elif "imp" in body_json:
                                    # openrtb style
                                    for imp in body_json.get("imp", []):
                                        pass
                    except Exception:
                        bidders = []
                    for b in bidders:
                        auction_bidders[aid].add(b)
            except Exception:
                logging.debug("on_request exception", exc_info=True)

        def on_response(resp):
            try:
                req = resp.request
                key = id(req)
                ts = time.time()
                status = None
                try:
                    status = resp.status
                except Exception:
                    status = None
                try:
                    rheaders = resp.headers or {}
                except Exception:
                    rheaders = {}
                if key in req_entries:
                    req_entries[key].update({
                        "response_ts": ts,
                        "status": status,
                        "response_headers_count": len(rheaders)
                    })
                else:
                    req_entries[key] = {
                        "request_ts": None,
                        "method": None,
                        "url": resp.url,
                        "headers": {},
                        "post_data": None,
                        "response_ts": ts,
                        "status": status,
                        "response_headers_count": len(rheaders)
                    }
                low = (resp.url or "").lower()
                # if pubmatic detected in response url or pre-existing flag
                is_pub = False
                if any(k in low for k in PUBMATIC_STRONG):
                    is_pub = True
                if key in req_entries and req_entries[key].get("is_pubmatic"):
                    is_pub = True
                if is_pub:
                    flags["pubmatic"] = True
                    # latency
                    rentry = req_entries.get(key, {})
                    if rentry.get("request_ts") and rentry.get("response_ts"):
                        lat_ms = int((rentry["response_ts"] - rentry["request_ts"]) * 1000)
                        counters["latencies_ms"].append(lat_ms)
                    # re-evaluate win detection by URL or response headers or body (beacon)
                    is_win = False
                    if any(m in low for m in PUBMATIC_WIN_MARKERS):
                        is_win = True
                    # check content-type hints for creative type
                    ctype = (rheaders.get("content-type") or "").lower()
                    if "video" in ctype or any(ext in (resp.url or "").lower() for ext in (".mp4", ".m3u8", ".m3u8", ".ts")):
                        counters["formats"]["video"] += 1
                        if is_win:
                            counters["formats"]["video"] += 0  # counted in formats only
                    # inspect body for dealid, schain, id tokens (best-effort)
                    body_text = None
                    try:
                        body_text = resp.text()
                    except Exception:
                        body_text = None
                    # dealid detection
                    did = extract_query_param(resp.url, ["dealid", "deal_id", "dealId", "pmp"])
                    if not did and body_text:
                        m = re.search(r'"dealId"\s*:\s*"([^"]+)"', body_text)
                        if m:
                            did = m.group(1)
                    if did:
                        counters["dealids"][did] += 1
                    # schain detection
                    sch = extract_query_param(resp.url, ["schain", "sch"])
                    if not sch and body_text:
                        m = re.search(r'"schain"\s*:\s*(\{[^\}]+\})', body_text)
                        if m:
                            # best-effort count nodes
                            nodes = re.findall(r'"asi"\s*:', m.group(1))
                            counters["schain_hops"].append(len(nodes) or 1)
                    # id solutions detection
                    if body_text:
                        if "id5" in body_text.lower():
                            counters["id_solutions"]["id5"] += 1
                        if "uid2" in body_text.lower():
                            counters["id_solutions"]["uid2"] += 1
                        if "identity" in body_text.lower() and "pubmatic" in body_text.lower():
                            counters["id_solutions"]["pubmatic_identity"] += 1
                    # direct vs reseller: check ads.txt if available and pub id present
                    # extract pub id from url or body
                    pubid = extract_query_param(resp.url, ["pubid", "pubId", "account", "publisher_id", "pmid"])
                    if not pubid and body_text:
                        m = re.search(r'pubId["\']?\s*[:=]\s*["\']?(\d+)', body_text)
                        if m:
                            pubid = m.group(1)
                    if is_win or req_entries[key].get("is_pubmatic_win_like"):
                        # count win
                        counters["pub_wins"] += 1
                        # if ads.txt checking enabled, mark direct vs reseller
                        if opts.get("ads_txt_check") and pubid:
                            direct_found = False
                            for a in ads_txt_entries:
                                # a is (adnetwork_domain, seller_account, type)
                                if "pubmatic" in a[0].lower() and str(a[1]) == str(pubid):
                                    if a[2] == "DIRECT":
                                        counters["direct_wins"] += 1
                                        direct_found = True
                                        break
                                    else:
                                        counters["reseller_wins"] += 1
                                        direct_found = True
                                        break
                            if not direct_found:
                                # unknown mapping; treat as reseller-uncertain
                                counters["reseller_wins"] += 0
                        # format classification by checking creative hints
                        # if content-type video OR url contains mp4/m3u8 -> video
                        if "video" in ctype or any(ext in (resp.url or "").lower() for ext in (".mp4", ".m3u8", ".m3u8", ".ts")):
                            counters["formats"]["video"] += 1
                        else:
                            # try to detect banner sizes in body or url
                            if body_text and re.search(r'(\d{2,4})x(\d{2,4})', body_text):
                                counters["formats"]["display"] += 1
                            else:
                                counters["formats"]["display"] += 1
                    # auction pressure: attempt to find auction id and count bidders
                    aid = extract_query_param(resp.url, ["auctionId", "auction_id", "tid", "auid", "requestId"])
                    if aid:
                        bcount = len(auction_bidders.get(aid, []))
                        if bcount:
                            counters["bidder_counts"].append(bcount)
                    # refresh detection: if additional wins arrived after initial wait mark refresh_wins
                    # We'll detect refresh as wins that occurred later than initial WAIT_AFTER_LOAD_MS + 1s
                    if rentry.get("response_ts") and (rentry.get("response_ts") - start_time) > (WAIT_AFTER_LOAD_MS/1000.0 + 1.0):
                        counters["refresh_wins"] += 1
                    # dealid counts already recorded
                # prebid detection on response body too
                if not flags["prebid"]:
                    try:
                        btext = None
                        try:
                            btext = resp.text()
                        except Exception:
                            btext = None
                        if btext and any(k in btext.lower() for k in PREBID_MARKERS):
                            flags["prebid"] = True
                    except Exception:
                        pass
            except Exception:
                logging.debug("on_response exception", exc_info=True)

        page.on("request", on_request)
        page.on("response", on_response)

        # Simulate human-like route for bot mitigation if requested (small random movements on page)
        def human_simulate_scroll_and_mouse(page_obj, viewport_height, simulate_extra=False):
            try:
                steps = 4 + random.randint(0,3)
                for i in range(steps):
                    # scroll fraction
                    frac = (i + 1) / steps
                    y = int(viewport_height * frac * (0.6 + random.random()*0.8))
                    # use evaluate scroll to avoid needing mouse wheel
                    try:
                        page_obj.evaluate("window.scrollTo({left: 0, top: %d, behavior: 'smooth'})" % y)
                    except Exception:
                        try:
                            page_obj.evaluate("window.scrollTo(0, %d)" % y)
                        except Exception:
                            pass
                    # small random mouse move
                    try:
                        page_obj.mouse.move(random.randint(10, 300), random.randint(10, 400))
                    except Exception:
                        pass
                    time.sleep(0.6 + random.random()*0.8)
                if simulate_extra:
                    # random small clicks in whitespace region
                    try:
                        page_obj.mouse.click(random.randint(10, 300), random.randint(10, 400))
                    except Exception:
                        pass
            except Exception:
                pass

        # navigation with NAV_TIMEOUT_MS (ms)
        try:
            page.goto(url, timeout=NAV_TIMEOUT_MS)
        except PlaywrightTimeoutError:
            logging.warning("Navigation timeout for url=%s", url)
        except Exception as e:
            logging.warning("Navigation exception %s", e)

        # check global timeout after navigation
        if (GLOBAL_PAGE_RUN_TIMEOUT_SEC and (time.time() - start_time) > GLOBAL_PAGE_RUN_TIMEOUT_SEC):
            logging.warning("Global run timeout exceeded after navigation, ending early for %s", url)
        else:
            # attempt to wait for networkidle for nav timeout or provided
            with suppress(Exception):
                page.wait_for_load_state("networkidle", timeout=min(NAV_TIMEOUT_MS, NAV_TIMEOUT_MS))

            # simulate human actions if requested to trigger lazy-load slots
            if opts.get("simulate_human"):
                try:
                    vp = page.viewport_size or {"width":1366, "height":768}
                    human_simulate_scroll_and_mouse(page, vp.get("height", 768), simulate_extra=True)
                except Exception:
                    pass

            # extra wait for late calls
            if not (GLOBAL_PAGE_RUN_TIMEOUT_SEC and (time.time() - start_time) > GLOBAL_PAGE_RUN_TIMEOUT_SEC):
                try:
                    page.wait_for_timeout(WAIT_AFTER_LOAD_MS)
                except Exception:
                    pass

            # optionally extended wait to capture refresh auctions
            if opts.get("extended_wait"):
                try:
                    page.wait_for_timeout( min(90000, max(5000, opts.get("extended_wait_ms", 90000))) )
                except Exception:
                    pass

    except Exception as e:
        logging.exception("Error during capture: %s", e)
    finally:
        # cancel watchdog timer first to avoid race where timer fires during shutdown sequence
        with suppress(Exception):
            if timer:
                timer.cancel()
        with suppress(Exception):
            if context:
                context.close()
        with suppress(Exception):
            if browser:
                browser.close()

    # Build compact ordered entries
    compact_list = []
    for key in seq:
        ent = req_entries.get(key, {})
        compact = {
            "request_ts": ent.get("request_ts"),
            "method": ent.get("method"),
            "url": ent.get("url"),
            "req_headers": ent.get("headers", {}),
            "post_data": ent.get("post_data"),
            "resource_type": ent.get("resource_type"),
            "response_ts": ent.get("response_ts"),
            "status": ent.get("status"),
            "response_headers_count": ent.get("response_headers_count"),
            "is_pubmatic": ent.get("is_pubmatic", False),
            "is_pubmatic_bid_like": ent.get("is_pubmatic_bid_like", False),
            "is_pubmatic_win_like": ent.get("is_pubmatic_win_like", False),
        }
        compact_list.append(compact)

    mini = {
        "generated": timestamp_str(),
        "domain": domain,
        "page_label": page_label,
        "url": url,
        "geo": geo,
        "user_agent": user_agent,
        "entries_count": len(compact_list),
        "entries": compact_list
    }

    try:
        with open(mini_har_path, "w", encoding="utf-8") as mf:
            json.dump(mini, mf)
    except Exception:
        logging.exception("Failed to write mini-har to %s", mini_har_path)

    # finalize summary (compute metrics)
    total = len(compact_list)
    pub_hits_list = [u for u in (e.get("url", "") for e in compact_list) if any(k in (u or "").lower() for k in PUBMATIC_STRONG)]
    # adtech-only share
    adtech_total = counters["adtech_requests"] or 0
    pub_adtech = counters["pub_adtech_requests"] or 0
    pub_adtech_share = (pub_adtech / adtech_total) if adtech_total else 0.0
    bids = counters["pub_bids"]
    wins = counters["pub_wins"]
    win_rate = (wins / bids) if bids else None
    # latencies metrics
    lat_list = counters["latencies_ms"]
    avg_lat = (sum(lat_list) / len(lat_list)) if lat_list else None
    p95_lat = None
    if lat_list:
        lat_sorted = sorted(lat_list)
        idx = int(len(lat_sorted) * 0.95) - 1
        idx = max(0, min(len(lat_sorted)-1, idx))
        p95_lat = lat_sorted[idx]
    # bidder counts
    bc = counters["bidder_counts"]
    bidder_avg = (sum(bc)/len(bc)) if bc else None

    summary = {
        "domain": domain,
        "page_label": page_label,
        "url": url,
        "geo": geo,
        "iteration": iteration,
        "run_idx": run_idx,
        "timestamp": run_ts,
        "mini_har_path": mini_har_path,
        "total_requests": total,
        "pubmatic_requests": len(pub_hits_list),
        "pubmatic_sample": pub_hits_list[:10],
        "proxy_used": proxy_url or "",
        "user_agent": user_agent,
        "prebid_detected": flags["prebid"],
        "pubmatic_detected": flags["pubmatic"],
        # new metrics
        "adtech_total_requests": adtech_total,
        "pubmatic_adtech_requests": pub_adtech,
        "pubmatic_adtech_share": pub_adtech_share,
        "pub_bids": bids,
        "pub_wins": wins,
        "pub_win_rate": win_rate,
        "avg_bid_latency_ms": avg_lat,
        "p95_bid_latency_ms": p95_lat,
        "bidder_count_avg": bidder_avg,
        "direct_wins": counters["direct_wins"],
        "reseller_wins": counters["reseller_wins"],
        "formats": dict(counters["formats"]),
        "dealids_count": dict(counters["dealids"]),
        "schain_hops_sample": counters["schain_hops"][:10],
        "id_solutions": dict(counters["id_solutions"]),
        "refresh_wins": counters["refresh_wins"],
    }

    try:
        with open(summary_file, "w", encoding="utf-8") as sf:
            json.dump(summary, sf, indent=2)
    except Exception:
        logging.exception("Failed to write summary json to %s", summary_file)

    logging.info("Run finished %s pub_wins=%s pub_bids=%s pub_adtech_share=%.4f win_rate=%s",
                 safe, summary["pub_wins"], summary["pub_bids"], summary["pubmatic_adtech_share"], str(summary["pub_win_rate"]))
    return summary

# aggregated per page (unchanged semantics, but pass opts)
def run_page_aggregated(playwright, url, outdir, domain, page_label, geo, proxy_url, mobile, iteration,
                        n_runs, NAV_TIMEOUT_MS, WAIT_AFTER_LOAD_MS, GLOBAL_PAGE_RUN_TIMEOUT_SEC, opts):
    runs = []
    for r in range(1, n_runs + 1):
        runs.append(capture_single_run(playwright, url, outdir, domain, page_label, geo, proxy_url, mobile, iteration, r,
                                       NAV_TIMEOUT_MS, WAIT_AFTER_LOAD_MS, GLOBAL_PAGE_RUN_TIMEOUT_SEC, opts))
        time.sleep(1)
    avg_total = sum(x["total_requests"] for x in runs) / len(runs) if runs else 0
    avg_pub = sum(x["pubmatic_requests"] for x in runs) / len(runs) if runs else 0
    any_prebid = any(x.get("prebid_detected") for x in runs)
    any_pubmatic = any(x.get("pubmatic_detected") for x in runs)
    # aggregate extra metrics (simple averages/merges)
    avg_win_rate = None
    wins = sum(x.get("pub_wins", 0) for x in runs)
    bids = sum(x.get("pub_bids", 0) for x in runs)
    avg_win_rate = (wins / bids) if bids else None
    return {"domain": domain, "page_label": page_label, "geo": geo, "iteration": iteration,
            "avg_total_requests": avg_total, "avg_pubmatic_requests": avg_pub, "runs": runs,
            "prebid_detected": any_prebid, "pubmatic_detected": any_pubmatic,
            "avg_win_rate": avg_win_rate}

# ---------------- main ----------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--targets", default="targets.json")
    parser.add_argument("--proxies", default=DEFAULT_PROXY_FILE)
    parser.add_argument("--outdir", default=DEFAULT_OUTDIR)
    parser.add_argument("--iteration", type=int, default=0, help="slot index 0..TOTAL_DAILY_SLOTS-1")
    parser.add_argument("--geo", default="US")
    parser.add_argument("--slots", type=int, default=None, help="override TOTAL_DAILY_SLOTS (env) for this run")
    parser.add_argument("--all", action="store_true", help="ignore slotting and run all publishers")
    parser.add_argument("--verbose", action="store_true", help="enable debug logging")
    # new flags
    parser.add_argument("--headful", action="store_true", help="run browser visible (headful) to avoid headless detection")
    parser.add_argument("--simulate-human", action="store_true", help="simulate human interactions (scroll, mouse)")
    parser.add_argument("--block-non-ad-resources", action="store_true", help="abort loading of non-ad images/videos to save proxy bandwidth")
    parser.add_argument("--ads-txt-check", action="store_true", help="perform ads.txt check to classify direct vs reseller")
    parser.add_argument("--extended-wait", action="store_true", help="wait longer to capture ad refreshes")
    parser.add_argument("--extended-wait-ms", type=int, default=90000, help="ms to wait when --extended-wait (default 90000ms)")
    parser.add_argument("--ad-domains-file", default=None, help="optional file with ad-tech substrings, one per line")
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    targets = load_json(args.targets)
    proxies = load_json(args.proxies)
    ensure_dir(args.outdir)
    timestamp = timestamp_str()
    run_root = os.path.join(args.outdir, timestamp)
    ensure_dir(run_root)

    # load ad-domains
    ad_strings = DEFAULT_ADTECH_STRINGS[:]
    if args.ad_domains_file and os.path.exists(args.ad_domains_file):
        try:
            with open(args.ad_domains_file, "r", encoding="utf-8") as af:
                extra = [l.strip() for l in af if l.strip()]
                ad_strings.extend(extra)
        except Exception:
            logging.debug("Failed loading ad domains file", exc_info=True)

    pubs = targets.get("publishers", [])
    total_slots = args.slots if args.slots is not None else TOTAL_DAILY_SLOTS
    if total_slots < 1:
        total_slots = TOTAL_DAILY_SLOTS

    pub_to_slots = allocate_slots(pubs, total_slots)

    # write slot_map for this run and a latest snapshot at top-level outdir
    slot_map_path = os.path.join(run_root, "slot_map.json")
    with open(slot_map_path, "w", encoding="utf-8") as f:
        json.dump(pub_to_slots, f, indent=2)
    try:
        latest_map_path = os.path.join(args.outdir, "slot_map_latest.json")
        with open(latest_map_path, "w", encoding="utf-8") as f2:
            json.dump({"total_slots": total_slots, "mapping": pub_to_slots}, f2, indent=2)
    except Exception:
        logging.debug("Failed writing slot_map_latest.json", exc_info=True)

    # logging: compute weights per slot and page counts for debugging
    slot_weights = {i: 0.0 for i in range(total_slots)}
    slot_pages = {i: 0 for i in range(total_slots)}
    name_to_pub = {p["name"]: p for p in pubs}
    for name, slots in pub_to_slots.items():
        w = float(name_to_pub.get(name, {}).get("weight_pct", 0.0))
        pages_count = len(name_to_pub.get(name, {}).get("pages", []))
        for s in slots:
            slot_weights[s] = slot_weights.get(s, 0.0) + w
            slot_pages[s] = slot_pages.get(s, 0) + pages_count
    logging.info("Slot weights summary (slot:index->weight_pct_sum,page_count): %s",
                 {i: {"weight_sum": round(slot_weights[i], 4), "pages": slot_pages[i]} for i in slot_weights})

    # select publishers for this iteration
    if args.all:
        selected = pubs[:]  # all publishers
        logging.info("Running with --all: selected all publishers (%s total)", len(selected))
    else:
        selected = [p for p in pubs if p["name"] in pub_to_slots and args.iteration in pub_to_slots[p["name"]]]
        selected = sorted(selected, key=lambda p: -float(p.get("weight_pct", 0)))
        logging.info("Iteration %s selected publishers (ordered by weight): %s", args.iteration, [p["name"] for p in selected])

    # build page entries (desktop + mobile)
    page_entries = []
    for pub in selected:
        for pg in pub.get("pages", []):
            if not pg.get("url"):
                continue
            page_entries.append((pub, pg, False))
            page_entries.append((pub, pg, True))
    total_pages = len(page_entries)

    # compute n_runs and timeouts based on pages count and target minutes
    n_runs_per_page, NAV_MS, WAIT_MS, GLOBAL_RUN_SEC = compute_timeouts_and_runs(total_pages,
                                                                                 target_minutes=TARGET_ITERATION_MINUTES,
                                                                                 flex_minutes=FLEX_MINUTES)
    logging.info("Total pages this iteration: %s -> n_runs_per_page=%s NAV_MS=%sms WAIT_MS=%sms GLOBAL_PAGE_RUN_TIMEOUT_SEC=%ss",
                 total_pages, n_runs_per_page, NAV_MS, WAIT_MS, GLOBAL_RUN_SEC)

    csv_rows = []
    opts = {
        "headful": args.headful,
        "simulate_human": args.simulate_human,
        "block_non_ad_resources": args.block_non_ad_resources,
        "ads_txt_check": args.ads_txt_check,
        "extended_wait": args.extended_wait,
        "extended_wait_ms": args.extended_wait_ms,
        "ad_strings": ad_strings
    }
    with sync_playwright() as p:
        for (pub, pg, mobile) in page_entries:
            domain = pub.get("domain")
            label = pg.get("label")
            url = pg.get("url")
            geos = pub.get("geos", [])
            chosen_geos = [g for g in geos if g["country_code"] == args.geo] if args.geo != "ALL" else geos
            if not chosen_geos:
                logging.info("No geos matching for %s", pub["name"])
                continue
            for geo_entry in chosen_geos:
                country = geo_entry["country_code"]
                proxy_url = geo_entry.get("proxy") or proxies.get(country)
                agg = run_page_aggregated(p, url, run_root, domain, f"{label}_desktop" if not mobile else f"{label}_mobile",
                                         country, proxy_url, mobile, args.iteration, n_runs_per_page, NAV_MS, WAIT_MS, GLOBAL_RUN_SEC, opts)
                avg_total = agg["avg_total_requests"]
                avg_pub = agg["avg_pubmatic_requests"]
                pubmatic_rate = (avg_pub / avg_total) if avg_total > 0 else 0.0
                csv_rows.append({"domain": agg["domain"], "page_label": agg["page_label"], "url": url, "geo": country,
                                 "iteration": args.iteration, "avg_total_requests": avg_total,
                                 "avg_pubmatic_requests": avg_pub, "pubmatic_rate": round(pubmatic_rate, 6),
                                 "runs_count": len(agg["runs"]), "prebid_detected": agg.get("prebid_detected", False),
                                 "pubmatic_detected": agg.get("pubmatic_detected", False), "avg_win_rate": agg.get("avg_win_rate")})

    # write csv
    import csv as _csv
    csv_path = os.path.join(run_root, f"summary_iteration_{args.iteration}.csv")
    fields = ["domain", "page_label", "url", "geo", "iteration", "avg_total_requests", "avg_pubmatic_requests", "pubmatic_rate", "runs_count", "prebid_detected", "pubmatic_detected", "avg_win_rate"]
    with open(csv_path, "w", newline="", encoding="utf-8") as cf:
        w = _csv.DictWriter(cf, fieldnames=fields)
        w.writeheader()
        w.writerows(csv_rows)

    # meta
    meta = {"timestamp": timestamp, "iteration": args.iteration, "selected_publishers": [p["name"] for p in selected],
            "total_pages": total_pages, "n_runs_per_page": n_runs_per_page,
            "NAV_MS": NAV_MS, "WAIT_MS": WAIT_MS, "GLOBAL_PAGE_RUN_TIMEOUT_SEC": GLOBAL_RUN_SEC,
            "total_slots_used": total_slots, "opts": opts}
    with open(os.path.join(run_root, "meta.json"), "w", encoding="utf-8") as jf:
        json.dump(meta, jf, indent=2)

    logging.info("Iteration complete: output in %s (csv=%s) meta=%s", run_root, csv_path, meta)

if __name__ == "__main__":
    main()
