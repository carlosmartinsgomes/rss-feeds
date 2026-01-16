#!/usr/bin/env python3
"""
scan_page.py - weighted-slot scanner with AdTech instrumentation

- Recolhe tráfego por publisher/página
- Instrumenta requests/responses com Playwright
- Extrai métricas de PubMatic e outros SSPs:
  - pubmatic_adtech_share
  - pub_bids, pub_wins, pub_win_rate
  - latências, bidder_count
  - ssp_financials (bids, wins, avg_cpm, avg_floor)
  - ssp_share_of_voice
- Escreve:
  - output/YYYY-MM-DD/<timestamp>/run_summary.json
  - output/YYYY-MM-DD/<timestamp>/run_summary.xlsx (flatten bruto)

Scoring avançado é feito no compute_scores.py.
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
from datetime import datetime, timezone
from pathlib import Path
from math import floor
from collections import defaultdict, Counter
from contextlib import suppress

import pandas as pd

try:
    from playwright.sync_api import (
        sync_playwright,
        TimeoutError as PlaywrightTimeoutError,
        Route,
        Request,
    )
except Exception as e:
    print(
        "FATAL: playwright not available. Run inside Playwright-enabled environment. Error:",
        e,
        file=sys.stderr,
    )
    sys.exit(3)

# -------------------------------------------------------------------
# CONFIG (tunable via env)
# -------------------------------------------------------------------
TOTAL_DAILY_SLOTS = int(os.environ.get("TOTAL_DAILY_SLOTS", "6"))
BASE_N_RUNS_PER_PAGE = int(os.environ.get("BASE_N_RUNS_PER_PAGE", "1"))
TARGET_ITERATION_MINUTES = int(os.environ.get("TARGET_ITERATION_MINUTES", "90"))
FLEX_MINUTES = int(os.environ.get("FLEX_MINUTES", "15"))

NAV_TIMEOUT_MIN = float(os.environ.get("NAV_TIMEOUT_MIN", "3"))
NAV_TIMEOUT_MAX = float(os.environ.get("NAV_TIMEOUT_MAX", "400"))
WAIT_AFTER_LOAD_MIN = float(os.environ.get("WAIT_AFTER_LOAD_MIN", "1"))
WAIT_AFTER_LOAD_MAX = float(os.environ.get("WAIT_AFTER_LOAD_MAX", "360"))
GLOBAL_PAGE_RUN_TIMEOUT_MIN = float(
    os.environ.get("GLOBAL_PAGE_RUN_TIMEOUT_MIN", "30")
)
GLOBAL_PAGE_RUN_TIMEOUT_MAX = float(
    os.environ.get("GLOBAL_PAGE_RUN_TIMEOUT_MAX", "2500")
)

DEFAULT_OUTDIR = os.environ.get("OUTDIR", "output")
DEFAULT_PROXY_FILE = os.environ.get("PROXIES_FILE", "country_proxies.json")

# detection markers
PUBMATIC_STRONG = ["pubmatic", "ads.pubmatic", "pubmatic.com", "hb.pubmatic", "pwt"]
PREBID_MARKERS = ["pbjs", "prebid", "prebid.js"]
PUBMATIC_WIN_MARKERS = [
    "type=win",
    "/wt",
    "event=win",
    "type=bidwon",
    "win=true",
    "win=1",
]

DEFAULT_ADTECH_STRINGS = [
    "pubmatic",
    "doubleclick",
    "googlesyndication",
    "googleads",
    "gpt",
    "adservice",
    "rubicon",
    "openx",
    "appnexus",
    "adx",
    "indexexchange",
    "index-adserver",
    "magnite",
    "adsafeprotected",
    "triplelift",
    "spotx",
    "sovrn",
    "adnxs",
    "adzerk",
    "serving-sys",
]

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
MAX_HEADER_VALUE_LEN = int(os.environ.get("MAX_HEADER_VALUE_LEN", "200"))
MAX_POSTDATA_LEN = int(os.environ.get("MAX_POSTDATA_LEN", "200"))

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s",
)

# -------------------------------------------------------------------
# helpers
# -------------------------------------------------------------------
def timestamp_str():
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def load_json(p: str):
    if not os.path.exists(p):
        return {}
    with open(p, "r", encoding="utf-8") as f:
        return json.load(f)


def ensure_dir(d: str):
    Path(d).mkdir(parents=True, exist_ok=True)


def sanitize(s: str) -> str:
    return "".join(c if (c.isalnum() or c in " -._") else "_" for c in s)[:200]


def _truncate_str(s, length: int):
    if s is None:
        return None
    s = str(s)
    if len(s) > length:
        return s[:length] + " ... (truncated)"
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


def is_adtech_url(url: str, ad_strings) -> bool:
    low = (url or "").lower()
    return any(s in low for s in ad_strings)


def extract_query_param(url: str, keynames):
    if not url:
        return None
    for key in keynames:
        m = re.search(r"[?&]" + re.escape(key) + r"=([^&\s]+)", url, flags=re.IGNORECASE)
        if m:
            return m.group(1)
    return None


def safe_json_load(s):
    try:
        return json.loads(s)
    except Exception:
        return None


def classify_ssp_from_url(url: str):
    if not url:
        return None
    low = url.lower()
    if "pubmatic" in low or "ads.pubmatic" in low or "pwt" in low or "hb.pubmatic" in low:
        return "pubmatic"
    if "rubicon" in low or "magnite" in low:
        return "magnite"
    if "doubleclick" in low or "googlesyndication" in low or "googleads" in low or "adservice.google" in low:
        return "google"
    if "indexexchange" in low or "casalemedia" in low or "indexww" in low:
        return "index"
    if "openx" in low or "openx.net" in low:
        return "openx"
    if "appnexus" in low or "adnxs" in low or "xandr" in low:
        return "xandr"
    if "triplelift" in low or "3lift" in low:
        return "triplelift"
    if "sharethrough" in low or "native.sharethrough" in low:
        return "sharethrough"
    if "sovrn" in low or "lijit" in low:
        return "sovrn"
    return None


def parse_ads_txt(domain: str):
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
        logging.debug("Failed to fetch/parse ads.txt for %s", domain, exc_info=True)
        return []


# -------------------------------------------------------------------
# slot allocation
# -------------------------------------------------------------------
def allocate_slots(publishers, total_slots: int):
    rows = []
    for p in publishers:
        w = float(p.get("weight_pct", 0.0))
        raw = w / 100.0 * total_slots
        desired = int(floor(raw))
        if desired < 1:
            desired = 1
        rows.append({"domain": p["domain"], "desired": desired, "raw": raw, "weight": w})

    copies = []
    for r in rows:
        for _ in range(r["desired"]):
            copies.append({"domain": r["domain"], "weight": r["weight"]})

    if not copies and rows:
        for r in rows:
            copies.append({"domain": r["domain"], "weight": r["weight"]})

    import heapq

    slot_buckets = [[] for _ in range(total_slots)]
    heap = [(0, idx) for idx in range(total_slots)]
    heapq.heapify(heap)
    for c in copies:
        size, slot = heapq.heappop(heap)
        slot_buckets[slot].append(c["domain"])
        heapq.heappush(heap, (size + 1, slot))

    pub_to_slots = {}
    for slot_idx, bucket in enumerate(slot_buckets):
        for domain in bucket:
            pub_to_slots.setdefault(domain, []).append(slot_idx)
    return pub_to_slots


# -------------------------------------------------------------------
# timeouts / runs
# -------------------------------------------------------------------
def compute_timeouts_and_runs(
    num_pages,
    target_minutes=TARGET_ITERATION_MINUTES,
    flex_minutes=FLEX_MINUTES,
):
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

    nav = max(NAV_TIMEOUT_MIN, min(NAV_TIMEOUT_MAX, nav))
    wait = max(WAIT_AFTER_LOAD_MIN, min(WAIT_AFTER_LOAD_MAX, wait))

    global_run_timeout = 0
    return int(n_runs), int(nav * 1000), int(wait * 1000), int(global_run_timeout)


# -------------------------------------------------------------------
# capture logic
# -------------------------------------------------------------------
def capture_single_run(
    playwright,
    url,
    outdir,
    domain,
    page_label,
    geo,
    proxy_url,
    mobile,
    iteration,
    run_idx,
    NAV_TIMEOUT_MS,
    WAIT_AFTER_LOAD_MS,
    GLOBAL_PAGE_RUN_TIMEOUT_SEC,
    opts,
):
    run_ts = timestamp_str()
    safe = sanitize(f"{domain}_{page_label}_{geo}_iter{iteration}_run{run_idx}_{run_ts}")
    mini_har_path = os.path.join(outdir, f"{safe}.minihar.json")

    req_entries = {}
    seq = []

    flags = {"prebid": False, "pubmatic": False}

    counters = {
        "pub_bids": 0,
        "pub_wins": 0,
        "pub_adtech_requests": 0,
        "adtech_requests": 0,
        "direct_wins": 0,
        "reseller_wins": 0,
        "dealids": Counter(),
        "formats": Counter(),
        "latencies_ms": [],
        "bidder_counts": [],
        "id_solutions": Counter(),
        "schain_hops": [],
        "refresh_wins": 0,
        "ssp_bids": Counter(),
        "ssp_wins": Counter(),
        "ssp_prices": defaultdict(list),
        "ssp_floors": defaultdict(list),
    }

    auction_bidders = defaultdict(lambda: set())

    ads_txt_entries = []
    if opts.get("ads_txt_check"):
        try:
            ads_txt_entries = parse_ads_txt(domain)
            logging.debug("ads.txt entries for %s: %s", domain, ads_txt_entries)
        except Exception:
            ads_txt_entries = []

    launch_args = {
        "headless": not opts.get("headful", False),
        "args": [
            "--no-sandbox",
            "--disable-setuid-sandbox",
            "--disable-dev-shm-usage",
            "--disable-gpu",
            "--disable-software-rasterizer",
            "--disable-extensions",
            "--disable-background-networking",
            "--disable-background-timer-throttling",
            "--disable-breakpad",
            "--disable-client-side-phishing-detection",
            "--disable-default-apps",
            "--disable-hang-monitor",
            "--disable-popup-blocking",
            "--disable-prompt-on-repost",
            "--disable-sync",
            "--metrics-recording-only",
            "--mute-audio",
        ],
    }

    ctx_args = {"ignore_https_errors": True}
    if proxy_url:
        launch_args["proxy"] = {"server": proxy_url}
    user_agent = (
        "Mozilla/5.0 (Linux; Android 11; Pixel 5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117 Mobile Safari/537.36"
        if mobile
        else "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117 Safari/537.36"
    )
    ctx_args["user_agent"] = user_agent

    browser = None
    context = None
    page = None
    start_time = time.time()
    timer = None
    watchdog_triggered = False

    def _kill_browser():
        nonlocal watchdog_triggered
        watchdog_triggered = True
        logging.warning("Watchdog triggered for %s — will close browser safely after run", safe)

    try:
        browser = playwright.chromium.launch(**launch_args)

        # watchdog adaptativo
        try:
            min_needed_ms = 0
            try:
                min_needed_ms = int(NAV_TIMEOUT_MS) + int(WAIT_AFTER_LOAD_MS)
            except Exception:
                min_needed_ms = int(NAV_TIMEOUT_MS or 0) + int(WAIT_AFTER_LOAD_MS or 0)

            ext_ms = 0
            if opts and opts.get("extended_wait"):
                try:
                    ext_ms = int(opts.get("extended_wait_ms", 0))
                except Exception:
                    ext_ms = 0
                ext_ms = min(90000, max(5000, ext_ms))

            buffer_ms = int(WAIT_AFTER_LOAD_MS) + ext_ms
            buffer_ms = max(15000, buffer_ms)

            desired_timeout_sec = (min_needed_ms + buffer_ms) / 1000.0
            base_global = GLOBAL_PAGE_RUN_TIMEOUT_SEC or 0
            timeout_to_use = max(base_global, desired_timeout_sec)
            try:
                timeout_to_use = min(timeout_to_use, float(GLOBAL_PAGE_RUN_TIMEOUT_MAX))
            except Exception:
                pass

            if timeout_to_use and float(timeout_to_use) > 0:
                timer = threading.Timer(float(timeout_to_use), _kill_browser)
                timer.daemon = True
                timer.start()
                logging.debug(
                    "Watchdog timer started (sec=%s) for %s (nav_ms=%s wait_ms=%s ext_ms=%s)",
                    timeout_to_use,
                    safe,
                    NAV_TIMEOUT_MS,
                    WAIT_AFTER_LOAD_MS,
                    opts.get("extended_wait_ms") if opts else None,
                )
        except Exception:
            logging.exception("Failed to start watchdog timer", exc_info=True)

        # device-aware context
        device = None
        try:
            if hasattr(playwright, "devices"):
                try:
                    device = playwright.devices.get("Pixel 5") if isinstance(playwright.devices, dict) else playwright.devices["Pixel 5"]
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
                context = browser.new_context(
                    user_agent=user_agent,
                    viewport={"width": 412, "height": 915},
                    is_mobile=True,
                    has_touch=True,
                    ignore_https_errors=True,
                )
        else:
            context = browser.new_context(
                user_agent=user_agent,
                viewport={"width": 1366, "height": 768},
                ignore_https_errors=True,
            )

        page = context.new_page()

        ad_strings = opts.get("ad_strings", DEFAULT_ADTECH_STRINGS)

        if opts.get("block_non_ad_resources"):
            def route_handler(route: Route, request: Request):
                try:
                    url_r = request.url
                    resource = request.resource_type
                    if resource in ("document", "xhr", "fetch", "script", "stylesheet"):
                        return route.continue_()
                    if is_adtech_url(url_r, ad_strings):
                        return route.continue_()
                    return route.abort()
                except Exception:
                    return route.continue_()

            page.route("**/*", route_handler)

        def record_pubmatic_request(req_obj, post_data, ts):
            url_r = req_obj.url
            low_r = (url_r or "").lower()
            is_pub = is_adtech_url(url_r, ["pubmatic", "pwt", "hb.pubmatic"])
            if not is_pub:
                return False, False, False
            is_bid = "hb.pubmatic" in low_r or "pwt" in low_r
            is_win = any(m in low_r for m in PUBMATIC_WIN_MARKERS)
            body = post_data or ""
            if body and isinstance(body, str):
                if '"type":"win"' in body or '"event":"win"' in body:
                    is_win = True
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
                    "method": getattr(r, "method", None),
                    "url": r.url,
                    "headers": _filter_headers(headers_val),
                    "post_data": _truncate_str(post, MAX_POSTDATA_LEN),
                    "resource_type": r.resource_type,
                }
                req_entries[key] = entry
                seq.append(key)
                low = (r.url or "").lower()

                if not flags["prebid"] and any(k in low for k in PREBID_MARKERS):
                    flags["prebid"] = True

                if is_adtech_url(r.url, ad_strings):
                    counters["adtech_requests"] += 1
                if any(s in low for s in ["pubmatic", "pwt", "hb.pubmatic"]):
                    counters["pub_adtech_requests"] += 1

                is_pub_detection = False
                try:
                    is_pub, is_bid, is_win = record_pubmatic_request(r, post, ts)
                    is_pub_detection = is_pub
                except Exception:
                    is_pub = is_bid = is_win = False

                if is_pub_detection:
                    req_entries[key]["is_pubmatic"] = True
                    req_entries[key]["is_pubmatic_bid_like"] = bool(is_bid)
                    req_entries[key]["is_pubmatic_win_like"] = bool(is_win)
                    if is_bid:
                        counters["pub_bids"] += 1
                    if is_win:
                        counters["pub_wins"] += 1

                aid = extract_query_param(
                    r.url,
                    [
                        "auctionId",
                        "auction_id",
                        "auctionIdEncoded",
                        "auid",
                        "tid",
                        "requestId",
                    ],
                )
                if not aid and post:
                    maybe = safe_json_load(post)
                    if isinstance(maybe, dict):
                        aid = (
                            maybe.get("auctionId")
                            or maybe.get("tid")
                            or maybe.get("id")
                        )
                if aid:
                    bidders = []
                    try:
                        if post:
                            body_json = safe_json_load(post)
                            if isinstance(body_json, dict):
                                if "bidders" in body_json:
                                    bidders = list(body_json.get("bidders") or [])
                                elif "bidderCodes" in body_json:
                                    bidders = body_json.get("bidderCodes") or []
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
                try:
                    status = resp.status
                except Exception:
                    status = None
                try:
                    rheaders = resp.headers or {}
                except Exception:
                    rheaders = {}

                if key in req_entries:
                    req_entries[key].update(
                        {
                            "response_ts": ts,
                            "status": status,
                            "response_headers_count": len(rheaders),
                        }
                    )
                else:
                    req_entries[key] = {
                        "request_ts": None,
                        "method": None,
                        "url": resp.url,
                        "headers": {},
                        "post_data": None,
                        "response_ts": ts,
                        "status": status,
                        "response_headers_count": len(rheaders),
                    }

                low = (resp.url or "").lower()
                is_pub = False
                if any(k in low for k in PUBMATIC_STRONG):
                    is_pub = True
                if key in req_entries and req_entries[key].get("is_pubmatic"):
                    is_pub = True
                if is_pub:
                    flags["pubmatic"] = True

                rentry = req_entries.get(key, {})
                if rentry.get("request_ts") and rentry.get("response_ts"):
                    lat_ms = int(
                        (rentry["response_ts"] - rentry["request_ts"]) * 1000
                    )
                    counters["latencies_ms"].append(lat_ms)

                body_text = None
                try:
                    body_text = resp.text()
                except Exception:
                    body_text = None

                price = None
                floor_val = None
                currency = None
                bidder_name = None

                body_json = None
                if body_text:
                    body_json = safe_json_load(body_text)

                try:
                    if isinstance(body_json, dict):
                        if "seatbid" in body_json:
                            for sb in body_json.get("seatbid", []):
                                for b in sb.get("bid", []):
                                    if "price" in b:
                                        price = float(b["price"])
                                        break
                                if price is not None:
                                    break
                        if "imp" in body_json:
                            for imp in body_json.get("imp", []):
                                if "bidfloor" in imp:
                                    floor_val = float(imp["bidfloor"])
                                    currency = imp.get("bidfloorcur") or currency
                                    break
                        if not currency and "cur" in body_json:
                            if isinstance(body_json["cur"], list) and body_json["cur"]:
                                currency = body_json["cur"][0]
                            elif isinstance(body_json["cur"], str):
                                currency = body_json["cur"]
                except Exception:
                    pass

                if price is None:
                    hb_pb = extract_query_param(
                        resp.url,
                        ["hb_pb", "hb_pb_pubmatic", "hb_pb_cat_dur"],
                    )
                    if hb_pb:
                        try:
                            price = float(hb_pb)
                        except Exception:
                            price = None

                if not bidder_name:
                    bidder_name = extract_query_param(
                        resp.url,
                        ["hb_bidder", "bidder", "ssp"],
                    )

                ssp_name = classify_ssp_from_url(resp.url)
                if not bidder_name and ssp_name:
                    bidder_name = ssp_name

                if bidder_name or ssp_name:
                    key_ssp = bidder_name or ssp_name
                    if is_adtech_url(resp.url, ad_strings):
                        counters["ssp_bids"][key_ssp] += 1

                is_win = False
                if any(m in low for m in PUBMATIC_WIN_MARKERS):
                    is_win = True

                if (bidder_name or ssp_name) and is_win:
                    key_ssp = bidder_name or ssp_name
                    counters["ssp_wins"][key_ssp] += 1
                    if price is not None:
                        counters["ssp_prices"][key_ssp].append(price)
                    if floor_val is not None:
                        counters["ssp_floors"][key_ssp].append(floor_val)

                ctype = (rheaders.get("content-type") or "").lower()
                if "video" in ctype or any(
                    ext in (resp.url or "").lower()
                    for ext in (".mp4", ".m3u8", ".ts")
                ):
                    counters["formats"]["video"] += 1
                else:
                    if body_text and re.search(r"(\d{2,4})x(\d{2,4})", body_text):
                        counters["formats"]["display"] += 1
                    else:
                        counters["formats"]["display"] += 1

                did = extract_query_param(
                    resp.url, ["dealid", "deal_id", "dealId", "pmp"]
                )
                if not did and body_text:
                    m = re.search(r'"dealid"\s*:\s*"([^"]+)"', body_text)
                    if m:
                        did = m.group(1)
                if did:
                    counters["dealids"][did] += 1

                sch = extract_query_param(resp.url, ["schain", "sch"])
                if not sch and body_text:
                    m = re.search(r'"schain"\s*:\s*(\{[^}]+\})', body_text)
                    if m:
                        nodes = re.findall(r'"asi"\s*:', m.group(1))
                        counters["schain_hops"].append(len(nodes) or 1)

                if body_text:
                    low_body = body_text.lower()
                    if "id5" in low_body:
                        counters["id_solutions"]["id5"] += 1
                    if "uid2" in low_body:
                        counters["id_solutions"]["uid2"] += 1
                    if "identity" in low_body and "pubmatic" in low_body:
                        counters["id_solutions"]["pubmatic_identity"] += 1

                pubid = extract_query_param(
                    resp.url,
                    ["pubid", "pubId", "account", "publisher_id", "pmid"],
                )
                if not pubid and body_text:
                    m = re.search(r'pubId["\']?\s*[:=]\s*["\']?(\d+)', body_text)
                    if m:
                        pubid = m.group(1)

                if is_win or rentry.get("is_pubmatic_win_like"):
                    counters["pub_wins"] += 1
                    if opts.get("ads_txt_check") and pubid:
                        direct_found = False
                        for a in ads_txt_entries:
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
                            counters["reseller_wins"] += 0

                aid = extract_query_param(
                    resp.url,
                    ["auctionId", "auction_id", "tid", "auid", "requestId"],
                )
                if aid:
                    bcount = len(auction_bidders.get(aid, []))
                    if bcount:
                        counters["bidder_counts"].append(bcount)

                refresh_threshold_sec = (WAIT_AFTER_LOAD_MS / 1000.0) + 5.0
                if rentry.get("response_ts") and (
                    rentry.get("response_ts") - start_time
                ) > refresh_threshold_sec:
                    counters["refresh_wins"] += 1

                if not flags["prebid"]:
                    try:
                        btext = body_text
                        if btext and any(k in btext.lower() for k in PREBID_MARKERS):
                            flags["prebid"] = True
                    except Exception:
                        pass
            except Exception:
                logging.debug("on_response exception", exc_info=True)

        page.on("request", on_request)
        page.on("response", on_response)

        def human_simulate_scroll_and_mouse(page_obj, viewport_height, simulate_extra=False):
            try:
                steps = 4 + random.randint(0, 3)
                for i in range(steps):
                    frac = (i + 1) / steps
                    y = int(viewport_height * frac * (0.6 + random.random() * 0.8))
                    try:
                        page_obj.evaluate(
                            "window.scrollTo({left: 0, top: %d, behavior: 'smooth'})" % y
                        )
                    except Exception:
                        try:
                            page_obj.evaluate("window.scrollTo(0, %d)" % y)
                        except Exception:
                            pass
                    try:
                        page_obj.mouse.move(
                            random.randint(10, 300), random.randint(10, 400)
                        )
                    except Exception:
                        pass
                    time.sleep(0.6 + random.random() * 0.8)

                if simulate_extra:
                    try:
                        page_obj.mouse.click(
                            random.randint(10, 300), random.randint(10, 400)
                        )
                    except Exception:
                        pass
            except Exception:
                pass

        try:
            page.goto(url, timeout=NAV_TIMEOUT_MS)
        except PlaywrightTimeoutError:
            logging.warning("Navigation timeout for url=%s", url)
        except Exception as e:
            logging.warning("Navigation exception %s", e)

        if GLOBAL_PAGE_RUN_TIMEOUT_SEC and (time.time() - start_time) > GLOBAL_PAGE_RUN_TIMEOUT_SEC:
            logging.warning(
                "Global run timeout exceeded after navigation, ending early for %s",
                url,
            )
        else:
            with suppress(Exception):
                page.wait_for_load_state(
                    "networkidle",
                    timeout=min(NAV_TIMEOUT_MS, NAV_TIMEOUT_MS),
                )

            if opts.get("simulate_human"):
                try:
                    vp = page.viewport_size or {"width": 1366, "height": 768}
                    human_simulate_scroll_and_mouse(
                        page, vp.get("height", 768), simulate_extra=True
                    )
                except Exception:
                    pass

            if watchdog_triggered:
                logging.warning(
                    "Watchdog triggered — skipping waits and finishing run early for %s",
                    url,
                )
            else:
                if not (
                    GLOBAL_PAGE_RUN_TIMEOUT_SEC
                    and (time.time() - start_time) > GLOBAL_PAGE_RUN_TIMEOUT_SEC
                ):
                    try:
                        page.wait_for_timeout(WAIT_AFTER_LOAD_MS)
                    except Exception:
                        pass

                if opts.get("extended_wait"):
                    try:
                        ew_ms = opts.get("extended_wait_ms", 90000)
                        page.wait_for_timeout(min(90000, max(5000, ew_ms)))
                    except Exception:
                        pass

    except Exception as e:
        logging.exception("Error during capture: %s", e)
    finally:
        with suppress(Exception):
            if timer:
                timer.cancel()
        with suppress(Exception):
            if context and not context.is_closed():
                context.close()
        with suppress(Exception):
            if browser and browser.is_connected():
                browser.close()

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
        "entries": compact_list,
    }

    try:
        with open(mini_har_path, "w", encoding="utf-8") as mf:
            json.dump(mini, mf)
    except Exception:
        logging.exception("Failed to write mini-har to %s", mini_har_path)

    total = len(compact_list)
    pub_hits_list = [
        u
        for u in (e.get("url", "") for e in compact_list)
        if any(k in (u or "").lower() for k in PUBMATIC_STRONG)
    ]
    adtech_total = counters["adtech_requests"] or 0
    pub_adtech = counters["pub_adtech_requests"] or 0
    pub_adtech_share = (pub_adtech / adtech_total) if adtech_total else 0.0

    bids = counters["pub_bids"]
    wins = counters["pub_wins"]
    win_rate = (wins / bids) if bids else None

    lat_list = counters["latencies_ms"]
    avg_lat = (sum(lat_list) / len(lat_list)) if lat_list else None
    p95_lat = None
    if lat_list:
        lat_sorted = sorted(lat_list)
        idx = int(len(lat_sorted) * 0.95) - 1
        idx = max(0, min(len(lat_sorted) - 1, idx))
        p95_lat = lat_sorted[idx]

    bc = counters["bidder_counts"]
    bidder_avg = (sum(bc) / len(bc)) if bc else None

    ssp_financials = {}
    for ssp, nbids in counters["ssp_bids"].items():
        nwins = counters["ssp_wins"].get(ssp, 0)
        prices = counters["ssp_prices"].get(ssp, [])
        floors = counters["ssp_floors"].get(ssp, [])
        avg_price = (sum(prices) / len(prices)) if prices else None
        avg_floor = (sum(floors) / len(floors)) if floors else None
        win_rate_ssp = (nwins / nbids) if nbids else None
        ssp_financials[ssp] = {
            "bids": nbids,
            "wins": nwins,
            "win_rate": win_rate_ssp,
            "avg_cpm": avg_price,
            "avg_floor": avg_floor,
        }

    total_wins_all = sum(counters["ssp_wins"].values()) or 0
    ssp_share_of_voice = {}
    if total_wins_all > 0:
        for ssp, nwins in counters["ssp_wins"].items():
            ssp_share_of_voice[ssp] = nwins / total_wins_all

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
        "ssp_financials": ssp_financials,
        "ssp_share_of_voice": ssp_share_of_voice,
    }

    logging.info(
        "Run finished %s pub_wins=%s pub_bids=%s pub_adtech_share=%.4f win_rate=%s",
        safe,
        summary["pub_wins"],
        summary["pub_bids"],
        summary["pubmatic_adtech_share"],
        str(summary["pub_win_rate"]),
    )
    return summary


def run_page_aggregated(
    playwright,
    url,
    outdir,
    domain,
    page_label,
    geo,
    proxy_url,
    mobile,
    iteration,
    n_runs,
    NAV_TIMEOUT_MS,
    WAIT_AFTER_LOAD_MS,
    GLOBAL_PAGE_RUN_TIMEOUT_SEC,
    opts,
):
    runs = []
    for r in range(1, n_runs + 1):
        runs.append(
            capture_single_run(
                playwright,
                url,
                outdir,
                domain,
                page_label,
                geo,
                proxy_url,
                mobile,
                iteration,
                r,
                NAV_TIMEOUT_MS,
                WAIT_AFTER_LOAD_MS,
                GLOBAL_PAGE_RUN_TIMEOUT_SEC,
                opts,
            )
        )
        time.sleep(1)

    avg_total = sum(x["total_requests"] for x in runs) / len(runs) if runs else 0
    avg_pub = sum(x["pubmatic_requests"] for x in runs) / len(runs) if runs else 0
    any_prebid = any(x.get("prebid_detected") for x in runs)
    any_pubmatic = any(x.get("pubmatic_detected") for x in runs)
    wins = sum(x.get("pub_wins", 0) for x in runs)
    bids = sum(x.get("pub_bids", 0) for x in runs)
    avg_win_rate = (wins / bids) if bids else None

    return {
        "domain": domain,
        "page_label": page_label,
        "geo": geo,
        "iteration": iteration,
        "avg_total_requests": avg_total,
        "avg_pubmatic_requests": avg_pub,
        "runs": runs,
        "prebid_detected": any_prebid,
        "pubmatic_detected": any_pubmatic,
        "avg_win_rate": avg_win_rate,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--targets", default="targets.json")
    parser.add_argument("--proxies", default=DEFAULT_PROXY_FILE)
    parser.add_argument("--outdir", default=DEFAULT_OUTDIR)
    parser.add_argument(
        "--iteration",
        type=int,
        default=0,
        help="slot index 0 .. TOTAL_DAILY_SLOTS-1",
    )
    parser.add_argument("--geo", default="US")
    parser.add_argument(
        "--slots",
        type=int,
        default=None,
        help="override TOTAL_DAILY_SLOTS (env) for this run",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="ignore slotting and run all publishers",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="enable debug logging",
    )
    parser.add_argument(
        "--headful",
        action="store_true",
        help="run browser visible (headful)",
    )
    parser.add_argument(
        "--simulate-human",
        action="store_true",
        help="simulate human interactions (scroll, mouse)",
    )
    parser.add_argument(
        "--block-non-ad-resources",
        action="store_true",
        help="abort loading of non-ad resources",
    )
    parser.add_argument(
        "--ads-txt-check",
        action="store_true",
        help="perform ads.txt check",
    )
    parser.add_argument(
        "--extended-wait",
        action="store_true",
        help="wait longer to capture ad refreshes",
    )
    parser.add_argument(
        "--extended-wait-ms",
        type=int,
        default=300000,
        help="ms to wait when --extended-wait (default 300000ms)",
    )
    parser.add_argument(
        "--ad-domains-file",
        default=None,
        help="optional file with ad-tech substrings, one per line",
    )
    parser.add_argument(
        "--config",
        type=str,
        default="targets.json",
        help="Path to targets.json (default: targets.json)",
    )
    parser.add_argument(
        "--publisher",
        type=str,
        help="Run only a specific publisher (domain name)",
    )

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    targets = load_json(args.config)
    proxies = load_json(args.proxies)
    ensure_dir(args.outdir)

    from datetime import datetime as dt

    today = dt.utcnow().strftime("%Y-%m-%d")
    day_root = os.path.join(args.outdir, today)
    ensure_dir(day_root)

    ts = timestamp_str()
    run_root = os.path.join(day_root, ts)
    ensure_dir(run_root)

    ad_strings = DEFAULT_ADTECH_STRINGS[:]
    if args.ad_domains_file and os.path.exists(args.ad_domains_file):
        try:
            with open(args.ad_domains_file, "r", encoding="utf-8") as af:
                extra = [l.strip() for l in af if l.strip()]
                ad_strings.extend(extra)
        except Exception:
            logging.debug("Failed loading ad domains file", exc_info=True)

    pubs = targets.get("publishers", [])
    if args.publisher:
        pubs = [p for p in pubs if p.get("domain") == args.publisher]
        if not pubs:
            raise ValueError(f"Publisher '{args.publisher}' não encontrado no targets.json")

    total_slots = args.slots if args.slots is not None else TOTAL_DAILY_SLOTS
    if total_slots < 1:
        total_slots = TOTAL_DAILY_SLOTS

    pub_to_slots = allocate_slots(pubs, total_slots)

    slot_map_path = os.path.join(run_root, "slot_map.json")
    with open(slot_map_path, "w", encoding="utf-8") as f:
        json.dump(pub_to_slots, f, indent=2)
    try:
        latest_map_path = os.path.join(args.outdir, "slot_map_latest.json")
        with open(latest_map_path, "w", encoding="utf-8") as f2:
            json.dump({"total_slots": total_slots, "mapping": pub_to_slots}, f2, indent=2)
    except Exception:
        logging.debug("Failed writing slot_map_latest.json", exc_info=True)

    slot_weights = {i: 0.0 for i in range(total_slots)}
    slot_pages = {i: 0 for i in range(total_slots)}
    name_to_pub = {p["domain"]: p for p in pubs}
    for name, slots in pub_to_slots.items():
        w = float(name_to_pub.get(name, {}).get("weight_pct", 0.0))
        pages_count = len(name_to_pub.get(name, {}).get("pages", []))
        for s in slots:
            slot_weights[s] = slot_weights.get(s, 0.0) + w
            slot_pages[s] = slot_pages.get(s, 0) + pages_count

    logging.info("Slot weights summary (slot -> weight, pages): %s", {
        s: {"weight": slot_weights[s], "pages": slot_pages[s]} for s in range(total_slots)
    })

    iteration = args.iteration
    selected_publishers = []

    if args.publisher:
        selected_publishers = pubs
    elif args.all:
        selected_publishers = pubs
    else:
        for p in pubs:
            slots = pub_to_slots.get(p["domain"], [])
            if iteration in slots:
                selected_publishers.append(p)

    num_pages = sum(len(p.get("pages", [])) for p in selected_publishers)
    n_runs, nav_ms, wait_ms, global_timeout_sec = compute_timeouts_and_runs(num_pages)

    logging.info(
        "Computed n_runs=%s nav_ms=%s wait_ms=%s global_timeout_sec=%s for %s pages",
        n_runs,
        nav_ms,
        wait_ms,
        global_timeout_sec,
        num_pages,
    )

    opts = {
        "headful": args.headful,
        "simulate_human": args.simulate_human,
        "block_non_ad_resources": args.block_non_ad_resources,
        "ads_txt_check": args.ads_txt_check,
        "extended_wait": args.extended_wait,
        "extended_wait_ms": args.extended_wait_ms,
        "ad_strings": ad_strings,
    }

    results = []

    with sync_playwright() as pw:
        for pub in selected_publishers:
            name = pub["domain"]
            pages = pub.get("pages", [])
            proxy_geo = args.geo
            proxy_url = None

            if proxies and proxy_geo in proxies:
                proxy_url = proxies[proxy_geo]
            elif proxies and "default" in proxies:
                proxy_url = proxies["default"]

            for page_def in pages:
                page_url = page_def["url"]
                page_label = page_def.get("label", page_url)
                mobile = bool(page_def.get("mobile", False))

                logging.info(
                    "Running %s (%s) geo=%s proxy=%s mobile=%s",
                    name,
                    page_label,
                    proxy_geo,
                    proxy_url,
                    mobile,
                )

                outdir_pub = os.path.join(run_root, sanitize(name))
                ensure_dir(outdir_pub)

                agg = run_page_aggregated(
                    pw,
                    page_url,
                    outdir_pub,
                    name,
                    page_label,
                    proxy_geo,
                    proxy_url,
                    mobile,
                    iteration,
                    n_runs,
                    nav_ms,
                    wait_ms,
                    global_timeout_sec,
                    opts,
                )
                results.append(agg)

    summary_path = os.path.join(run_root, "run_summary.json")

    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)

    try:
        flat_rows = []
        for agg in results:
            domain = agg.get("domain")
            page_label = agg.get("page_label")
            geo = agg.get("geo")
            iteration = agg.get("iteration")
            for run in agg.get("runs", []):
                row = {
                    "domain": domain,
                    "page_label": page_label,
                    "geo": geo,
                    "iteration": iteration,
                    "timestamp": run.get("timestamp"),
                    "total_requests": run.get("total_requests"),
                    "pubmatic_requests": run.get("pubmatic_requests"),
                    "pubmatic_adtech_share": run.get("pubmatic_adtech_share"),
                    "pub_bids": run.get("pub_bids"),
                    "pub_wins": run.get("pub_wins"),
                    "pub_win_rate": run.get("pub_win_rate"),
                    "avg_bid_latency_ms": run.get("avg_bid_latency_ms"),
                    "p95_bid_latency_ms": run.get("p95_bid_latency_ms"),
                    "bidder_count_avg": run.get("bidder_count_avg"),
                    "direct_wins": run.get("direct_wins"),
                    "reseller_wins": run.get("reseller_wins"),
                    "refresh_wins": run.get("refresh_wins"),
                    "ssp_financials": run.get("ssp_financials"),
                    "ssp_share_of_voice": run.get("ssp_share_of_voice"),
                }
                flat_rows.append(row)

        if flat_rows:
            df = pd.DataFrame(flat_rows)
            xlsx_path = summary_path.replace(".json", ".xlsx")
            df.to_excel(xlsx_path, index=False)
            logging.info(
                "Run complete. Summary written to %s and %s",
                summary_path,
                xlsx_path,
            )
        else:
            logging.info("Run complete. Summary written to %s (no runs to export to Excel)", summary_path)
    except Exception:
        logging.exception("Failed to write Excel summary for %s", summary_path)


if __name__ == "__main__":
    main()

