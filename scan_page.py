#!/usr/bin/env python3
"""
scan_page.py — weighted-slot scanner (adaptative timing) with compact "mini-HAR" output

Changes from previous:
- removed Playwright record_har_path (no full HARs)
- each run produces a compact JSON "mini-har" containing only essential fields per request/response:
    - request: timestamp, method, url, selected headers (truncated), post_data (truncated)
    - response: timestamp, status, selected headers-count
  This drastically reduces disk usage while keeping the data you need to debug/measure pubmatic hits.
- retains summary JSON per run and an aggregated CSV and meta JSON per iteration.

Usage:
    python3 scan_page.py --targets targets.json --iteration 0 --geo US --outdir output
"""

import os
import sys
import json
import time
import argparse
import logging
from datetime import datetime
from pathlib import Path
from math import floor
from contextlib import suppress

try:
    from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
except Exception as e:
    print("FATAL: playwright not available. Run inside Playwright-enabled environment (docker or preinstalled). Error:", e, file=sys.stderr)
    sys.exit(3)

# ---------------- CONFIG (tunable via env) ----------------
TOTAL_DAILY_SLOTS = int(os.environ.get("TOTAL_DAILY_SLOTS", "6"))
# nominal runs per page (will be adjusted down if many pages to keep iteration approx target minutes)
BASE_N_RUNS_PER_PAGE = int(os.environ.get("BASE_N_RUNS_PER_PAGE", "1"))
# target minutes per iteration (aim). We'll allow +/- FLEX_MINUTES
TARGET_ITERATION_MINUTES = int(os.environ.get("TARGET_ITERATION_MINUTES", "60"))
FLEX_MINUTES = int(os.environ.get("FLEX_MINUTES", "15"))

# fallback min/max timeouts (seconds)
NAV_TIMEOUT_MIN = float(os.environ.get("NAV_TIMEOUT_MIN", "3"))    # lower bound for nav timeout (s)
NAV_TIMEOUT_MAX = float(os.environ.get("NAV_TIMEOUT_MAX", "12"))   # upper bound (s)
WAIT_AFTER_LOAD_MIN = float(os.environ.get("WAIT_AFTER_LOAD_MIN", "1"))   # sec
WAIT_AFTER_LOAD_MAX = float(os.environ.get("WAIT_AFTER_LOAD_MAX", "8"))   # sec
GLOBAL_PAGE_RUN_TIMEOUT_MIN = float(os.environ.get("GLOBAL_PAGE_RUN_TIMEOUT_MIN", "8"))  # sec
GLOBAL_PAGE_RUN_TIMEOUT_MAX = float(os.environ.get("GLOBAL_PAGE_RUN_TIMEOUT_MAX", "30")) # sec

DEFAULT_OUTDIR = os.environ.get("OUTDIR", "output")
DEFAULT_PROXY_FILE = os.environ.get("PROXIES_FILE", "country_proxies.json")
# keyword detection (improved)
PUBMATIC_STRONG = ["pubmatic", "ads.pubmatic", "pubmatic.com", "hb.pubmatic"]
PREBID_MARKERS = ["pbjs", "prebid", "prebid.js"]
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
# limits for captured strings
MAX_HEADER_VALUE_LEN = int(os.environ.get("MAX_HEADER_VALUE_LEN", "200"))
MAX_POSTDATA_LEN = int(os.environ.get("MAX_POSTDATA_LEN", "200"))

# ---------------------------------------------------------

logging.basicConfig(level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
                    format="%(asctime)s %(levelname)s %(message)s")


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


# ---------- slot allocation (same algorithm you had) ----------
def allocate_slots(publishers, total_slots):
    rows = []
    for p in publishers:
        w = float(p.get("weight_pct", 0.0))
        raw = w / 100.0 * total_slots
        initial = int(floor(raw))
        rem = raw - initial
        rows.append({"name": p["name"], "initial": initial, "remainder": rem, "raw": raw, "obj": p})
    sum_initial = sum(r["initial"] for r in rows)
    remaining = total_slots - sum_initial
    rows_sorted = sorted(rows, key=lambda r: r["remainder"], reverse=True)
    i = 0
    while remaining > 0 and i < len(rows_sorted):
        rows_sorted[i]["initial"] += 1
        remaining -= 1
        i += 1
        if i == len(rows_sorted):
            i = 0
    # distribute counts across slots fairly
    import heapq
    slot_buckets = [[] for _ in range(total_slots)]
    heap = [(0, idx) for idx in range(total_slots)]
    heapq.heapify(heap)
    pub_counts = [(r["name"], r["initial"]) for r in rows_sorted]
    for name, count in pub_counts:
        for _ in range(count):
            size, slot = heapq.heappop(heap)
            slot_buckets[slot].append(name)
            heapq.heappush(heap, (size + 1, slot))
    pub_to_slots = {}
    for slot_idx, bucket in enumerate(slot_buckets):
        for name in bucket:
            pub_to_slots.setdefault(name, []).append(slot_idx)
    return pub_to_slots


# ---------- dynamic timing helper ----------
def compute_timeouts_and_runs(num_pages, target_minutes=TARGET_ITERATION_MINUTES, flex_minutes=FLEX_MINUTES):
    """
    Given number of pages (desktop+mobile) to run in this iteration, compute:
      - n_runs_per_page (int)
      - NAV_TIMEOUT_MS, WAIT_AFTER_LOAD_MS, GLOBAL_PAGE_RUN_TIMEOUT_SEC
    Strategy (leia de forma leiga):
      - We want total iteration wall-time ≈ target_minutes (± flex)
      - total_seconds_budget = target_minutes * 60
      - per_page_budget = total_seconds_budget / max(1, num_pages)
      - NAV timeout, wait_after_load and safety are fractions of per_page_budget with min/max clamps.
      - If per_page_budget is tiny (many pages), we lower N_RUNS_PER_PAGE to 1.
    Returns values in seconds (timeouts) and n_runs_per_page.
    """
    if num_pages <= 0:
        num_pages = 1
    budget_seconds = target_minutes * 60
    per_page_budget = budget_seconds / num_pages
    # determine runs: if per_page_budget < 20 sec -> 1 run; else use base
    if per_page_budget < 20:
        n_runs = 1
    else:
        n_runs = BASE_N_RUNS_PER_PAGE
    # nav timeout approx 40% of per_page_budget but clamped
    nav = max(NAV_TIMEOUT_MIN, min(NAV_TIMEOUT_MAX, per_page_budget * 0.40))
    wait = max(WAIT_AFTER_LOAD_MIN, min(WAIT_AFTER_LOAD_MAX, per_page_budget * 0.20))
    global_run_timeout = max(GLOBAL_PAGE_RUN_TIMEOUT_MIN, min(GLOBAL_PAGE_RUN_TIMEOUT_MAX, per_page_budget * 0.7))
    # convert to ms where needed
    return int(n_runs), int(nav * 1000), int(wait * 1000), int(global_run_timeout)


# ---------- compact capture helper ----------
def _truncate_str(s, length):
    if s is None:
        return None
    s = str(s)
    if len(s) > length:
        return s[:length] + "...(truncated)"
    return s


def _filter_headers(headers):
    """
    Keep a small set of headers that are useful for debugging and
    truncate values so per-entry size stays small.
    """
    keep = {}
    if not headers:
        return keep
    for k, v in headers.items():
        kl = k.lower()
        if kl in ("content-type", "user-agent", "referer", "origin", "accept", "x-forwarded-for"):
            keep[k] = _truncate_str(v, MAX_HEADER_VALUE_LEN)
    return keep


# ---------- capture logic (improved detection flags) ----------
def capture_single_run(playwright, url, outdir, domain, page_label, geo, proxy_url, mobile, iteration, run_idx,
                       NAV_TIMEOUT_MS, WAIT_AFTER_LOAD_MS, GLOBAL_PAGE_RUN_TIMEOUT_SEC):
    """
    Capture one run: do navigation and record a compact per-request mini-har JSON + summary JSON.
    """
    run_ts = timestamp_str()
    safe = sanitize(f"{domain}_{page_label}_{geo}_iter{iteration}_run{run_idx}_{run_ts}")
    mini_har_path = os.path.join(outdir, f"{safe}.minihar.json")
    summary_file = os.path.join(outdir, f"{safe}.json")

    # data structures: map request_id -> entry
    req_entries = {}  # key: id(request_object) -> dict
    seq = []  # maintain order of request keys for final array

    flags = {"prebid": False, "pubmatic": False}
    collected_urls = []  # list of all request urls (lowercase)

    # launch/playwright context
    launch_args = {"headless": True, "args": ["--no-sandbox", "--disable-setuid-sandbox"]}
    ctx_args = {"ignore_https_errors": True}
    if proxy_url:
        launch_args["proxy"] = {"server": proxy_url}
    user_agent = "Mozilla/5.0 (Linux; Android 11; Pixel 5)..." if mobile else "Mozilla/5.0 (Windows NT 10.0; Win64; x64)..."
    ctx_args["user_agent"] = user_agent

    browser = None
    context = None
    page = None
    start_time = time.time()

    try:
        browser = playwright.chromium.launch(**launch_args)
        context = browser.new_context(**ctx_args)
        page = context.new_page()

        # Request event
        def on_request(r):
            try:
                key = id(r)
                ts = time.time()
                # attempt to get post data - Playwright request has post_data() method
                try:
                    post = r.post_data or r.post_data()
                except Exception:
                    post = None
                entry = {
                    "request_ts": ts,
                    "method": getattr(r, "method", None) or (r._impl_obj.method if hasattr(r, "_impl_obj") else None),
                    "url": r.url,
                    "headers": _filter_headers(getattr(r, "headers", lambda: {})() if callable(getattr(r, "headers", None)) else getattr(r, "headers", {})),
                    "post_data": _truncate_str(post, MAX_POSTDATA_LEN)
                }
                req_entries[key] = entry
                seq.append(key)
                low = r.url.lower() if r.url else ""
                collected_urls.append(low)
                if not flags["prebid"] and any(k in low for k in PREBID_MARKERS):
                    flags["prebid"] = True
                if not flags["pubmatic"] and any(k in low for k in PUBMATIC_STRONG):
                    flags["pubmatic"] = True
            except Exception:
                # don't fail capture on request parsing
                logging.debug("on_request exception", exc_info=True)

        # Response event
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
                # minimal response headers info (counts)
                try:
                    rheaders = resp.headers or {}
                except Exception:
                    rheaders = {}
                # attach to entry if present
                if key in req_entries:
                    req_entries[key].update({
                        "response_ts": ts,
                        "status": status,
                        "response_headers_count": len(rheaders)
                    })
                else:
                    # sometimes responses come for preflight or requests we didn't record; store minimally
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
                if not flags["pubmatic"] and any(k in low for k in PUBMATIC_STRONG):
                    flags["pubmatic"] = True
                if not flags["prebid"] and any(k in low for k in PREBID_MARKERS):
                    flags["prebid"] = True
            except Exception:
                logging.debug("on_response exception", exc_info=True)

        page.on("request", on_request)
        page.on("response", on_response)

        # navigation
        try:
            page.goto(url, timeout=NAV_TIMEOUT_MS)
        except PlaywrightTimeoutError:
            logging.warning("Navigation timeout for url=%s", url)
        except Exception as e:
            logging.warning("Navigation exception %s", e)

        # wait for networkidle best-effort
        with suppress(Exception):
            page.wait_for_load_state("networkidle", timeout=min(PAGE_NETWORK_IDLE_TIMEOUT_MS, NAV_TIMEOUT_MS))

        # extra short wait for late calls
        try:
            page.wait_for_timeout(WAIT_AFTER_LOAD_MS)
        except Exception:
            pass

    except Exception as e:
        logging.exception("Error during capture: %s", e)
    finally:
        # ensure close context (no HAR auto-write)
        with suppress(Exception):
            if context:
                context.close()
        with suppress(Exception):
            if browser:
                browser.close()

    # Build compact list in original order
    compact_list = []
    for key in seq:
        ent = req_entries.get(key, {})
        compact = {
            "request_ts": ent.get("request_ts"),
            "method": ent.get("method"),
            "url": ent.get("url"),
            "req_headers": ent.get("headers", {}),
            "post_data": ent.get("post_data"),
            "response_ts": ent.get("response_ts"),
            "status": ent.get("status"),
            "response_headers_count": ent.get("response_headers_count")
        }
        compact_list.append(compact)

    # write mini-har
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

    # finalize summary
    total = len(compact_list)
    pub_hits = [u for u in (e.get("url", "") for e in compact_list) if any(k in (u or "").lower() for k in PUBMATIC_STRONG)]
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
        "pubmatic_requests": len(pub_hits),
        "pubmatic_sample": pub_hits[:10],
        "proxy_used": proxy_url or "",
        "user_agent": user_agent,
        "prebid_detected": flags["prebid"],
        "pubmatic_detected": flags["pubmatic"],
    }

    try:
        with open(summary_file, "w", encoding="utf-8") as sf:
            json.dump(summary, sf, indent=2)
    except Exception:
        logging.exception("Failed to write summary json to %s", summary_file)

    logging.info("Run finished %s pubmatic_hits=%s total_requests=%s prebid=%s pubmatic=%s", safe,
                 summary["pubmatic_requests"], total, flags["prebid"], flags["pubmatic"])
    return summary


# aggregated per page
def run_page_aggregated(playwright, url, outdir, domain, page_label, geo, proxy_url, mobile, iteration,
                        n_runs, NAV_TIMEOUT_MS, WAIT_AFTER_LOAD_MS, GLOBAL_PAGE_RUN_TIMEOUT_SEC):
    runs = []
    for r in range(1, n_runs + 1):
        runs.append(capture_single_run(playwright, url, outdir, domain, page_label, geo, proxy_url, mobile, iteration, r,
                                       NAV_TIMEOUT_MS, WAIT_AFTER_LOAD_MS, GLOBAL_PAGE_RUN_TIMEOUT_SEC))
        # short pause to vary cache behavior
        time.sleep(1)
    avg_total = sum(x["total_requests"] for x in runs) / len(runs) if runs else 0
    avg_pub = sum(x["pubmatic_requests"] for x in runs) / len(runs) if runs else 0
    any_prebid = any(x.get("prebid_detected") for x in runs)
    any_pubmatic = any(x.get("pubmatic_detected") for x in runs)
    return {"domain": domain, "page_label": page_label, "geo": geo, "iteration": iteration,
            "avg_total_requests": avg_total, "avg_pubmatic_requests": avg_pub, "runs": runs,
            "prebid_detected": any_prebid, "pubmatic_detected": any_pubmatic}


# ---------------- main ----------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--targets", default="targets.json")
    parser.add_argument("--proxies", default=DEFAULT_PROXY_FILE)
    parser.add_argument("--outdir", default=DEFAULT_OUTDIR)
    parser.add_argument("--iteration", type=int, default=0, help="slot index 0..TOTAL_DAILY_SLOTS-1")
    parser.add_argument("--geo", default="US")
    args = parser.parse_args()

    targets = load_json(args.targets)
    proxies = load_json(args.proxies)
    ensure_dir(args.outdir)
    timestamp = timestamp_str()
    run_root = os.path.join(args.outdir, timestamp)
    ensure_dir(run_root)

    pubs = targets.get("publishers", [])
    pub_to_slots = allocate_slots(pubs, TOTAL_DAILY_SLOTS)
    # save mapping
    with open(os.path.join(run_root, "slot_map.json"), "w", encoding="utf-8") as f:
        json.dump(pub_to_slots, f, indent=2)

    selected = [p for p in pubs if p["name"] in pub_to_slots and args.iteration in pub_to_slots[p["name"]]]
    logging.info("Iteration %s selected publishers: %s", args.iteration, [p["name"] for p in selected])

    # build list of pages (desktop+mobile) count to compute budgets
    page_entries = []
    for pub in selected:
        for pg in pub.get("pages", []):
            if not pg.get("url"):
                continue
            page_entries.append((pub, pg, False))  # desktop
            page_entries.append((pub, pg, True))   # mobile
    total_pages = len(page_entries)
    # compute dynamic n_runs and timeouts
    n_runs_per_page, NAV_MS, WAIT_MS, GLOBAL_RUN_SEC = compute_timeouts_and_runs(total_pages,
                                                                                 target_minutes=TARGET_ITERATION_MINUTES,
                                                                                 flex_minutes=FLEX_MINUTES)
    logging.info("Total pages this iteration: %s -> n_runs_per_page=%s NAV_MS=%sms WAIT_MS=%sms GLOBAL_PAGE_RUN_TIMEOUT_SEC=%ss",
                 total_pages, n_runs_per_page, NAV_MS, WAIT_MS, GLOBAL_RUN_SEC)

    csv_rows = []
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
                                         country, proxy_url, mobile, args.iteration, n_runs_per_page, NAV_MS, WAIT_MS, GLOBAL_RUN_SEC)
                avg_total = agg["avg_total_requests"]
                avg_pub = agg["avg_pubmatic_requests"]
                pubmatic_rate = (avg_pub / avg_total) if avg_total > 0 else 0.0
                csv_rows.append({"domain": agg["domain"], "page_label": agg["page_label"], "url": url, "geo": country,
                                 "iteration": args.iteration, "avg_total_requests": avg_total,
                                 "avg_pubmatic_requests": avg_pub, "pubmatic_rate": round(pubmatic_rate, 6),
                                 "runs_count": len(agg["runs"]), "prebid_detected": agg.get("prebid_detected", False),
                                 "pubmatic_detected": agg.get("pubmatic_detected", False)})

    # write csv
    import csv as _csv
    csv_path = os.path.join(run_root, f"summary_iteration_{args.iteration}.csv")
    fields = ["domain", "page_label", "url", "geo", "iteration", "avg_total_requests", "avg_pubmatic_requests", "pubmatic_rate", "runs_count", "prebid_detected", "pubmatic_detected"]
    with open(csv_path, "w", newline="", encoding="utf-8") as cf:
        w = _csv.DictWriter(cf, fieldnames=fields)
        w.writeheader()
        w.writerows(csv_rows)

    # meta
    meta = {"timestamp": timestamp, "iteration": args.iteration, "selected_publishers": [p["name"] for p in selected],
            "total_pages": total_pages, "n_runs_per_page": n_runs_per_page,
            "NAV_MS": NAV_MS, "WAIT_MS": WAIT_MS, "GLOBAL_PAGE_RUN_TIMEOUT_SEC": GLOBAL_RUN_SEC}
    with open(os.path.join(run_root, "meta.json"), "w", encoding="utf-8") as jf:
        json.dump(meta, jf, indent=2)

    logging.info("Iteration complete: output in %s (csv=%s) meta=%s", run_root, csv_path, meta)


if __name__ == "__main__":
    main()
