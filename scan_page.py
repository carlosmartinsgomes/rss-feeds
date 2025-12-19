#!/usr/bin/env python3
"""
scan_page.py — weighted-slot scanner (adaptative timing) with compact "mini-HAR" output

Features applied:
- slot allocation based on weight_pct
- dynamic per-page budget (TARGET_ITERATION_MINUTES ± FLEX_MINUTES)
- device-aware mobile context (Playwright device descriptor "Pixel 5" when available)
- NO full HARs: instead writes compact mini-har JSON per run (much smaller)
- detection flags: prebid_detected / pubmatic_detected (reduce false positives)
- safety: global per-page run timeout checked and will end early if exceeded
- watchdog: force-close browser/context if global timeout exceeded (threading.Timer)
"""

import os
import sys
import json
import time
import argparse
import logging
import threading
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

# detection markers
PUBMATIC_STRONG = ["pubmatic", "ads.pubmatic", "pubmatic.com", "hb.pubmatic"]
PREBID_MARKERS = ["pbjs", "prebid", "prebid.js"]

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

# ---------- slot allocation ----------
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

# ---------- dynamic timing ----------
def compute_timeouts_and_runs(num_pages, target_minutes=TARGET_ITERATION_MINUTES, flex_minutes=FLEX_MINUTES):
    if num_pages <= 0:
        num_pages = 1
    budget_seconds = target_minutes * 60
    per_page_budget = budget_seconds / num_pages

    # decide runs:
    if per_page_budget < 20:
        n_runs = 1
    elif per_page_budget < 90:
        n_runs = 1
    elif per_page_budget < 240:
        n_runs = 2
    else:
        n_runs = max(1, BASE_N_RUNS_PER_PAGE)  # se tens base>1 adapta aqui

    # compute timeouts from fractions of per_page_budget but clamp to min/max
    nav = per_page_budget * 0.45
    wait = per_page_budget * 0.30
    safety = per_page_budget * 0.20

    # clamp to configured min/max
    nav = max(NAV_TIMEOUT_MIN, min(NAV_TIMEOUT_MAX, nav))
    wait = max(WAIT_AFTER_LOAD_MIN, min(WAIT_AFTER_LOAD_MAX, wait))
    global_run_timeout = max(GLOBAL_PAGE_RUN_TIMEOUT_MIN, min(GLOBAL_PAGE_RUN_TIMEOUT_MAX, safety))

    # convert to ms where needed
    return int(n_runs), int(nav * 1000), int(wait * 1000), int(global_run_timeout)


# ---------- capture logic (mini-har) ----------
def capture_single_run(playwright, url, outdir, domain, page_label, geo, proxy_url, mobile, iteration, run_idx,
                       NAV_TIMEOUT_MS, WAIT_AFTER_LOAD_MS, GLOBAL_PAGE_RUN_TIMEOUT_SEC):
    run_ts = timestamp_str()
    safe = sanitize(f"{domain}_{page_label}_{geo}_iter{iteration}_run{run_idx}_{run_ts}")
    mini_har_path = os.path.join(outdir, f"{safe}.minihar.json")
    summary_file = os.path.join(outdir, f"{safe}.json")

    req_entries = {}
    seq = []
    flags = {"prebid": False, "pubmatic": False}

    launch_args = {"headless": True, "args": ["--no-sandbox", "--disable-setuid-sandbox"] }
    ctx_args = {"ignore_https_errors": True}
    if proxy_url:
        launch_args["proxy"] = {"server": proxy_url}
    user_agent = "Mozilla/5.0 (Linux; Android 11; Pixel 5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117 Mobile Safari/537.36" if mobile else "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117 Safari/537.36"
    ctx_args["user_agent"] = user_agent

    browser = None
    context = None
    page = None
    start_time = time.time()
    timer = None  # watchdog timer handle

    try:
        browser = playwright.chromium.launch(**launch_args)

        # --- START WATCHDOG: força fechar context/browser se GLOBAL_PAGE_RUN_TIMEOUT_SEC for excedido ---
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
            # start watchdog (seconds)
            if GLOBAL_PAGE_RUN_TIMEOUT_SEC and GLOBAL_PAGE_RUN_TIMEOUT_SEC > 0:
                timer = threading.Timer(GLOBAL_PAGE_RUN_TIMEOUT_SEC, _kill_browser)
                timer.daemon = True
                timer.start()
        except Exception:
            logging.exception("Failed to start watchdog timer", exc_info=True)
        # --- END WATCHDOG ---

        # device-aware context (use Playwright descriptor if available)
        device = None
        try:
            if hasattr(playwright, "devices"):
                # safe access patterns
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

        # helper to check global timeout and abort capture early
        def check_global_timeout():
            if GLOBAL_PAGE_RUN_TIMEOUT_SEC and (time.time() - start_time) > GLOBAL_PAGE_RUN_TIMEOUT_SEC:
                return True
            return False

        def on_request(r):
            try:
                key = id(r)
                ts = time.time()
                # post data safely
                post = None
                try:
                    if callable(getattr(r, "post_data", None)):
                        post = r.post_data()
                    else:
                        post = getattr(r, "post_data", None)
                except Exception:
                    post = None
                # headers safe extraction
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
                    "post_data": _truncate_str(post, MAX_POSTDATA_LEN)
                }
                req_entries[key] = entry
                seq.append(key)
                low = (r.url or "").lower()
                if not flags["prebid"] and any(k in low for k in PREBID_MARKERS):
                    flags["prebid"] = True
                if not flags["pubmatic"] and any(k in low for k in PUBMATIC_STRONG):
                    flags["pubmatic"] = True
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
                if not flags["pubmatic"] and any(k in low for k in PUBMATIC_STRONG):
                    flags["pubmatic"] = True
                if not flags["prebid"] and any(k in low for k in PREBID_MARKERS):
                    flags["prebid"] = True
            except Exception:
                logging.debug("on_response exception", exc_info=True)

        page.on("request", on_request)
        page.on("response", on_response)

        # navigation with NAV_TIMEOUT_MS (ms)
        try:
            page.goto(url, timeout=NAV_TIMEOUT_MS)
        except PlaywrightTimeoutError:
            logging.warning("Navigation timeout for url=%s", url)
        except Exception as e:
            logging.warning("Navigation exception %s", e)

        # check global timeout after navigation
        if check_global_timeout():
            logging.warning("Global run timeout exceeded after navigation, ending early for %s", url)
        else:
            with suppress(Exception):
                page.wait_for_load_state("networkidle", timeout=min(NAV_TIMEOUT_MS, PAGE_NETWORK_IDLE_TIMEOUT_MS if 'PAGE_NETWORK_IDLE_TIMEOUT_MS' in globals() else NAV_TIMEOUT_MS))

            # extra wait for late calls
            if not check_global_timeout():
                try:
                    page.wait_for_timeout(WAIT_AFTER_LOAD_MS)
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
            "response_ts": ent.get("response_ts"),
            "status": ent.get("status"),
            "response_headers_count": ent.get("response_headers_count")
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

    logging.info("Run finished %s pubmatic_hits=%s total_requests=%s prebid=%s pubmatic=%s",
                 safe, summary["pubmatic_requests"], total, flags["prebid"], flags["pubmatic"])
    return summary

# aggregated per page
def run_page_aggregated(playwright, url, outdir, domain, page_label, geo, proxy_url, mobile, iteration,
                        n_runs, NAV_TIMEOUT_MS, WAIT_AFTER_LOAD_MS, GLOBAL_PAGE_RUN_TIMEOUT_SEC):
    runs = []
    for r in range(1, n_runs + 1):
        runs.append(capture_single_run(playwright, url, outdir, domain, page_label, geo, proxy_url, mobile, iteration, r,
                                       NAV_TIMEOUT_MS, WAIT_AFTER_LOAD_MS, GLOBAL_PAGE_RUN_TIMEOUT_SEC))
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
    with open(os.path.join(run_root, "slot_map.json"), "w", encoding="utf-8") as f:
        json.dump(pub_to_slots, f, indent=2)

    selected = [p for p in pubs if p["name"] in pub_to_slots and args.iteration in pub_to_slots[p["name"]]]
    logging.info("Iteration %s selected publishers: %s", args.iteration, [p["name"] for p in selected])

    # build page entries (desktop + mobile)
    page_entries = []
    for pub in selected:
        for pg in pub.get("pages", []):
            if not pg.get("url"):
                continue
            page_entries.append((pub, pg, False))
            page_entries.append((pub, pg, True))
    total_pages = len(page_entries)

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
