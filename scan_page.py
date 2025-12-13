#!/usr/bin/env python3
"""
Robust Playwright scanner

- Melhor logging
- Timeouts e tratamento para não ficar pendurado
- Fecha browser/context em finally
- Suporta proxies via country_proxies.json
- Designed to run inside a Docker image that already bundles browsers (recommended)
"""

import os
import sys
import json
import time
import csv
import argparse
import logging
from datetime import datetime
from pathlib import Path
from contextlib import suppress

# Try import playwright, will fail cleanly if not installed
try:
    from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
except Exception as e:
    print("FATAL: playwright not available. Run inside Playwright-enabled environment (docker or preinstalled). Error:", e, file=sys.stderr)
    sys.exit(3)

# --- Configurable defaults ---
N_RUNS_PER_ITERATION = int(os.environ.get("N_RUNS_PER_ITERATION", "3"))
WAIT_AFTER_LOAD_MS = int(os.environ.get("WAIT_AFTER_LOAD_MS", "10000"))  # ms
NAV_TIMEOUT_MS = int(os.environ.get("NAV_TIMEOUT_MS", "40000"))  # ms for page.goto
PAGE_NETWORK_IDLE_TIMEOUT_MS = int(os.environ.get("PAGE_NETWORK_IDLE_TIMEOUT_MS", "20000"))
GLOBAL_PAGE_RUN_TIMEOUT_SEC = int(os.environ.get("GLOBAL_PAGE_RUN_TIMEOUT_SEC", "60"))  # safety per run
PUBMATIC_KEYWORDS = ["pubmatic", "pbjs", "hb.pubmatic", "ads.pubmatic", "pubmatic.com"]
DEFAULT_OUTDIR = "output"
DEFAULT_PROXY_FILE = "country_proxies.json"
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
# ------------------------------

logging.basicConfig(level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
                    format="%(asctime)s %(levelname)s %(message)s")

def timestamp_str():
    return datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

def load_json(path):
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def ensure_dir(p):
    Path(p).mkdir(parents=True, exist_ok=True)

def sanitize_filename(s):
    return "".join(c if (c.isalnum() or c in "-._") else "_" for c in s)[:200]

def capture_run(playwright, url, outdir, domain_name, page_label, geo, proxy_url, mobile, iteration, run_idx):
    """Single run: launch browser, record har, collect requests. Returns summary dict."""
    run_ts = timestamp_str()
    safe_name = sanitize_filename(f"{domain_name}_{page_label}_{geo}_iter{iteration}_run{run_idx}_{run_ts}")
    har_path = os.path.join(outdir, f"{safe_name}.har")
    summary_path = os.path.join(outdir, f"{safe_name}.json")
    logging.info("Starting run %s (url=%s geo=%s mobile=%s proxy=%s)", run_idx, url, geo, mobile, proxy_url)

    # launch args - add no-sandbox for many CI envs
    launch_args = {"headless": True, "args": ["--no-sandbox", "--disable-setuid-sandbox"]}
    context_args = {"ignore_https_errors": True}
    if proxy_url:
        launch_args["proxy"] = {"server": proxy_url}
    user_agent = "Mozilla/5.0 (Linux; Android 11; Pixel 5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117 Mobile Safari/537.36" if mobile else "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117 Safari/537.36"
    context_args["user_agent"] = user_agent

    reqs = []
    browser = None
    context = None
    page = None

    try:
        logging.info("Launching browser...")
        browser = playwright.chromium.launch(**launch_args)
        logging.info("Browser launched.")
        context = browser.new_context(record_har_path=har_path, **context_args)
        page = context.new_page()
        page.on("request", lambda r: reqs.append(r.url))
        # Navigate and capture
        try:
            logging.info("Navigating to %s (timeout %sms)", url, NAV_TIMEOUT_MS)
            page.goto(url, timeout=NAV_TIMEOUT_MS)
        except PlaywrightTimeoutError:
            logging.warning("Navigation timeout for url=%s", url)
        except Exception as e:
            logging.warning("Navigation raised exception: %s", e)

        # wait for network idle (best effort)
        try:
            page.wait_for_load_state("networkidle", timeout=PAGE_NETWORK_IDLE_TIMEOUT_MS)
        except PlaywrightTimeoutError:
            logging.debug("networkidle timeout (ok, continuing)")
        except Exception as e:
            logging.debug("wait_for_load_state exception: %s", e)

        # extra wait for late calls
        logging.debug("Waiting extra %sms", WAIT_AFTER_LOAD_MS)
        page.wait_for_timeout(WAIT_AFTER_LOAD_MS)

    except Exception as e:
        logging.exception("Exception during run (url=%s): %s", url, e)
    finally:
        # close context and browser (ensures HAR written)
        with suppress(Exception):
            if context:
                context.close()
        with suppress(Exception):
            if browser:
                browser.close()

    total_requests = len(reqs)
    pubmatic_hits = [u for u in reqs if any(k in u.lower() for k in PUBMATIC_KEYWORDS)]
    pubmatic_count = len(pubmatic_hits)

    summary = {
        "domain": domain_name,
        "page_label": page_label,
        "url": url,
        "geo": geo,
        "iteration": iteration,
        "run_idx": run_idx,
        "timestamp": run_ts,
        "har_path": har_path,
        "total_requests": total_requests,
        "pubmatic_requests": pubmatic_count,
        "pubmatic_sample": pubmatic_hits[:10],
        "user_agent": user_agent,
        "proxy_used": proxy_url or ""
    }

    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    logging.info("Run finished: %s pubmatic_hits=%s total_requests=%s", safe_name, pubmatic_count, total_requests)
    return summary

def run_scan_for_page(playwright, url, outdir, domain_name, page_label, geo, proxy_url, mobile, iteration):
    """Aggregates N runs for a page. Ensures no leaks."""
    runs = []
    for run_idx in range(1, N_RUNS_PER_ITERATION + 1):
        # small delay between runs to vary caches
        if run_idx > 1:
            time.sleep(2)
        try:
            r = capture_run(playwright, url, outdir, domain_name, page_label, geo, proxy_url, mobile, iteration, run_idx)
            runs.append(r)
        except Exception as e:
            logging.exception("Run %s failed for %s: %s", run_idx, url, e)

    if not runs:
        return {
            "domain": domain_name,
            "page_label": page_label,
            "geo": geo,
            "iteration": iteration,
            "avg_total_requests": 0,
            "avg_pubmatic_requests": 0,
            "runs": []
        }

    avg_total = sum(r["total_requests"] for r in runs) / len(runs)
    avg_pub = sum(r["pubmatic_requests"] for r in runs) / len(runs)
    agg = {
        "domain": domain_name,
        "page_label": page_label,
        "geo": geo,
        "iteration": iteration,
        "avg_total_requests": avg_total,
        "avg_pubmatic_requests": avg_pub,
        "runs": runs
    }
    return agg

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--targets", default="targets.json")
    parser.add_argument("--proxies", default=DEFAULT_PROXY_FILE)
    parser.add_argument("--outdir", default=DEFAULT_OUTDIR)
    parser.add_argument("--iteration", type=int, default=0)
    parser.add_argument("--geo", default="ALL")
    parser.add_argument("--only_domain", default=None)
    args = parser.parse_args()

    logging.info("scan_page.py starting. iteration=%s geo=%s targets=%s", args.iteration, args.geo, args.targets)

    targets = load_json(args.targets)
    proxies = load_json(args.proxies)
    ensure_dir(args.outdir)

    timestamp = timestamp_str()
    run_out_dir = os.path.join(args.outdir, timestamp)
    ensure_dir(run_out_dir)
    csv_path = os.path.join(run_out_dir, f"summary_iteration_{args.iteration}.csv")
    csv_fields = ["domain", "page_label", "url", "geo", "iteration", "avg_total_requests", "avg_pubmatic_requests", "runs_count"]
    csv_rows = []

    # Start Playwright - wrap to catch errors early
    try:
        logging.info("Starting Playwright context...")
        with sync_playwright() as p:
            logging.info("Playwright started.")
            for pub in targets.get("publishers", []):
                domain = pub.get("domain")
                if args.only_domain and args.only_domain != domain:
                    continue
                pages = pub.get("pages", [])
                geos = pub.get("geos", [])
                selected_geos = [g for g in geos if (args.geo == "ALL" or g.get("country_code") == args.geo)]
                if not selected_geos and args.geo != "ALL":
                    continue
                if not selected_geos:
                    selected_geos = geos

                logging.info("Publisher %s pages=%s geos=%s", domain, len(pages), [g.get("country_code") for g in selected_geos])
                for geo_entry in selected_geos:
                    country = geo_entry.get("country_code")
                    proxy_url = geo_entry.get("proxy") or proxies.get(country)
                    for page_obj in pages:
                        page_label = page_obj.get("label")
                        url = page_obj.get("url")
                        if not url:
                            continue
                        logging.info("Scanning domain=%s page=%s geo=%s", domain, page_label, country)
                        # desktop
                        agg = run_scan_for_page(p, url, run_out_dir, domain, f"{page_label}_desktop", country, proxy_url, mobile=False, iteration=args.iteration)
                        csv_rows.append({
                            "domain": agg["domain"],
                            "page_label": agg["page_label"],
                            "url": url,
                            "geo": country,
                            "iteration": args.iteration,
                            "avg_total_requests": agg["avg_total_requests"],
                            "avg_pubmatic_requests": agg["avg_pubmatic_requests"],
                            "runs_count": len(agg["runs"])
                        })
                        # mobile
                        agg_m = run_scan_for_page(p, url, run_out_dir, domain, f"{page_label}_mobile", country, proxy_url, mobile=True, iteration=args.iteration)
                        csv_rows.append({
                            "domain": agg_m["domain"],
                            "page_label": agg_m["page_label"],
                            "url": url,
                            "geo": country,
                            "iteration": args.iteration,
                            "avg_total_requests": agg_m["avg_total_requests"],
                            "avg_pubmatic_requests": agg_m["avg_pubmatic_requests"],
                            "runs_count": len(agg_m["runs"])
                        })

    except Exception as e:
        logging.exception("Fatal error running Playwright scan: %s", e)
        # write minimal meta and exit non-zero
        with open(os.path.join(run_out_dir, "meta_summary.json"), "w") as jf:
            json.dump({"timestamp": timestamp, "iteration": args.iteration, "error": str(e)}, jf, indent=2)
        sys.exit(4)

    # write CSV
    with open(csv_path, "w", newline="", encoding="utf-8") as csvf:
        w = csv.DictWriter(csvf, fieldnames=csv_fields)
        w.writeheader()
        for r in csv_rows:
            w.writerow(r)

    with open(os.path.join(run_out_dir, "meta_summary.json"), "w", encoding="utf-8") as jf:
        json.dump({"timestamp": timestamp, "iteration": args.iteration, "rows": len(csv_rows)}, jf, indent=2)

    logging.info("Scan finished. Output dir: %s CSV: %s", run_out_dir, csv_path)

if __name__ == "__main__":
    main()
