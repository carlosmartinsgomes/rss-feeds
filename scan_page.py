#!/usr/bin/env python3
"""
scan_page.py

Playwright scanner for header-bid detection and HAR capture.

Features:
- Reads targets from targets.json (domain + list of page URLs + video/mobile flags + geos with %)
- For each target page:
    - For a chosen geo (or all): performs N_RUNS_PER_ITERATION = 3 consecutive loads (clearing context each time)
    - Each load: goto -> wait_for_load_state('networkidle') -> wait extra WAIT_AFTER_LOAD_MS (default 10000ms)
    - Captures HAR (record_har_path), collects all request URLs, counts pubmatic-related hits
    - Saves per-run HAR and a summary JSON + a cumulative CSV summary for the iteration
- Designed to be called once per iteration (e.g. one of the 6 daily runs).
- Supports optional proxy per country (if you provide proxies mapping JSON or set env var)
- Use workflow_dispatch to trigger on GitHub Actions (self-hosted runner)

Usage example:
    python3 scan_page.py --iteration 0 --geo US --targets targets.json --outdir output

"""
import os
import sys
import json
import time
import csv
import argparse
from datetime import datetime
from pathlib import Path
from playwright.sync_api import sync_playwright
import logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


# ---------- CONFIG ----------
N_RUNS_PER_ITERATION = 3    # number of consecutive loads per page per iteration
WAIT_AFTER_LOAD_MS = 10000  # extra wait after networkidle (ms)
NAV_TIMEOUT_MS = 40000
PUBMATIC_KEYWORDS = ["pubmatic", "pbjs", "hb.pubmatic", "ads.pubmatic", "pubmatic.com"]
MOBILE_USER_AGENTS = [
    "Mozilla/5.0 (Linux; Android 11; Pixel 5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117 Mobile Safari/537.36",
    # add more if desired
]
DESKTOP_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117 Safari/537.36"
]
# default output dir
DEFAULT_OUTDIR = "output"
# proxy mapping filename (optional). JSON mapping country_code->proxy_url
DEFAULT_PROXY_FILE = "country_proxies.json"
# ---------- END CONFIG ----------

def load_targets(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def load_proxies(path):
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def ensure_dir(p):
    Path(p).mkdir(parents=True, exist_ok=True)

def sanitize_filename(s):
    return "".join(c if (c.isalnum() or c in "-._") else "_" for c in s)[:200]

def timestamp_str():
    return datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

def run_scan_for_page(playwright, url, outdir, domain_name, page_label, geo, proxy_url=None, mobile=False, iteration=0):
    """
    Performs N_RUNS_PER_ITERATION loads; saves HAR per run and returns aggregated summary
    """
    summary_runs = []
    # choose UA
    user_agent = MOBILE_USER_AGENTS[0] if mobile else DESKTOP_USER_AGENTS[0]

    for run_idx in range(1, N_RUNS_PER_ITERATION + 1):
        run_ts = timestamp_str()
        safe_name = sanitize_filename(f"{domain_name}_{page_label}_{geo}_iter{iteration}_run{run_idx}_{run_ts}")
        har_path = os.path.join(outdir, f"{safe_name}.har")
        summary_path = os.path.join(outdir, f"{safe_name}.json")

        # configure browser launch options
        launch_args = {"headless": True}
        context_args = {"user_agent": user_agent, "ignore_https_errors": True}
        if proxy_url:
            launch_args["proxy"] = {"server": proxy_url}

        logging.info("Launching browser for url=%s geo=%s proxy=%s mobile=%s", url, geo, proxy_url, mobile)
        browser = playwright.chromium.launch(**launch_args)
        logging.info("Browser launched.")

        browser = playwright.chromium.launch(**launch_args)
        logging.info("Launching browser for url=%s geo=%s proxy=%s mobile=%s", url, geo, proxy_url, mobile)
        browser = playwright.chromium.launch(**launch_args)
        logging.info("Browser launched.")
        
        context = browser.new_context(record_har_path=har_path, **context_args)
        logging.info("Launching browser for url=%s geo=%s proxy=%s mobile=%s", url, geo, proxy_url, mobile)
        browser = playwright.chromium.launch(**launch_args)
        logging.info("Browser launched.")

        page = context.new_page()
        reqs = []

        def on_request(r):
            try:
                reqs.append(r.url)
            except Exception:
                pass

        page.on("request", on_request)
        try:
            page.goto(url, timeout=NAV_TIMEOUT_MS)
        except Exception as e:
            # navigation failed; still collect whatever requests happened
            print(f"[WARN] Navigation failed for {url}: {e}")
        try:
            page.wait_for_load_state("networkidle", timeout=20000)
        except Exception:
            # not critical - continue to wait extra
            pass
        # extra wait
        page.wait_for_timeout(WAIT_AFTER_LOAD_MS)

        # close context (will write HAR)
        try:
            context.close()
        except Exception:
            pass
        try:
            browser.close()
        except Exception:
            pass

        # compute metrics
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
            "proxy_used": proxy_url or "",
        }

        with open(summary_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2)

        summary_runs.append(summary)
        # small sleep between consecutive runs (to vary caches) - immediate is fine but add short sleep
        time.sleep(2)

    # aggregate across the N runs (average)
    avg_total = sum(r["total_requests"] for r in summary_runs) / len(summary_runs)
    avg_pub = sum(r["pubmatic_requests"] for r in summary_runs) / len(summary_runs)
    agg = {
        "domain": domain_name,
        "page_label": page_label,
        "geo": geo,
        "iteration": iteration,
        "avg_total_requests": avg_total,
        "avg_pubmatic_requests": avg_pub,
        "runs": summary_runs,
    }
    return agg

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--targets", default="targets.json", help="targets JSON file")
    parser.add_argument("--proxies", default=DEFAULT_PROXY_FILE, help="optional JSON mapping country->proxy_url")
    parser.add_argument("--outdir", default=DEFAULT_OUTDIR, help="output directory")
    parser.add_argument("--iteration", type=int, default=0, help="iteration index (0..5)")
    parser.add_argument("--geo", default="ALL", help="restrict to a geo country code (e.g. US) or ALL")
    parser.add_argument("--only_domain", default=None, help="if set, only run for this domain")
    args = parser.parse_args()

    targets = load_targets(args.targets)
    proxies = load_proxies(args.proxies)
    ensure_dir(args.outdir)

    timestamp = timestamp_str()
    run_out_dir = os.path.join(args.outdir, timestamp)
    ensure_dir(run_out_dir)

    # Summary CSV
    csv_path = os.path.join(run_out_dir, f"summary_iteration_{args.iteration}.csv")
    csv_fields = ["domain", "page_label", "url", "geo", "iteration", "avg_total_requests", "avg_pubmatic_requests", "runs_count"]
    csv_rows = []

    print("[INFO] Starting scan. Targets count:", len(targets["publishers"]))
    logging.info("Opening Playwright context...")
    try:
        with sync_playwright() as p:
            logging.info("Playwright started successfully.")
            # ... restante código ...
    except Exception as e:
        logging.exception("Fatal error while running Playwright scan: %s", e)
        sys.exit(2)

    with sync_playwright() as p:
        for pub in targets["publishers"]:
            domain = pub.get("domain")
            if args.only_domain and args.only_domain != domain:
                continue
            pages = pub.get("pages", [])
            geos = pub.get("geos", [])
            # decide which geos to run: if args.geo != ALL, filter
            selected_geos = [g for g in geos if (args.geo == "ALL" or g["country_code"] == args.geo)]
            if not selected_geos:
                # if none selected and args.geo specified, skip
                if args.geo != "ALL":
                    continue
                # otherwise use all geos
                selected_geos = geos

            print(f"[INFO] Publisher: {domain} -> pages: {len(pages)} -> geos: {[g['country_code'] for g in selected_geos]}")
            for geo_entry in selected_geos:
                country = geo_entry["country_code"]
                proxy_url = geo_entry.get("proxy") or proxies.get(country)
                for page in pages:
                    page_label = page.get("label")
                    url = page.get("url")
                    if not url:
                        continue
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
                    # mobile (same URL but mobile UA)
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

    # write CSV
    with open(csv_path, "w", newline="", encoding="utf-8") as csvf:
        w = csv.DictWriter(csvf, fieldnames=csv_fields)
        w.writeheader()
        for r in csv_rows:
            w.writerow(r)

    # also dump an overall summary JSON
    with open(os.path.join(run_out_dir, "meta_summary.json"), "w", encoding="utf-8") as jf:
        json.dump({"timestamp": timestamp, "iteration": args.iteration, "rows": len(csv_rows)}, jf, indent=2)

    print("[INFO] Scan finished. Output dir:", run_out_dir)
    print("[INFO] CSV summary:", csv_path)

if __name__ == "__main__":
    main()
