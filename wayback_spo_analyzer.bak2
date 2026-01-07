#!/usr/bin/env python3
"""
wayback_spo_analyzer.py (updated - retry/backoff)

This file is the same analyzer you provided previously but with a more robust
backoff/retry strategy applied to HTTP requests (CDX + Wayback fetches).
Policies implemented:
 - Exponential backoff with jitter on transient errors (network issues, 5xx, 429).
 - Honors Retry-After header when present.
 - Configurable max retries and backoff factor via globals (and CLI can be added later).
 - Logs retry attempts to stdout.

Usage:
  python3 wayback_spo_analyzer.py --domains-file principaldomains --log-file analise_log.json --out wayback_spo_report.xlsx
"""
import argparse
import requests
import time
import json
import csv
import os
import re
import random
from datetime import datetime, timezone, timedelta, date
from collections import Counter, defaultdict, namedtuple
import math
import pandas as pd
import pycountry

# -----------------------
# Configuráveis
# -----------------------
CDX_API = "http://web.archive.org/cdx/search/cdx"
WAYBACK_GET = "http://web.archive.org/web/{ts}/{orig}"
START_DATE = "20200101"   # start period fallback
ANALYSIS_LOG = "analise_log.json"
OUT_XLSX = "wayback_spo_report.xlsx"

# sampling config
SNAPSHOTS_PER_YEAR = 2  # initial sampling, can be adjusted
MAX_SNAPSHOT_FETCH = 1200  # protective cap for raw list before collapsing
SLEEP_MIN = 0.12
SLEEP_MAX = 0.28

# retry/backoff config (new)
MAX_RETRIES = 5
BACKOFF_FACTOR = 1.0  # base seconds, exponential multiplier
BACKOFF_MAX = 60.0    # max sleep between retries
RETRY_STATUS_CODES = {429, 500, 502, 503, 504}

# minimum persistence for exclusivity (in days)
EXCLUSIVITY_MIN_PERSIST_DAYS = 7
EXCLUSIVITY_MIN_CONSECUTIVE = 2  # need at least two consecutive snapshots in that state

# truncation detection thresholds
TRUNCATION_RELATIVE_THRESHOLD = 0.20  # if length < 20% of median -> suspect
TRUNCATION_MIN_BYTES = 64  # if length smaller than this -> suspect

# top SSPs signature configuration (ads domain and tech tokens)
SSP_DEFS = {
    "google": {
        "ads_domains": ["google.com", "doubleclick.net", "googlesyndication.com"],
        "tech_tokens": ["gpt.js", "doubleclick.net", "googlesyndication.com", "adservice.google"],
    },
    "magnite": {
        "ads_domains": ["rubiconproject.com", "magnite.com", "telaria.com", "spotx.tv", "spotxchange.com"],
        "tech_tokens": ["mweb","magnite.js","rubiconproject","telaria","spotx"],
    },
    "pubmatic": {
        "ads_domains": ["pubmatic.com"],
        "tech_tokens": ["openwrap","hb.pubmatic","ow.js","pubmatic.com","ads.pubmatic.com"],
    },
    "index": {
        "ads_domains": ["indexexchange.com","casalemedia.com"],
        "tech_tokens": ["indexww.com","cygnus","casalemedia"],
    },
    "openx": {
        "ads_domains": ["openx.com"],
        "tech_tokens": ["openx.net","ox-delivery","openx.com"],
    },
    "xandr": {
        "ads_domains": ["appnexus.com","xandr.com"],
        "tech_tokens": ["adnxs.com","ast.js","xandr","ib.adnxs.com"],
    },
    "triplelift": {
        "ads_domains": ["triplelift.com"],
        "tech_tokens": ["tlx","3lift.com","triplelift.net"],
    },
    "sharethrough": {
        "ads_domains": ["sharethrough.com"],
        "tech_tokens": ["str","sharethrough.js","native.sharethrough.com"],
    },
    "sovrn": {
        "ads_domains": ["sovrn.com","lijit.com"],
        "tech_tokens": ["sovrn.com","lijit.com","ap.lijit.com"],
    },
    "adform": {
        "ads_domains": ["adform.com"],
        "tech_tokens": ["adform.net","adform.js","track.adform.net"],
    }
}

# Ads.txt manager known hosts (examples)
ADS_MANAGERS = ["sellers.json","adstxt.events","adstxt.guide","ads.txt.manager","adstxtapi","adstxt"]

# regex helpers (case-insensitive)
RE_COMMENT = re.compile(r'^\s*#')
RE_ADS_LINE = re.compile(r'^\s*([^,\s]+)\s*,\s*([^,\s]+)\s*,\s*([^,\s]+)', re.I)  # domain, seller_id, rel(DIRECT/RESELLER)
RE_HTML_LANG = re.compile(r'<html[^>]*lang=["\']?([A-Za-z]{2})(?:-([A-Za-z]{2}))?["\']?', re.I)
RE_CC_TLD_IN_HOST = re.compile(r'\.([a-z]{2})(?:[:/]|$)', re.I)

# namedtuple for snapshot metadata
Snapshot = namedtuple("Snapshot", ["timestamp","original","statuscode","digest","length"])

# -----------------------
# Utilities
# -----------------------
def now_yyyymmdd():
    return datetime.utcnow().strftime("%Y%m%d")

def sleep_random():
    # deterministic-ish sleep between min and max (midpoint) to be polite
    time.sleep(SLEEP_MIN + (SLEEP_MAX - SLEEP_MIN) * 0.5)

def _compute_backoff(attempt):
    """Compute exponential backoff seconds with jitter."""
    # attempt is 0-based (0 => first retry wait = BACKOFF_FACTOR * 2^0)
    base = BACKOFF_FACTOR * (2 ** attempt)
    jitter = random.uniform(0, 1.0)
    val = min(BACKOFF_MAX, base + jitter)
    return val

def safe_request(url, params=None, timeout=20, allow_redirects=True, max_retries=MAX_RETRIES):
    """
    Robust HTTP GET with retries/backoff.
    - retries on network errors and on specific HTTP status codes (429,5xx).
    - honors Retry-After header when present (if numeric seconds or HTTP-date).
    Returns requests.Response or None on fatal failure.
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; WaybackSPOAnalyzer/1.0)"
    }
    attempt = 0
    while True:
        try:
            r = requests.get(url, params=params, timeout=timeout, allow_redirects=allow_redirects, headers=headers)
        except Exception as e:
            attempt += 1
            if attempt > max_retries:
                print(f"[WARN] safe_request giving up after {attempt-1} retries on network error for {url}: {e}")
                return None
            sleep_for = _compute_backoff(attempt-1)
            print(f"[DEBUG] network error fetching {url}: {e}. retry {attempt}/{max_retries} -> sleeping {sleep_for:.1f}s")
            time.sleep(sleep_for)
            continue

        # If status code indicates transient error -> retry
        status = r.status_code
        if status in RETRY_STATUS_CODES:
            # honor Retry-After if given
            retry_after = r.headers.get("Retry-After")
            if retry_after:
                try:
                    # first try numeric
                    wait = int(retry_after)
                except Exception:
                    # fallback: try HTTP-date parse
                    try:
                        retry_date = datetime.strptime(retry_after, "%a, %d %b %Y %H:%M:%S %Z")
                        wait = (retry_date - datetime.utcnow()).total_seconds()
                        if wait < 0:
                            wait = _compute_backoff(attempt)
                    except Exception:
                        wait = _compute_backoff(attempt)
            else:
                wait = _compute_backoff(attempt)
            attempt += 1
            if attempt > max_retries:
                print(f"[WARN] safe_request: max retries reached for {url} status={status}")
                return r  # return final response (could be 429/5xx) so caller sees it
            print(f"[DEBUG] safe_request: transient status {status} for {url}. retry {attempt}/{max_retries} sleeping {wait:.1f}s")
            time.sleep(max(0.1, wait))
            continue

        # success or non-retriable status -> return
        return r

def read_domains(path="principaldomains"):
    if not os.path.isfile(path):
        raise FileNotFoundError(f"{path} not found")
    with open(path, 'r', encoding='utf-8') as f:
        return [line.strip() for line in f if line.strip() and not line.strip().startswith('#')]

def load_log(path=ANALYSIS_LOG):
    if not os.path.isfile(path):
        return {}
    with open(path,'r',encoding='utf-8') as f:
        try:
            return json.load(f)
        except Exception:
            return {}

def save_log(log, path=ANALYSIS_LOG):
    with open(path,'w',encoding='utf-8') as f:
        json.dump(log, f, indent=2, ensure_ascii=False)

# -----------------------
# CDX & Snapshot helpers
# -----------------------
def cdx_query(url_pattern, from_ts=None, to_ts=None, filters=None, limit=10000):
    """
    Query CDX and return list of Snapshot ordered ascending by timestamp.
    Uses safe_request() with retries/backoff.
    """
    params = {
        "url": url_pattern,
        "output": "json",
        "fl": "timestamp,original,statuscode,digest,length",
        "limit": str(limit),
    }
    if filters:
        params["filter"] = filters
    if from_ts:
        params["from"] = from_ts
    if to_ts:
        params["to"] = to_ts

    r = safe_request(CDX_API, params=params, timeout=30, allow_redirects=True)
    if not r:
        print(f"[WARN] CDX query failed for {url_pattern}")
        return []
    if r.status_code != 200:
        print(f"[WARN] CDX returned status {r.status_code} for {url_pattern}")
        try:
            data = r.json()
            # sometimes the API returns error info; we'll treat as empty
        except Exception:
            pass
        return []

    try:
        data = r.json()
    except Exception:
        print(f"[WARN] CDX returned non-json for {url_pattern}")
        return []
    if not isinstance(data, list) or len(data) < 2:
        return []
    rows = data[1:]
    out = []
    for row in rows:
        if len(row) < 5:
            continue
        ts = row[0]
        orig = row[1]
        status = row[2]
        digest = row[3]
        length = row[4]
        try:
            length = int(length) if length else None
        except:
            length = None
        out.append(Snapshot(timestamp=ts, original=orig, statuscode=status, digest=digest, length=length))
    out.sort(key=lambda s: s.timestamp)
    return out

def wayback_fetch(snapshot: Snapshot, follow_redirects=True):
    """
    Fetch archived content for a snapshot using the Wayback get URL.
    Uses safe_request() for retry/backoff.
    Returns tuple (status_code, text, final_url) or (None,None,None) on fatal failure.
    """
    url = WAYBACK_GET.format(ts=snapshot.timestamp, orig=snapshot.original)
    r = safe_request(url, timeout=30, allow_redirects=follow_redirects)
    if not r:
        print(f"[WARN] wayback_fetch: failed to fetch {snapshot.original} @ {snapshot.timestamp}")
        return None, None, None
    # if non-200 returned but not retried (safe_request returned it), still return for caller to inspect
    try:
        text = r.text
    except Exception:
        text = None
    return r.status_code, text, r.url

# -----------------------
# Ads.txt parsing & signature
# -----------------------
def parse_ads_txt(content):
    """
    Parse ads.txt content into list of entries: (adssystem_domain, seller_account_id, rel)
    Ignores comment lines (#).
    """
    if content is None:
        return []
    lines = content.splitlines()
    entries = []
    for ln in lines:
        ln = ln.strip()
        if not ln:
            continue
        if RE_COMMENT.match(ln):
            continue
        # remove inline comments trailing
        if '#' in ln:
            ln = ln.split('#',1)[0].strip()
        parts = [p.strip() for p in ln.split(',')]
        if len(parts) >= 3:
            adsys = parts[0].lower()
            seller = parts[1].lower()
            rel = parts[2].upper()
            entries.append((adsys, seller, rel))
    return entries

def normalize_domain_key(domain):
    d = domain.lower().strip()
    d = re.sub(r'^https?://', '', d)
    d = d.split('/')[0]
    if d.startswith('www.'):
        d = d[4:]
    return d

def build_signature_from_ads(entries):
    """
    Given ads.txt parsed entries, return a signature dict with booleans for SSP_DEFS presence
    and counts of unique IDs (direct/reseller) per SSP.
    """
    sig = {}
    id_counts = {ssp: {"direct": set(), "reseller": set()} for ssp in SSP_DEFS.keys()}
    total_lines = len(entries)
    for adsys, seller, rel in entries:
        for ssp, defs in SSP_DEFS.items():
            for domain_token in defs["ads_domains"]:
                domain_token = domain_token.lower()
                if domain_token in adsys:
                    role = "direct" if rel.upper().startswith("DIRECT") else "reseller"
                    # normalize seller id by stripping protocol/path if present
                    s_id = seller.split('/')[0].strip().lower()
                    id_counts[ssp][role].add(s_id)
    for ssp in SSP_DEFS.keys():
        sig[f"{ssp}_ads"] = (len(id_counts[ssp]["direct"]) + len(id_counts[ssp]["reseller"])) > 0
        sig[f"{ssp}_ids_direct"] = len(id_counts[ssp]["direct"])
        sig[f"{ssp}_ids_reseller"] = len(id_counts[ssp]["reseller"])
        # include lists for detail exports
        sig[f"{ssp}_list_direct"] = sorted(list(id_counts[ssp]["direct"]))
        sig[f"{ssp}_list_reseller"] = sorted(list(id_counts[ssp]["reseller"]))
    sig["total_lines_ads"] = total_lines
    return sig

# -----------------------
# HTML heuristics & tech scan
# -----------------------
def scan_html_for_tech(html_text):
    """
    Scan HTML for tech tokens and country clues.
    Returns dict: tech_presence per SSP (pubmatic_tech True/False ...), country_clues Counter, html_lang
    """
    out = {}
    text = (html_text or "").lower()
    for ssp, defs in SSP_DEFS.items():
        out[f"{ssp}_tech"] = any(tok.lower() in text for tok in defs["tech_tokens"])
    lang_match = RE_HTML_LANG.search(html_text or "")
    html_lang = None
    if lang_match:
        if lang_match.group(2):
            html_lang = lang_match.group(2).upper()
        else:
            html_lang = lang_match.group(1).upper()
    country_clues = Counter()
    # names
    for country in pycountry.countries:
        name = country.name.lower()
        if name in text:
            try:
                country_clues[country.alpha_2] += text.count(name)
            except Exception:
                pass
    # ccTLD heuristic
    for m in RE_CC_TLD_IN_HOST.finditer(text):
        cc = m.group(1).upper()
        if cc and len(cc) == 2:
            country_clues[cc] += 1
    return out, country_clues, html_lang

# -----------------------
# Snapshot utility: collapse or sample
# -----------------------
def reduce_snapshots_to_daily(snaps):
    per_day = {}
    for s in snaps:
        day = s.timestamp[:8]  # YYYYMMDD
        per_day[day] = s
    out = sorted(per_day.values(), key=lambda x: x.timestamp)
    return out

def evenly_sample_by_year(snaps, per_year=SNAPSHOTS_PER_YEAR):
    if not snaps:
        return []
    year_map = defaultdict(list)
    for s in snaps:
        y = s.timestamp[:4]
        year_map[y].append(s)
    sampled = []
    for y, arr in sorted(year_map.items()):
        n = len(arr)
        if n <= per_year:
            sampled.extend(arr)
        else:
            for i in range(per_year):
                idx = int(round(i * (n-1) / max(1, per_year-1)))
                sampled.append(arr[idx])
    unique = []
    seen = set()
    for s in sorted(sampled, key=lambda x: x.timestamp):
        key = (s.timestamp, s.digest)
        if key not in seen:
            seen.add(key)
            unique.append(s)
    return unique

# -----------------------
# truncation detection helpers
# -----------------------
def median_length_of_snapshots(snaps):
    vals = [s.length for s in snaps if s.length and s.length > 0]
    if not vals:
        return None
    vals.sort()
    n = len(vals)
    if n % 2 == 1:
        return vals[n//2]
    else:
        return (vals[n//2 - 1] + vals[n//2]) // 2

def is_length_suspicious(snapshot: Snapshot, snaps_context):
    """
    Decide if snapshot.length is suspicious (drastic drop) vs median of snaps_context.
    snaps_context - list of snapshots (same domain reduced) used to compute median.
    """
    if snapshot.length is None:
        return False
    med = median_length_of_snapshots(snaps_context)
    if med is None:
        return False
    if snapshot.length < TRUNCATION_MIN_BYTES:
        return True
    if snapshot.length < TRUNCATION_RELATIVE_THRESHOLD * med:
        return True
    return False

# -----------------------
# Core Bissecção logic
# -----------------------
def signatures_equal(sigA, sigB, consider_ids=True):
    if sigA is None or sigB is None:
        return False
    for ssp in SSP_DEFS.keys():
        a_has = bool(sigA.get(f"{ssp}_ads"))
        b_has = bool(sigB.get(f"{ssp}_ads"))
        if a_has != b_has:
            return False
        if consider_ids:
            if sigA.get(f"{ssp}_ids_direct",0) != sigB.get(f"{ssp}_ids_direct",0):
                return False
            if sigA.get(f"{ssp}_ids_reseller",0) != sigB.get(f"{ssp}_ids_reseller",0):
                return False
    return True

def binary_search_change(snaps, get_signature_fn, left_idx, right_idx, max_iterations=30):
    if left_idx >= right_idx:
        return None
    sigL = get_signature_fn(left_idx)
    sigR = get_signature_fn(right_idx)
    if signatures_equal(sigL, sigR):
        return None
    lo = left_idx
    hi = right_idx
    iteration = 0
    while (hi - lo) > 1 and iteration < max_iterations:
        mid = (lo + hi) // 2
        sigM = get_signature_fn(mid)
        if sigM is None:
            moved = False
            for d in (-1,1,-2,2):
                nmid = mid + d
                if nmid > lo and nmid < hi:
                    sigM = get_signature_fn(nmid)
                    if sigM is not None:
                        mid = nmid
                        moved = True
                        break
            if not moved:
                break
        if signatures_equal(sigL, sigM):
            lo = mid
            sigL = sigM
        else:
            hi = mid
            sigR = sigM
        iteration += 1
    return lo, hi

# -----------------------
# High-level domain analysis
# -----------------------
def analyze_domain(domain, from_date, to_date):
    results = {
        "domain": domain,
        "from": from_date,
        "to": to_date,
        "snapshots_count": 0,
        "per_year_counts": {},
        "longest_gap_days": None,
        "events": [],
        "snapshots_records": [],
        "ads_managers": set(),
        "country_clues": Counter(),
        "ssp_id_rows": [],   # rows for SSP_IDs sheet per domain
        "host_rows": [],     # rows for Hosts sheet per snapshot
        "human_summary": []
    }

    ads_path = domain.rstrip('/') + "/ads.txt"
    snaps = cdx_query(ads_path, from_ts=from_date, to_ts=to_date, filters=["statuscode:200","statuscode:301","statuscode:302"], limit=20000)
    sleep_random()
    used_subdomain_mode = False
    if not snaps:
        wildcard_pattern = domain.rstrip('/') + "/*"
        snaps = cdx_query(wildcard_pattern, from_ts=from_date, to_ts=to_date, filters=["statuscode:200","statuscode:301","statuscode:302"], limit=20000)
        used_subdomain_mode = True
    if not snaps:
        return results

    results["snapshots_count"] = len(snaps)

    by_year = Counter()
    last_ts = None
    max_gap = 0
    for s in snaps:
        y = int(s.timestamp[:4])
        by_year[y] += 1
        if last_ts:
            prev = datetime.strptime(last_ts, "%Y%m%d%H%M%S")
            curr = datetime.strptime(s.timestamp, "%Y%m%d%H%M%S")
            gap = (curr - prev).days
            if gap > max_gap:
                max_gap = gap
        last_ts = s.timestamp
    cur_year = datetime.utcnow().year
    per_year = {}
    for y in range(2020, cur_year+1):
        per_year[y] = by_year.get(y, 0)
    results["per_year_counts"] = per_year
    results["longest_gap_days"] = max_gap

    snaps_reduced = snaps
    if len(snaps) > MAX_SNAPSHOT_FETCH:
        snaps_reduced = reduce_snapshots_to_daily(snaps)

    sampled = evenly_sample_by_year(snaps_reduced, per_year=SNAPSHOTS_PER_YEAR)
    if snaps_reduced:
        if snaps_reduced[0] not in sampled:
            sampled.insert(0, snaps_reduced[0])
        if snaps_reduced[-1] not in sampled:
            sampled.append(snaps_reduced[-1])

    # prepare lazy signature cache and helper functions
    sig_cache = {}
    median_len = median_length_of_snapshots(snaps_reduced)

    def get_sig_by_index_in_reduced(idx):
        s = snaps_reduced[idx]
        key = (s.timestamp, s.digest)
        if key in sig_cache:
            return sig_cache[key]
        fetched = None
        try:
            status, text, final = wayback_fetch(s, follow_redirects=True)
            time.sleep(SLEEP_MIN)
            suspect = False
            if status == 200 and text:
                entries = parse_ads_txt(text)
                sig = build_signature_from_ads(entries)
                if not text.endswith("\n"):
                    suspect = True
                if is_length_suspicious(s, snaps_reduced):
                    suspect = True
                fetched = {"signature": sig, "entries": entries, "length": s.length, "digest": s.digest, "suspect": suspect, "raw": text, "timestamp": s.timestamp, "original": s.original}
            else:
                fetched = {"signature": None, "entries": [], "length": s.length, "digest": s.digest, "suspect": True, "raw": None, "timestamp": s.timestamp, "original": s.original}
        except Exception:
            fetched = {"signature": None, "entries": [], "length": s.length, "digest": s.digest, "suspect": True, "raw": None, "timestamp": s.timestamp, "original": s.original}
        sig_cache[key] = fetched
        return fetched

    timestamps_to_index = {s.timestamp:i for i,s in enumerate(snaps_reduced)}
    sampled_indices = []
    for samp in sampled:
        if samp.timestamp in timestamps_to_index:
            sampled_indices.append(timestamps_to_index[samp.timestamp])
    sampled_indices = sorted(set(sampled_indices))

    windows_to_check = []
    for i in range(len(sampled_indices)-1):
        a_idx = sampled_indices[i]
        b_idx = sampled_indices[i+1]
        sigA = get_sig_by_index_in_reduced(a_idx)["signature"]
        sigB = get_sig_by_index_in_reduced(b_idx)["signature"]
        if not signatures_equal(sigA, sigB):
            windows_to_check.append((a_idx, b_idx))

    events = []
    snapshots_records = []

    # ensure we have sig_cache entries for all snaps_reduced for reporting (hosts/ids)
    for idx in range(len(snaps_reduced)):
        _ = get_sig_by_index_in_reduced(idx)

    # For each detected window, run binary search
    for (a_idx, b_idx) in windows_to_check:
        res = binary_search_change(snaps_reduced, lambda idx: get_sig_by_index_in_reduced(idx)["signature"], a_idx, b_idx)
        if res is None:
            continue
        lo, hi = res
        sig_lo = get_sig_by_index_in_reduced(lo)
        sig_hi = get_sig_by_index_in_reduced(hi)
        changed_ssps = []
        for ssp in SSP_DEFS.keys():
            a_has = bool((sig_lo["signature"] or {}).get(f"{ssp}_ads"))
            b_has = bool((sig_hi["signature"] or {}).get(f"{ssp}_ads"))
            if a_has != b_has:
                changed_ssps.append(ssp)
        dtA = datetime.strptime(snaps_reduced[lo].timestamp, "%Y%m%d%H%M%S")
        dtB = datetime.strptime(snaps_reduced[hi].timestamp, "%Y%m%d%H%M%S")
        window = [dtA.strftime("%Y-%m-%d"), dtB.strftime("%Y-%m-%d")]
        for ssp in changed_ssps:
            a_has = bool((sig_lo["signature"] or {}).get(f"{ssp}_ads"))
            b_has = bool((sig_hi["signature"] or {}).get(f"{ssp}_ads"))
            ev_type = "added" if (not a_has and b_has) else "removed" if (a_has and not b_has) else "changed"
            events.append({
                "domain": domain,
                "ssp": ssp,
                "type": ev_type,
                "window_from": window[0],
                "window_to": window[1],
                "index_lo": lo,
                "index_hi": hi,
                "sig_lo": sig_lo["signature"],
                "sig_hi": sig_hi["signature"]
            })
            if ev_type == "added":
                results["human_summary"].append(f"{ssp.upper()} foi ADICIONADO como SSP para {domain} entre {window[0]} e {window[1]}.")
            elif ev_type == "removed":
                results["human_summary"].append(f"{ssp.upper()} foi REMOVIDO como SSP para {domain} entre {window[0]} e {window[1]}.")
            else:
                results["human_summary"].append(f"{ssp.upper()} mudou assinatura (detalhes disponíveis) para {domain} entre {window[0]} e {window[1]}.")
        snapshots_records.append({
            "domain": domain,
            "pos_lo": lo,
            "pos_hi": hi,
            "timestamp_lo": snaps_reduced[lo].timestamp,
            "timestamp_hi": snaps_reduced[hi].timestamp,
            "digest_lo": snaps_reduced[lo].digest,
            "digest_hi": snaps_reduced[hi].digest,
            "length_lo": snaps_reduced[lo].length,
            "length_hi": snaps_reduced[hi].length,
            "suspect_lo": sig_lo["suspect"],
            "suspect_hi": sig_hi["suspect"]
        })

    # Build presence_by_index for exclusivity detection
    presence_by_index = []
    for idx, s in enumerate(snaps_reduced):
        fetched = get_sig_by_index_in_reduced(idx)
        sig = fetched["signature"]
        count = 0
        for ssp in SSP_DEFS.keys():
            if sig and sig.get(f"{ssp}_ads"):
                count += 1
        presence_by_index.append((idx, s.timestamp, count, sig, fetched))

    for i in range(1, len(presence_by_index)):
        prev = presence_by_index[i-1]
        cur = presence_by_index[i]
        prev_count = prev[2]; cur_count = cur[2]
        if prev_count >= 8 and cur_count == 1:
            cur_sig = cur[3] or {}
            remaining = [ssp for ssp in SSP_DEFS.keys() if cur_sig.get(f"{ssp}_ads")]
            if len(remaining) == 1:
                ssp_rem = remaining[0]
                consec = 1
                j = i+1
                while j < len(presence_by_index):
                    nxt = presence_by_index[j]
                    nxt_count = nxt[2]
                    nxt_sig = nxt[3] or {}
                    if nxt_count == 1 and nxt_sig.get(f"{ssp_rem}_ads"):
                        prev_ts = datetime.strptime(presence_by_index[j-1][1], "%Y%m%d%H%M%S")
                        cur_ts = datetime.strptime(nxt[1], "%Y%m%d%H%M%S")
                        if (cur_ts - prev_ts).days >= EXCLUSIVITY_MIN_PERSIST_DAYS or (consec+1) >= EXCLUSIVITY_MIN_CONSECUTIVE:
                            consec += 1
                            j += 1
                            continue
                        else:
                            break
                    else:
                        break
                if consec >= EXCLUSIVITY_MIN_CONSECUTIVE:
                    dt_prev = datetime.strptime(prev[1], "%Y%m%d%H%M%S").strftime("%Y-%m-%d")
                    dt_cur = datetime.strptime(cur[1], "%Y%m%d%H%M%S").strftime("%Y-%m-%d")
                    events.append({
                        "domain": domain,
                        "ssp": ssp_rem,
                        "type": "potential_exclusivity",
                        "window_from": dt_prev,
                        "window_to": dt_cur,
                        "details": {"consecutive": consec}
                    })
                    results["human_summary"].append(f"POTENCIAL EXCLUSIVIDADE: {ssp_rem.upper()} permaneceu sozinho para {domain} entre {dt_prev} e {dt_cur} (consecutivos={consec}).")

    # collect ads managers and country clues by sampling a subset (we already fetched all reduced)
    for idx, s in enumerate(snaps_reduced):
        fetched = get_sig_by_index_in_reduced(idx)
        raw = fetched.get("raw")
        if raw:
            tl = raw.lower()
            for m in ADS_MANAGERS:
                if m in tl:
                    results["ads_managers"].add(m)
    results["events"] = events
    results["snapshots_records"] = snapshots_records
    results["used_subdomain_mode"] = used_subdomain_mode
    results["ads_managers"] = list(results["ads_managers"])

    # Build SSP_IDs rows and Hosts rows from sig_cache
    ssp_agg = {ssp: {"direct": set(), "reseller": set()} for ssp in SSP_DEFS.keys()}
    last_seen_map = {ssp: None for ssp in SSP_DEFS.keys()}

    for idx, s in enumerate(snaps_reduced):
        fetched = get_sig_by_index_in_reduced(idx)
        sig = fetched.get("signature")
        ts = s.timestamp
        for ssp in SSP_DEFS.keys():
            direct_list = sig.get(f"{ssp}_list_direct", []) if sig else []
            res_list = sig.get(f"{ssp}_list_reseller", []) if sig else []
            for sid in direct_list:
                ssp_agg[ssp]["direct"].add(sid)
                results["host_rows"].append({
                    "domain": domain,
                    "snapshot_ts": ts,
                    "ssp": ssp,
                    "role": "DIRECT",
                    "seller_id": sid
                })
                last_seen_map[ssp] = ts
            for sid in res_list:
                ssp_agg[ssp]["reseller"].add(sid)
                results["host_rows"].append({
                    "domain": domain,
                    "snapshot_ts": ts,
                    "ssp": ssp,
                    "role": "RESELLER",
                    "seller_id": sid
                })
                last_seen_map[ssp] = ts

    # build ssp_id_rows per domain/ssp
    for ssp in SSP_DEFS.keys():
        dir_ids = sorted(list(ssp_agg[ssp]["direct"]))
        res_ids = sorted(list(ssp_agg[ssp]["reseller"]))
        total_unique = len(dir_ids) + len(res_ids)
        results["ssp_id_rows"].append({
            "domain": domain,
            "ssp": ssp,
            "total_unique_ids": total_unique,
            "ids_direct_count": len(dir_ids),
            "ids_reseller_count": len(res_ids),
            "ids_direct_list": ",".join(dir_ids),
            "ids_reseller_list": ",".join(res_ids),
            "last_seen_ts": last_seen_map.get(ssp)
        })

    if not results["human_summary"]:
        results["human_summary"].append(f"Sem mudanças detectadas para {domain} no período {from_date}..{to_date} (baseado nas amostras analisadas).")

    return results

# -----------------------
# Reporting helpers
# -----------------------
def generate_report(all_results, out_xlsx=OUT_XLSX):
    summary_rows = []
    timeline_rows = []
    adsdetail_rows = []
    host_rows = []
    ssp_id_rows = []
    managers_rows = []
    human_rows = []
    country_rows = []

    for res in all_results:
        domain = res["domain"]
        summary_rows.append({
            "domain": domain,
            "from": res.get("from"),
            "to": res.get("to"),
            "snapshots_count": res.get("snapshots_count",0),
            "longest_gap_days": res.get("longest_gap_days", None),
            "ads_managers": ",".join(res.get("ads_managers",[])),
            "per_year_counts": json.dumps(res.get("per_year_counts",{})),
            "used_subdomain_mode": res.get("used_subdomain_mode", False)
        })
        for ev in res.get("events",[]):
            timeline_rows.append({
                "domain": domain,
                "ssp": ev.get("ssp"),
                "type": ev.get("type"),
                "window_from": ev.get("window_from"),
                "window_to": ev.get("window_to"),
                "details": json.dumps(ev.get("details", {})) if isinstance(ev.get("details", {}), dict) else json.dumps({"sig_lo": ev.get("sig_lo"), "sig_hi": ev.get("sig_hi")})
            })
        for sr in res.get("snapshots_records",[]):
            adsdetail_rows.append({
                "domain": domain,
                "timestamp_lo": sr.get("timestamp_lo"),
                "timestamp_hi": sr.get("timestamp_hi"),
                "digest_lo": sr.get("digest_lo"),
                "digest_hi": sr.get("digest_hi"),
                "length_lo": sr.get("length_lo"),
                "length_hi": sr.get("length_hi"),
                "suspect_lo": sr.get("suspect_lo"),
                "suspect_hi": sr.get("suspect_hi")
            })
        for m in res.get("ads_managers",[]):
            managers_rows.append({"domain": domain, "manager": m})
        for hr in res.get("human_summary", []):
            human_rows.append({"domain": domain, "message": hr})
        for h in res.get("host_rows", []):
            host_rows.append(h)
        for srow in res.get("ssp_id_rows", []):
            ssp_id_rows.append(srow)

    # write Excel with new sheets
    with pd.ExcelWriter(out_xlsx, engine='openpyxl') as writer:
        pd.DataFrame(summary_rows).to_excel(writer, sheet_name="Summary", index=False)
        pd.DataFrame(timeline_rows).to_excel(writer, sheet_name="Timeline", index=False)
        pd.DataFrame(adsdetail_rows).to_excel(writer, sheet_name="AdsSnapshots", index=False)
        pd.DataFrame(ssp_id_rows).to_excel(writer, sheet_name="SSP_IDs", index=False)
        pd.DataFrame(host_rows).to_excel(writer, sheet_name="Hosts", index=False)
        pd.DataFrame(managers_rows).to_excel(writer, sheet_name="AdsManagers", index=False)
        pd.DataFrame(human_rows).to_excel(writer, sheet_name="HumanSummary", index=False)
    print(f"Wrote report -> {out_xlsx}")

# -----------------------
# CLI / Orchestration
# -----------------------
def main():
    parser = argparse.ArgumentParser(description="Wayback SPO Analyzer: CDX + Bisection for ads.txt changes")
    parser.add_argument("--domains-file", default="principaldomains", help="file with domains one per line")
    parser.add_argument("--log-file", default=ANALYSIS_LOG, help="analysis log JSON path")
    parser.add_argument("--from-default", default=START_DATE, help="default start YYYYMMDD (inclusive)")
    parser.add_argument("--sleep-min", type=float, default=SLEEP_MIN)
    parser.add_argument("--sleep-max", type=float, default=SLEEP_MAX)
    parser.add_argument("--out", default=OUT_XLSX)
    args = parser.parse_args()

    global SLEEP_MIN, SLEEP_MAX
    SLEEP_MIN = args.sleep_min
    SLEEP_MAX = args.sleep_max

    domains = read_domains(args.domains_file)
    log = load_log(args.log_file)
    all_results = []

    today = datetime.utcnow().strftime("%Y%m%d")
    for dom in domains:
        print(f"[INFO] Domain {dom}")
        last_checked = log.get(dom, {}).get("last_checked")
        if last_checked:
            from_date = last_checked
        else:
            from_date = args.from_default
        to_date = today
        try:
            res = analyze_domain(dom, from_date, to_date)
            all_results.append(res)
            # update log for this domain (set last_checked to to_date inclusive)
            log[dom] = {"last_checked": to_date, "last_run": datetime.utcnow().isoformat()}
            time.sleep(SLEEP_MIN)
        except Exception as e:
            print(f"[ERR] Domain {dom} analysis error: {e}")
            continue

    generate_report(all_results, out_xlsx=args.out)
    save_log(log, args.log_file)

if __name__ == "__main__":
    main()
