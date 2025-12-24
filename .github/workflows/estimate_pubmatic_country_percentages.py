#!/usr/bin/env python3
"""
estimate_pubmatic_country_percentages_revenue.py

Aggressive scanning + Prebid parsing + ads.txt & sellers.json validation + revenue-weighting + optional origin simulation + optional MaxMind GeoIP.

Usage examples:
  python3 estimate_pubmatic_country_percentages_revenue.py \
    --domains-file principaldomains \
    --out pubmatic_country_revenue_estimates.xlsx \
    --total-requests 1000 \
    --alpha 5.0 \
    --timeout 10 \
    --simulate "JP:203.0.113.5:ja-JP" "IN:203.0.113.6:en-IN" \
    --maxmind-db /path/to/GeoLite2-City.mmdb

Notes:
 - Simulation entries: format "CC:IP:Accept-Language" (CC optional, used just for labeling).
 - priors.csv optional: header: domain,US,GB,AU,JP,IN,... values fractions (not required to sum to 1).
 - If --maxmind-db provided and geoip2 installed, uses local DB for geolocation, else uses ip-api.com (free).
"""

import argparse
import requests
import socket
import re
import time
import csv
import json
import sys
import math
import traceback
from collections import Counter, defaultdict
from urllib.parse import urlparse, urljoin
import pandas as pd

# Optional geoip2
try:
    import geoip2.database
    GEOIP2_AVAILABLE = True
except Exception:
    GEOIP2_AVAILABLE = False

# -----------------------
# Config / heuristics
# -----------------------
PUB_KEYWORDS = ['pubmatic', 'openwrap', 'openwrapsdk', 'hb.pubmatic', 'ads.pubmatic']
GENERAL_KEYWORDS = ['prebid', 'pbjs', 'bidder', 'bid', 'adUnit', 'floor', 'floorPrice', 'currency', 'countries', 'appliesTo']
DOMAIN_RE = re.compile(r'([a-z0-9\-_\.]+\.[a-z]{2,6})', re.IGNORECASE)
# pbjs objects heuristics
PBJS_OBJ_RE = re.compile(r'(pbjs\.adUnits\s*=\s*|pbjs\.que\.push\(|var\s+pbjs\s*=)', re.IGNORECASE)
JSON_LIKE_RE = re.compile(r'(\{(?:[^{}]|(?R))*\})', re.DOTALL)  # attempt to find JSON-like blocks (greedy, may be noisy)
# prebid adUnit / bidder keys
ADUNIT_KEYWORDS = ['adUnits', 'adUnitCode', 'mediaTypes', 'bids', 'params', 'floor', 'floorPrice', 'currency', 'geo', 'countries', 'appliesTo']

# geo API (fallback)
GEO_API = "http://ip-api.com/json/{ip}?fields=status,countryCode,query,message"
IPAPI_DELAY = 0.45  # seconds between calls to avoid aggressive hitting

# default prior for unknowns (uniform fallback)
DEFAULT_ALPHA = 5.0

# scoring weights (tweakable)
WEIGHT_PREBID_ADUNIT = 2.0           # each adUnit occurrence
WEIGHT_PREBID_FLOOR = 0.5            # multiplier per floor amount (higher floor increases score)
WEIGHT_ADSTXT_DIRECT = 4.0          # direct entry in ads.txt
WEIGHT_ADSTXT_RESELLER = 1.5        # reseller entry
WEIGHT_SELLERS_VALIDATION = 3.0     # validated sellers.json increases confidence/weight
WEIGHT_SIMULATION_VARIANT = 1.2     # multiplier if signal only seen under simulation
SMOOTH_ALPHA = 1.0                   # Dirichlet prior smoothing for revenue proportions

# polite delay for HTTP fetch to remote sites (homepage)
FETCH_DELAY = 0.12

# -----------------------
# Helpers
# -----------------------
def fetch_url(url, timeout=10, headers=None, allow_redirects=True):
    headers = headers or {'User-Agent': 'Mozilla/5.0 (compatible; PubMaticEstimator/1.0)'}
    try:
        r = requests.get(url, headers=headers, timeout=timeout, allow_redirects=allow_redirects)
        return r.status_code, r.text, r.url
    except Exception:
        return None, None, None

def extract_hosts_aggressive(html, base_domain=None):
    """
    Aggressively extract hosts and domain-like tokens from HTML and inline scripts.
    Returns list of host strings (lowercased, deduped).
    """
    if not html:
        return []
    hosts = []
    lower = html.lower()

    # 1) direct pubmatic-like hostnames
    for m in re.finditer(r'([a-z0-9\-_\.]*pubmatic[a-z0-9\-_\.]*\.[a-z]{2,6})', html, flags=re.I):
        hosts.append(m.group(1).lower())

    # 2) script src / url occurrences
    for m in re.finditer(r'(https?://[^\s"\'>]+)', html, flags=re.I):
        url = m.group(1)
        try:
            p = urlparse(url)
            host = p.hostname
            if host:
                host = host.lower()
                if any(k in host for k in PUB_KEYWORDS + GENERAL_KEYWORDS):
                    hosts.append(host)
                else:
                    if 'pubmatic' in url.lower():
                        hosts.append(host)
        except:
            pass

    # 3) JSON-like segments near prebid keywords
    for kw in ['pbjs','pbjs.adUnits','pbjs.que','bidder','bid','adUnit','floor','floorPrice']:
        for m in re.finditer(r'.{0,500}'+re.escape(kw)+r'.{0,500}', html, flags=re.I|re.S):
            seg = m.group(0)
            for mm in DOMAIN_RE.finditer(seg):
                h = mm.group(1).lower()
                if any(c.isalpha() for c in h):
                    hosts.append(h)

    # 4) general domain tokens but keep if contain keywords
    for mm in DOMAIN_RE.finditer(html):
        h = mm.group(1).lower()
        if any(k in h for k in PUB_KEYWORDS + GENERAL_KEYWORDS):
            hosts.append(h)

    # dedupe preserve order
    seen = set()
    out = []
    for h in hosts:
        if not h:
            continue
        if ':' in h:
            h = h.split(':',1)[0]
        if h not in seen:
            seen.add(h)
            out.append(h)
    if not out and base_domain:
        out.append(base_domain.lower())
    return out

def resolve_host(host):
    ips = set()
    try:
        infos = socket.getaddrinfo(host, None)
        for info in infos:
            addr = info[4][0]
            if ':' in addr:
                continue
            ips.add(addr)
    except Exception:
        pass
    return list(ips)

# Geo helpers: either MaxMind (geoip2) or ip-api
class GeoResolver:
    def __init__(self, maxmind_db_path=None, delay=IPAPI_DELAY):
        self.delay = delay
        self.use_maxmind = False
        self.maxmind_reader = None
        if maxmind_db_path and GEOIP2_AVAILABLE:
            try:
                self.maxmind_reader = geoip2.database.Reader(maxmind_db_path)
                self.use_maxmind = True
            except Exception as e:
                print(f"[WARN] Could not open MaxMind DB at {maxmind_db_path}: {e}")
                self.maxmind_reader = None
                self.use_maxmind = False
        self.cache = {}

    def lookup(self, ip):
        if not ip:
            return ''
        if ip in self.cache:
            return self.cache[ip]
        if self.use_maxmind and self.maxmind_reader:
            try:
                rec = self.maxmind_reader.city(ip)
                cc = rec.country.iso_code or ''
                self.cache[ip] = cc or ''
                return cc or ''
            except Exception:
                # fallback to ip-api
                pass
        # fallback ip-api
        try:
            url = GEO_API.format(ip=ip)
            r = requests.get(url, timeout=8)
            if r.status_code == 200:
                j = r.json()
                if j.get('status') == 'success':
                    cc = j.get('countryCode','') or ''
                    self.cache[ip] = cc
                    time.sleep(self.delay)
                    return cc
        except Exception:
            pass
        self.cache[ip] = ''
        time.sleep(self.delay)
        return ''

# -----------------------
# Prebid / JS heuristics
# -----------------------
def try_parse_json_like(s):
    """
    Try to coerce JSON-like string to real JSON and parse.
    - Replace single quotes with double where safe.
    - Replace trailing commas.
    This is heuristic and may fail.
    """
    if not s or not isinstance(s, str):
        return None
    # attempt: find first { and last matching } naive
    start = s.find('{')
    end = s.rfind('}')
    if start == -1 or end == -1 or end <= start:
        return None
    candidate = s[start:end+1]
    # common fixes
    candidate = candidate.replace('\r',' ').replace('\n',' ')
    # convert JS true/false/null capitalization
    candidate = re.sub(r'\bundefined\b', 'null', candidate)
    candidate = re.sub(r'\b([A-Za-z0-9_]+)\s*:', r'"\1":', candidate)  # attempt to quote keys (aggressive!)
    candidate = candidate.replace("'", '"')
    # remove trailing commas before } or ]
    candidate = re.sub(r',\s*([\]\}])', r'\1', candidate)
    try:
        return json.loads(candidate)
    except Exception:
        # fallback: try ast literal eval? (dangerous) - skip
        return None

def extract_prebid_signals(html):
    """
    Search for pbjs/adUnits/bidderSettings instances and extract:
    - number_of_adunits
    - per-country floors (if found)
    - currencies encountered
    - geofencing country lists
    Returns dict with aggregated signals and list of raw findings.
    """
    out = {
        "adunit_count": 0,
        "floors": [],           # list of (amount, currency, maybe country context)
        "currencies": set(),
        "geo_clues": set(),
        "raw_matches": []
    }
    if not html:
        return out
    text = html

    # quick find: locate 'pbjs' occurrences and extract a window
    for m in re.finditer(r'(pbjs\.adUnits|pbjs\.que|pbjs\.addAdUnits|bidderSettings|bidderConfig)', text, flags=re.I):
        start = max(0, m.start()-500)
        end = min(len(text), m.end()+2000)
        seg = text[start:end]
        out["raw_matches"].append(seg[:1000])
        parsed = try_parse_json_like(seg)
        # If parsed is dict/ list, attempt to find floors and countries
        if parsed:
            # search adUnits
            def recurse(obj):
                if isinstance(obj, dict):
                    for k,v in obj.items():
                        lk = str(k).lower()
                        if lk in ('adunits','adunits','adunit'):
                            if isinstance(v, list):
                                out["adunit_count"] += len(v)
                        # floors within dict
                        if 'floor' in lk or 'floorprice' in lk or 'cpm' in lk:
                            try:
                                val = float(v) if v not in (None, '') else None
                                if val is not None:
                                    # try to find currency in same dict
                                    curr = obj.get('currency') or obj.get('curr') or obj.get('currencyCode') or None
                                    if curr:
                                        out["currencies"].add(str(curr).upper())
                                    out["floors"].append((val, (curr or '').upper()))
                            except Exception:
                                pass
                        # geo lists
                        if lk in ('geo','countries','appliesto','appliesTo'.lower()):
                            # v might be dict or list
                            if isinstance(v, list):
                                for it in v:
                                    try:
                                        out["geo_clues"].add(str(it).upper())
                                    except:
                                        pass
                            elif isinstance(v, dict):
                                for it in v.get('countries',[]):
                                    out["geo_clues"].add(str(it).upper())
                        recurse(v)
                elif isinstance(obj, list):
                    for it in obj:
                        recurse(it)
            try:
                recurse(parsed)
            except Exception:
                pass
        else:
            # fallback regex: floors like "floor": 2.00 or "floorPrice": "2.00" or "floor": {"value":2,...}
            for fm in re.finditer(r'(?:"|\'|)floor(?:Price|_price|)\s*"\s*[:=]\s*(?:"|\')?([0-9]+(?:\.[0-9]+)?)', seg, flags=re.I):
                try:
                    val = float(fm.group(1))
                    out["floors"].append((val,''))
                except:
                    pass
            # currency codes
            for cm in re.finditer(r'"\s*currency\s*"\s*:\s*"(.*?)"', seg, flags=re.I):
                out["currencies"].add(cm.group(1).upper())
            # countries arrays
            for ccm in re.finditer(r'countries\s*[:=]\s*\[([^\]]+)\]', seg, flags=re.I):
                arr = ccm.group(1)
                for code in re.findall(r'["\']?([A-Za-z]{2})["\']?', arr):
                    out["geo_clues"].add(code.upper())

    # final sanity: dedupe currencies
    out["currencies"] = set([c for c in out["currencies"] if c])
    return out

# -----------------------
# ads.txt & sellers.json helpers
# -----------------------
def fetch_ads_txt(domain, timeout=10):
    """
    Tries to fetch domain/ads.txt (https then http). Returns (status, text, final_url).
    """
    urls = [f'https://{domain}/ads.txt', f'http://{domain}/ads.txt']
    for u in urls:
        try:
            code, text, final = fetch_url(u, timeout=timeout)
            if code == 200 and text:
                return 200, text, final
            # if redirect etc we might still want to follow: fetch_url follows redirects by default
            if code in (301,302) and text:
                return code, text, final
        except Exception:
            pass
    return None, None, None

def parse_ads_txt_entries(ads_txt):
    """
    Parse ads.txt lines to list of tuples (adsystem_domain, seller_id, rel)
    Ignores commented lines starting with #
    """
    if not ads_txt:
        return []
    entries = []
    for ln in ads_txt.splitlines():
        ln = ln.strip()
        if not ln:
            continue
        if ln.startswith('#'):
            continue
        # remove inline comment
        if '#' in ln:
            ln = ln.split('#',1)[0].strip()
        parts = [p.strip() for p in ln.split(',')]
        if len(parts) >= 3:
            entries.append((parts[0].lower(), parts[1].lower(), parts[2].upper()))
    return entries

def try_fetch_sellers_json_for_adsystem(adsystem_domain, timeout=8):
    """
    Attempt to fetch sellers.json for an adsystem domain.
    Try a few common locations (https://adsystem/sellers.json, https://adsystem/.well-known/sellers.json)
    Returns parsed JSON or None.
    """
    candidates = [
        f"https://{adsystem_domain}/sellers.json",
        f"https://{adsystem_domain}/.well-known/sellers.json",
        f"http://{adsystem_domain}/sellers.json",
        f"http://{adsystem_domain}/.well-known/sellers.json",
    ]
    for u in candidates:
        try:
            code, text, final = fetch_url(u, timeout=timeout)
            if code == 200 and text:
                try:
                    j = json.loads(text)
                    return j, u
                except Exception:
                    # try to extract JSON block
                    m = re.search(r'(\{.*\})', text, flags=re.S)
                    if m:
                        try:
                            j = json.loads(m.group(1))
                            return j, u
                        except:
                            pass
            # continue on other candidates
        except Exception:
            pass
    return None, None

# -----------------------
# Revenue-weighting logic
# -----------------------
def compute_revenue_scores(domain_signals, total_requests, priors_for_domain=None, alpha=SMOOTH_ALPHA):
    """
    domain_signals: dict containing:
        - observed_countries_counter: Counter(country->count) from host IPs
        - hosts_detail: list of {host, ip, country}
        - prebid: prebid signals dict (adunit_count, floors, currencies, geo_clues)
        - ads_txt_entries: list of entries (adsys, seller, rel)
        - ads_txt_pubmatic_ids: list of (seller_id, role)
        - sellers_validation: dict (seller_id -> validated_country or None)
        - simulation_variants: list of variant dicts (same structure as base signals)
    Returns:
        posterior fractions per country and estimated request counts per country
    """
    # We'll build "score_by_country" from several components:
    # 1) IP-based observed_counts (low-weight fallback)
    # 2) prebid signals: floors and geofencing -> strong signal for countries listed
    # 3) ads.txt direct/reseller: seller entries mapped to country via sellers.json -> strong signal
    # 4) simulation-only signals get a slight multiplier

    # initialize
    score = defaultdict(float)
    observed = domain_signals.get('observed_countries', Counter())
    hosts_detail = domain_signals.get('hosts_detail', [])
    prebid = domain_signals.get('prebid', {})
    ads_entries = domain_signals.get('ads_txt_entries', [])
    pubmatic_ids = domain_signals.get('ads_txt_pubmatic_ids', [])
    sellers_valid = domain_signals.get('sellers_validation', {})  # seller_id -> country code or None
    sim_variants = domain_signals.get('simulation_variants', [])

    # Component A: IP observed counts (low weight): distribute weight proportional to observed counts
    observed_total = sum(observed.values())
    if observed_total > 0:
        for cc, cnt in observed.items():
            score[cc] += cnt * 0.6  # small factor - observed server location -> signal but weaker

    # Component B: Prebid signals
    # If prebid has geo_clues, boost those countries strongly
    try:
        adunit_count = int(prebid.get('adunit_count', 0))
    except:
        adunit_count = 0
    floors = prebid.get('floors', [])  # list of (amount, currency)
    geo_clues = set(prebid.get('geo_clues', []))
    # number of floors and avg floor amount
    flo_val = 0.0
    flo_count = 0
    for f,c in floors:
        try:
            if f and float(f) > 0:
                flo_val += float(f)
                flo_count += 1
        except:
            pass
    avg_floor = (flo_val / flo_count) if flo_count else 0.0
    # apply to geo clues
    if geo_clues:
        for cc in geo_clues:
            score[cc] += WEIGHT_PREBID_ADUNIT * max(1, adunit_count) + WEIGHT_PREBID_FLOOR * avg_floor * (1 + flo_count/2)
    else:
        # if no geo clues but floors exist, distribute by currency inference (if currency is JPY -> JP)
        currencies = prebid.get('currencies', set())
        for cur in currencies:
            if cur == 'JPY':
                score['JP'] += WEIGHT_PREBID_FLOOR * avg_floor * 1.5
            elif cur == 'GBP':
                score['GB'] += WEIGHT_PREBID_FLOOR * avg_floor * 1.2
            elif cur == 'AUD':
                score['AU'] += WEIGHT_PREBID_FLOOR * avg_floor * 1.2
            # else keep as small hint
        # also adunit_count as general signal distributed proportionally to observed (fallback)
        if observed_total > 0:
            for cc,cnt in observed.items():
                score[cc] += (cnt/observed_total) * WEIGHT_PREBID_ADUNIT * adunit_count

    # Component C: ads.txt - direct / reseller counts and sellers.json validation
    # For pubmatic entries: if sellers.json gives country for that seller (publisher account), weight that country
    for seller_id, role in pubmatic_ids:
        # find validated country
        validated_cc = sellers_valid.get(seller_id)
        weight = WEIGHT_ADSTXT_DIRECT if role == 'DIRECT' else WEIGHT_ADSTXT_RESELLER
        if validated_cc:
            score[validated_cc] += weight * (2.0 if role == 'DIRECT' else 1.0) + WEIGHT_SELLERS_VALIDATION
        else:
            # no validation: use observed IPs as fallback: increase score of most common observed
            if observed_total > 0:
                # increment proportionally
                for cc, cnt in observed.items():
                    score[cc] += (cnt/observed_total) * weight

    # Component D: simulation variants: if signals appear only under simulation, add them with sim multiplier
    for var in sim_variants:
        # each var should have 'observed_countries', 'prebid', 'ads_txt_pubmatic_ids', 'label'
        v_obs = var.get('observed_countries', Counter())
        v_pre = var.get('prebid', {})
        v_pub = var.get('ads_txt_pubmatic_ids', [])
        # boost countries seen here
        for cc, cnt in v_obs.items():
            score[cc] += cnt * 0.8 * WEIGHT_SIMULATION_VARIANT
        # prebid geo clues
        g = set(v_pre.get('geo_clues', []))
        if g:
            for cc in g:
                score[cc] += WEIGHT_PREBID_ADUNIT * 0.7 * WEIGHT_SIMULATION_VARIANT
        # pubmatic ids in sellers (rare)
        for sid, role in v_pub:
            vc = sellers_valid.get(sid)
            if vc:
                score[vc] += WEIGHT_ADSTXT_DIRECT * 0.5 * WEIGHT_SIMULATION_VARIANT

    # If we still have zero scores, fallback to priors or uniform distribution across observed countries
    if not score:
        if priors_for_domain:
            # use prior probabilities scaled by a factor
            for cc, frac in priors_for_domain.items():
                score[cc] = frac * 10.0
        elif observed_total > 0:
            for cc,cnt in observed.items():
                score[cc] = float(cnt)
        else:
            # no signal at all -> unknown
            score['UNKNOWN'] = 1.0

    # Convert defaultdict to normal dict and normalize to fractions
    score_dict = dict(score)
    # Sum
    total_score = sum(score_dict.values()) if score_dict else 0.0
    if total_score <= 0:
        # fallback uniform
        keys = list(score_dict.keys()) or ['UNKNOWN']
        for k in keys:
            score_dict[k] = 1.0
        total_score = sum(score_dict.values())

    posterior = {k: (v/total_score) for k,v in score_dict.items()}

    # Apply Dirichlet-like smoothing with priors_for_domain if provided
    if priors_for_domain:
        # ensure same key set
        for k in list(posterior.keys()):
            if k not in priors_for_domain:
                priors_for_domain[k] = 0.0
        smooth_counts = {}
        for k in posterior.keys():
            smooth_counts[k] = alpha * priors_for_domain.get(k, 0.0) + posterior.get(k, 0.0)
        s = sum(smooth_counts.values()) or 1.0
        posterior = {k: smooth_counts[k]/s for k in smooth_counts}

    # compute est counts
    est_by_country = {k: int(round(v * total_requests)) for k,v in posterior.items()}
    # ensure sum equals total_requests by adjusting largest
    s_est = sum(est_by_country.values())
    if s_est != total_requests:
        diff = total_requests - s_est
        if posterior:
            largest = max(posterior.items(), key=lambda t: t[1])[0]
            est_by_country[largest] += diff

    return posterior, est_by_country, score_dict

# -----------------------
# Orchestrator per-domain
# -----------------------
def analyze_domain_full(domain, priors_map, geo_resolver, total_requests=1000, alpha=SMOOTH_ALPHA, timeout=10, simulate_variants=None):
    """
    For a domain:
     - Fetch homepage (default) and optionally simulated headers variants.
     - Aggressive host extraction -> resolve hosts -> geolocate IPs -> observed country counts
     - Extract prebid signals from HTML
     - Fetch ads.txt and parse pubmatic IDs
     - For each pubmatic seller ID attempt sellers.json validation and infer country if present
     - Compose domain_signals dict and compute revenue posterior
    """
    simulate_variants = simulate_variants or []
    # load prior for this domain
    prior_for_domain = priors_map.get(domain, None)

    # 0) fetch base homepage
    code, html, final_url = fetch_url(f"https://{domain}", timeout=timeout)
    if code is None:
        code, html, final_url = fetch_url(f"http://{domain}", timeout=timeout)
    time.sleep(FETCH_DELAY)

    # Prebid extraction from base HTML
    prebid = extract_prebid_signals(html)

    # Aggressive host extraction
    hosts_list = extract_hosts_aggressive(html, base_domain=domain)

    hosts_detail = []
    observed = Counter()
    for h in hosts_list:
        ips = resolve_host(h)
        if not ips:
            hosts_detail.append({'host': h, 'ip': '', 'country': ''})
            continue
        for ip in ips:
            cc = geo_resolver.lookup(ip)
            hosts_detail.append({'host': h, 'ip': ip, 'country': cc})
            if cc:
                observed[cc] += 1

    # Ads.txt fetch & parse
    ads_status, ads_text, ads_final = fetch_ads_txt(domain, timeout=timeout)
    time.sleep(FETCH_DELAY)
    ads_entries = parse_ads_txt_entries(ads_text) if ads_text else []

    # extract pubmatic seller ids from ads_entries
    pubmatic_ids = []
    for adsys, seller, rel in ads_entries:
        if 'pubmatic' in adsys:
            role = 'DIRECT' if rel.startswith('DIRECT') else 'RESELLER'
            pubmatic_ids.append((seller, role))

    # Try to validate pubmatic seller ids via sellers.json on adsystem domains present in ads_entries
    sellers_validation = {}
    # collect unique adsystem domains to query sellers.json
    adsystems = set([adsys for adsys,_,_ in ads_entries])
    for adsys in adsystems:
        if not adsys:
            continue
        try:
            j, src = try_fetch_sellers_json_for_adsystem(adsys)
            if j and isinstance(j, dict):
                # common structure: {"sellers": [{ "seller_id": "...", "name": "...", "domain": "...", "seller_type": "...", "country": "..."}]}
                sellers = j.get('sellers') or j.get('nodes') or []
                for s in sellers:
                    sid = str(s.get('seller_id') or s.get('id') or '').lower()
                    cc = s.get('country') or s.get('country_code') or s.get('countryCode') or None
                    if sid:
                        sellers_validation[sid.lower()] = (cc.upper() if cc else None)
        except Exception:
            pass

    # Compose base signals dict
    domain_signals = {
        'domain': domain,
        'observed_countries': observed,
        'hosts_detail': hosts_detail,
        'prebid': prebid,
        'ads_txt_entries': ads_entries,
        'ads_txt_pubmatic_ids': pubmatic_ids,
        'sellers_validation': sellers_validation,
        'simulation_variants': []
    }

    # Handle simulation variants (simulate_variants is list of tuples (label, ip, accept_language))
    for sv in simulate_variants:
        label, sim_ip, accept_lang = sv.get('label'), sv.get('ip'), sv.get('al', '')
        headers = {'User-Agent': 'Mozilla/5.0 (compatible; PubMaticEstimator/1.0)'}
        if accept_lang:
            headers['Accept-Language'] = accept_lang
        # craft X-Forwarded-For to simulated IP
        if sim_ip:
            headers['X-Forwarded-For'] = sim_ip
        try:
            status, html_sim, final_sim = fetch_url(f"https://{domain}", timeout=timeout, headers=headers)
            if status is None:
                status, html_sim, final_sim = fetch_url(f"http://{domain}", timeout=timeout, headers=headers)
            time.sleep(FETCH_DELAY)
        except Exception:
            html_sim = None
        # extract same signals
        prebid_sim = extract_prebid_signals(html_sim)
        hosts_sim = extract_hosts_aggressive(html_sim, base_domain=domain)
        hosts_detail_sim = []
        observed_sim = Counter()
        for h in hosts_sim:
            ips = resolve_host(h)
            for ip in ips:
                cc = geo_resolver.lookup(ip)
                hosts_detail_sim.append({'host': h, 'ip': ip, 'country': cc})
                if cc:
                    observed_sim[cc] += 1
        # ads.txt entries under simulation not expected to change often but we still attempt to fetch ads.txt with these headers
        try:
            ads_status_sim, ads_text_sim, _ = fetch_ads_txt(domain, timeout=timeout)
        except Exception:
            ads_text_sim = None
        ads_entries_sim = parse_ads_txt_entries(ads_text_sim) if ads_text_sim else []
        pubmatic_ids_sim = []
        for adsys,seller,rel in ads_entries_sim:
            if 'pubmatic' in adsys:
                pubmatic_ids_sim.append((seller, 'DIRECT' if rel.startswith('DIRECT') else 'RESELLER'))
        domain_signals['simulation_variants'].append({
            'label': label or f"sim_{sim_ip or accept_lang}",
            'observed_countries': observed_sim,
            'hosts_detail': hosts_detail_sim,
            'prebid': prebid_sim,
            'ads_txt_pubmatic_ids': pubmatic_ids_sim
        })

    # Compute revenue posterior
    posterior, est_by_country, raw_score = compute_revenue_scores(domain_signals, total_requests, priors_for_domain=priors_map.get(domain), alpha=alpha)

    # Format outputs for Excel: hosts_detail rows, prebid signals
    hosts_rows = []
    for hd in hosts_detail:
        hosts_rows.append({'domain': domain, 'host': hd.get('host'), 'ip': hd.get('ip'), 'country': hd.get('country')})

    prebid_row = {
        'domain': domain,
        'adunit_count': prebid.get('adunit_count', 0),
        'floor_count': len(prebid.get('floors', [])),
        'avg_floor': (sum([f for f,c in prebid.get('floors', [])]) / max(1, len(prebid.get('floors', [])))) if prebid.get('floors') else None,
        'currencies': ",".join(sorted(list(prebid.get('currencies', set())))),
        'geo_clues': ",".join(sorted(list(prebid.get('geo_clues', set()))))
    }

    # ads.txt ids row(s)
    ads_ids_rows = []
    for sid, role in pubmatic_ids:
        ads_ids_rows.append({'domain': domain, 'seller_id': sid, 'role': role, 'validated_country': domain_signals.get('sellers_validation', {}).get(sid)})

    # sellers.json validation rows
    sellers_rows = []
    for sid, cc in sellers_validation.items():
        sellers_rows.append({'domain': domain, 'seller_id': sid, 'country': cc})

    # simulation rows
    sim_rows = []
    for sv in domain_signals.get('simulation_variants', []):
        # aggregate observed countries counts into string
        obs = sv.get('observed_countries', {})
        sim_rows.append({
            'domain': domain,
            'variant': sv.get('label'),
            'observed_countries': json.dumps({k:v for k,v in obs.items()}),
            'adunit_count': sv.get('prebid',{}).get('adunit_count',0),
            'floors': json.dumps(sv.get('prebid',{}).get('floors',[]))
        })

    # Build return structure
    result = {
        'domain': domain,
        'posterior': posterior,
        'est_by_country': est_by_country,
        'raw_score': raw_score,
        'hosts_rows': hosts_rows,
        'prebid_row': prebid_row,
        'ads_ids_rows': ads_ids_rows,
        'sellers_rows': sellers_rows,
        'simulation_rows': sim_rows,
        'observed_countries': dict(observed)
    }
    return result

# -----------------------
# CLI / Orchestration
# -----------------------
def load_priors_flexible(priors_file):
    priors = {}
    try:
        with open(priors_file, newline='', encoding='utf-8') as f:
            rdr = csv.DictReader(f)
            fields = rdr.fieldnames or []
            country_cols = [c for c in fields if c and c.lower()!='domain']
            for r in rdr:
                d = r.get('domain','').strip()
                if not d: continue
                p = {}
                for c in country_cols:
                    try:
                        p[c.upper()] = float(r.get(c,0) or 0.0)
                    except:
                        p[c.upper()] = 0.0
                s = sum(p.values())
                if s>0:
                    for k in p: p[k] = p[k]/s
                priors[d] = p
    except FileNotFoundError:
        return {}
    except Exception:
        return {}
    return priors

def parse_simulate_args(sim_list):
    """
    sim_list: list of strings "CC:IP:ACCEPT-LANG" or "IP:ACCEPT-LANG" or "IP"
    returns list of dicts: {'label':CC, 'ip': ip, 'al': accept-lang}
    """
    out = []
    for s in sim_list or []:
        parts = s.split(':')
        if len(parts) == 3:
            cc, ip, al = parts
            out.append({'label': cc.upper(), 'ip': ip, 'al': al})
        elif len(parts) == 2:
            ip, al = parts
            out.append({'label': None, 'ip': ip, 'al': al})
        elif len(parts) == 1 and parts[0]:
            out.append({'label': None, 'ip': parts[0], 'al': ''})
    return out

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--domains-file', required=True, help='plain text file with domains (one per line)')
    parser.add_argument('--out', default='pubmatic_country_revenue_estimates.xlsx')
    parser.add_argument('--total-requests', type=int, default=1000)
    parser.add_argument('--alpha', type=float, default=DEFAULT_ALPHA, help='prior strength for smoothing')
    parser.add_argument('--timeout', type=int, default=10)
    parser.add_argument('--priors-file', default='priors.csv', help='optional priors CSV domain,<country codes>')
    parser.add_argument('--simulate', nargs='*', help='simulation variants: "CC:IP:Accept-Language" or "IP:AL" or "IP"')
    parser.add_argument('--maxmind-db', default=None, help='optional path to GeoLite2-City.mmdb (requires geoip2)')
    args = parser.parse_args()

    # load domains
    with open(args.domains_file, 'r', encoding='utf-8') as f:
        domains = [line.strip() for line in f if line.strip()]

    priors_map = load_priors_flexible(args.priors_file) if args.priors_file else {}

    simulate_variants = parse_simulate_args(args.simulate)

    geo_resolver = GeoResolver(maxmind_db_path=args.maxmind_db)

    results = []
    hosts_rows_all = []
    prebid_rows = []
    adsids_rows = []
    sellers_rows = []
    sim_rows_all = []
    bycountry_rows = []

    for dom in domains:
        try:
            print(f"[INFO] Processing {dom}...", file=sys.stderr)
            res = analyze_domain_full(dom, priors_map, geo_resolver, total_requests=args.total_requests, alpha=args.alpha, timeout=args.timeout, simulate_variants=simulate_variants)
            results.append({
                'domain': dom,
                'posterior_json': json.dumps(res.get('posterior',{})),
                'est_by_country_json': json.dumps(res.get('est_by_country',{})),
                'raw_score_json': json.dumps(res.get('raw_score',{})),
                'observed_countries': json.dumps(res.get('observed_countries',{}))
            })
            # collect sheets
            hosts_rows_all.extend(res.get('hosts_rows', []))
            prebid_rows.append(res.get('prebid_row', {}))
            adsids_rows.extend(res.get('ads_ids_rows', []))
            sellers_rows.extend(res.get('sellers_rows', []))
            sim_rows_all.extend(res.get('simulation_rows', []))
            # by-country flatten
            for cc, pct in res.get('posterior', {}).items():
                bycountry_rows.append({'domain': dom, 'country': cc, 'posterior_pct': round(pct*100,4), 'est_requests': res.get('est_by_country', {}).get(cc,0)})

            time.sleep(FETCH_DELAY)
        except Exception as e:
            print(f"[ERR] {dom} -> {e}\n{traceback.format_exc()}", file=sys.stderr)
            continue

    # write excel
    df_summary = pd.DataFrame(results)
    df_hosts = pd.DataFrame(hosts_rows_all)
    df_prebid = pd.DataFrame(prebid_rows)
    df_adsids = pd.DataFrame(adsids_rows)
    df_sellers = pd.DataFrame(sellers_rows)
    df_sim = pd.DataFrame(sim_rows_all)
    df_bycountry = pd.DataFrame(bycountry_rows)

    with pd.ExcelWriter(args.out, engine='openpyxl') as writer:
        df_summary.to_excel(writer, sheet_name='Estimates', index=False)
        if not df_bycountry.empty:
            df_bycountry.to_excel(writer, sheet_name='ByCountry', index=False)
        if not df_hosts.empty:
            df_hosts.to_excel(writer, sheet_name='Detected_Hosts', index=False)
        if not df_prebid.empty:
            df_prebid.to_excel(writer, sheet_name='PrebidSignals', index=False)
        if not df_adsids.empty:
            df_adsids.to_excel(writer, sheet_name='AdsTxt_IDs', index=False)
        if not df_sellers.empty:
            df_sellers.to_excel(writer, sheet_name='SellersValidation', index=False)
        if not df_sim.empty:
            df_sim.to_excel(writer, sheet_name='SimulationVariants', index=False)

    print(f"[DONE] Wrote {args.out}")

if __name__ == '__main__':
    main()
