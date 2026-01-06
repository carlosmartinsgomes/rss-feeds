#!/usr/bin/env python3
"""
estimate_pubmatic_country_percentages_revenue_with_HAR.py

Aggressive scanning + Prebid parsing + ads.txt & sellers.json validation + revenue-weighting
+ optional origin simulation + optional MaxMind GeoIP + HAR-module for authoritative signals.

New features (added):
 - HAR module: --har-dir to provide directory with HAR files. Uses ijson if available for streaming
   otherwise falls back to a safe json streaming approach (careful with large files).
 - WEIGHT_HAR_SIGNAL constant integrated into compute_revenue_scores. HAR signals dominate when present.
 - HAR_Analysis sheet in output Excel with per-HAR-event summary (requests, fills, inferred country clues).
 - CLI argument --har-dir and integration into domain pipeline: if HAR data exists for domain, HAR signals
   are used as highest-confidence input to scoring and also to populate HAR_Analysis.

Notes:
 - This script is intended to run in a self-hosted runner. Be mindful of network/IO limits.
 - Requires: requests, pandas, openpyxl. Optional: geoip2, ijson for HAR streaming.

Usage (excerpt):
  python3 estimate_pubmatic_country_percentages_revenue_with_HAR.py \
    --domains-file principaldomains \
    --out pubmatic_country_revenue_estimates_with_har.xlsx \
    --har-dir ./hars \
    --total-requests 1000

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
import os
from collections import Counter, defaultdict
from urllib.parse import urlparse, urljoin
import pandas as pd

# Optional geoip2
try:
    import geoip2.database
    GEOIP2_AVAILABLE = True
except Exception:
    GEOIP2_AVAILABLE = False

# Optional ijson for streaming HARs
try:
    import ijson
    IJSON_AVAILABLE = True
except Exception:
    IJSON_AVAILABLE = False

# -----------------------
# Config / heuristics
# -----------------------
PUB_KEYWORDS = ['pubmatic', 'openwrap', 'openwrapsdk', 'hb.pubmatic', 'ads.pubmatic']
GENERAL_KEYWORDS = ['prebid', 'pbjs', 'bidder', 'bid', 'adUnit', 'floor', 'floorPrice', 'currency', 'countries', 'appliesTo']
DOMAIN_RE = re.compile(r'([a-z0-9\-_\.]+\.[a-z]{2,6})', re.IGNORECASE)
# pbjs objects heuristics
PBJS_OBJ_RE = re.compile(r'(pbjs\.adUnits\s*=\s*|pbjs\.que\.push\(|var\s+pbjs\s*=)', re.IGNORECASE)
# JSON_LIKE_RE baseado em blocos simples; a extração real de JSON aninhado
# é feita pela função extract_json_blocks (ver abaixo).
JSON_LIKE_RE = re.compile(r'\{[^{}]*\}', re.DOTALL)

ADUNIT_KEYWORDS = ['adUnits', 'adUnitCode', 'mediaTypes', 'bids', 'params',
                   'floor', 'floorPrice', 'currency', 'countries', 'appliesTo',
                   'ortb2', 'ortb2Imp', 'device', 'site']


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
WEIGHT_HAR_SIGNAL = 10.0             # HAR signals are strongest evidence when present

# polite delay for HTTP fetch to remote sites (homepage)
FETCH_DELAY = 0.12
HAR_IO_CHUNK = 1024 * 64

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
    if not html:
        return []
    hosts = []
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
                pass
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
def extract_json_blocks(text, max_blocks=50, max_len=20000):
    """
    Extrai blocos JSON aninhados de uma string usando contagem de chavetas.
    É muito mais robusto do que tentar usar regex recursiva (que o Python não suporta).
    """
    blocks = []
    if not text:
        return blocks
    n = len(text)
    i = 0
    while i < n:
        if text[i] == '{':
            depth = 0
            start = i
            j = i
            while j < n:
                ch = text[j]
                if ch == '{':
                    depth += 1
                elif ch == '}':
                    depth -= 1
                    if depth == 0:
                        end = j + 1
                        candidate = text[start:end]
                        if len(candidate) <= max_len:
                            blocks.append(candidate)
                        i = end
                        break
                j += 1
            else:
                i += 1
        else:
            i += 1
        if len(blocks) >= max_blocks:
            break
    return blocks

def try_parse_json_like(s, max_candidates=5):
    """
    Tenta extrair e parsear um ou mais blocos JSON de uma string potencialmente suja/minificada.
    Usa extract_json_blocks para suportar JSON profundamente aninhado.
    """
    if not s or not isinstance(s, str):
        return None

    candidates = extract_json_blocks(s, max_blocks=max_candidates)
    if not candidates:
        return None

    def normalize_json_like(txt):
        txt = txt.replace('\r', ' ').replace('\n', ' ')
        # substitui undefined por null
        txt = re.sub(r'\bundefined\b', 'null', txt)
        # tenta colocar aspas em chaves simples estilo JS (muito heurístico)
        txt = re.sub(r'(\{|,)\s*([A-Za-z_][A-Za-z0-9_]*)\s*:', r'\1 "\2":', txt)
        # normaliza aspas simples -> duplas
        txt = txt.replace("'", '"')
        # remove vírgulas a mais antes de ] ou }
        txt = re.sub(r',\s*([\]\}])', r'\1', txt)
        return txt

    for cand in candidates:
        norm = normalize_json_like(cand)
        try:
            obj = json.loads(norm)
            return obj
        except Exception:
            continue
    return None


def extract_prebid_signals(html):
    """
    Extrai sinais relevantes de Prebid/OpenWrap:
      - número de adUnits
      - floors médios
      - moedas
      - pistas de geo (countries, device.geo, ortb2.site, ortb2Imp, etc.)
    Usa parsing heurístico com suporte para JSON aninhado.
    """
    out = {
        "adunit_count": 0,
        "floors": [],
        "currencies": set(),
        "geo_clues": set(),
        "raw_matches": []
    }
    if not html:
        return out

    text = html

    # 1) Procurar padrões óbvios de Prebid / pbjs / openwrap
    prebid_markers = [
        r'pbjs\.adUnits', r'pbjs\.que', r'pbjs\.addAdUnits',
        r'bidderSettings', r'bidderConfig', r'openwrap', r'ow\.pbjs'
    ]

    for pat in prebid_markers:
        for m in re.finditer(pat, text, flags=re.I):
            start = max(0, m.start()-800)
            end = min(len(text), m.end()+4000)
            seg = text[start:end]
            out["raw_matches"].append(seg[:2000])

            parsed = try_parse_json_like(seg, max_candidates=8)
            if parsed:
                def recurse(obj):
                    if isinstance(obj, dict):
                        lk_keys = {str(k).lower(): k for k in obj.keys()}

                        # adUnits / adUnitCode
                        if 'adunits' in lk_keys:
                            v = obj[lk_keys['adunits']]
                            if isinstance(v, list):
                                out["adunit_count"] += len(v)

                        # floors, floorPrice, bidfloor, cpm
                        for lk, orig_k in lk_keys.items():
                            v = obj[orig_k]
                            if any(key in lk for key in ['floor', 'floorprice', 'bidfloor', 'cpm']):
                                try:
                                    if isinstance(v, (int, float, str)) and str(v).strip() not in ('', 'none', 'null'):
                                        val = float(v)
                                        curr = obj.get('currency') or obj.get('curr') or obj.get('currencyCode')
                                        if curr:
                                            out["currencies"].add(str(curr).upper())
                                        out["floors"].append((val, (curr or '').upper()))
                                except Exception:
                                    pass

                        # currencies explícitas
                        for kopt in ('currency', 'curr', 'currencyCode'):
                            if kopt in lk_keys:
                                cv = obj[lk_keys[kopt]]
                                try:
                                    if isinstance(cv, str) and len(cv) <= 4:
                                        out["currencies"].add(cv.upper())
                                except Exception:
                                    pass

                        # geo / countries / appliesTo
                        for gk in ('geo', 'countries', 'appliesto', 'appliesTo'):
                            if gk.lower() in lk_keys:
                                gv = obj[lk_keys[gk.lower()]]
                                if isinstance(gv, list):
                                    for it in gv:
                                        try:
                                            code = str(it).upper()
                                            if len(code) == 2:
                                                out["geo_clues"].add(code)
                                        except:
                                            pass
                                elif isinstance(gv, dict):
                                    for it in gv.get('countries', []):
                                        try:
                                            code = str(it).upper()
                                            if len(code) == 2:
                                                out["geo_clues"].add(code)
                                        except:
                                            pass

                        # ortb2 / ortb2Imp / device.geo / site
                        # ortb2.site.country, ortb2.site.content.language, device.geo.country
                        if 'ortb2' in lk_keys:
                            o2 = obj[lk_keys['ortb2']]
                            if isinstance(o2, dict):
                                site = o2.get('site', {})
                                if isinstance(site, dict):
                                    ctry = site.get('country') or site.get('ref') or None
                                    if isinstance(ctry, str) and len(ctry) == 2:
                                        out["geo_clues"].add(ctry.upper())
                                    lang = site.get('content', {}).get('language') if isinstance(site.get('content'), dict) else None
                                    if isinstance(lang, str) and len(lang) == 2:
                                        out["geo_clues"].add(lang.upper())
                                device = o2.get('device', {})
                                if isinstance(device, dict):
                                    geo = device.get('geo', {})
                                    if isinstance(geo, dict):
                                        ctry = geo.get('country')
                                        if isinstance(ctry, str) and len(ctry) == 2:
                                            out["geo_clues"].add(ctry.upper())

                        if 'ortb2imp' in lk_keys:
                            o2i = obj[lk_keys['ortb2imp']]
                            if isinstance(o2i, list):
                                for it in o2i:
                                    if isinstance(it, dict):
                                        geo = it.get('geo') or {}
                                        if isinstance(geo, dict):
                                            ctry = geo.get('country')
                                            if isinstance(ctry, str) and len(ctry) == 2:
                                                out["geo_clues"].add(ctry.upper())

                        # device.geo fora de ortb2
                        if 'device' in lk_keys:
                            dev = obj[lk_keys['device']]
                            if isinstance(dev, dict):
                                geo = dev.get('geo', {})
                                if isinstance(geo, dict):
                                    ctry = geo.get('country')
                                    if isinstance(ctry, str) and len(ctry) == 2:
                                        out["geo_clues"].add(ctry.upper())

                        # recursion
                        for v in obj.values():
                            recurse(v)

                    elif isinstance(obj, list):
                        for it in obj:
                            recurse(it)

                try:
                    recurse(parsed)
                except Exception:
                    pass
            else:
                # fallback extremamente heurístico, apenas se nada parseável foi encontrado
                for fm in re.finditer(r'"\s*floor(?:Price|_price|)\s*"\s*[:=]\s*"?([0-9]+(?:\.[0-9]+)?)', seg, flags=re.I):
                    try:
                        val = float(fm.group(1))
                        out["floors"].append((val, ''))
                    except:
                        pass
                for cm in re.finditer(r'"\s*currency\s*"\s*:\s*"(.*?)"', seg, flags=re.I):
                    out["currencies"].add(cm.group(1).upper())
                for ccm in re.finditer(r'countries\s*[:=]\s*\[([^\]]+)\]

', seg, flags=re.I):
                    arr = ccm.group(1)
                    for code in re.findall(r'["\']?([A-Za-z]{2})["\']?', arr):
                        out["geo_clues"].add(code.upper())

    # limpeza final
    out["currencies"] = set([c for c in out["currencies"] if c])
    out["geo_clues"] = set([g for g in out["geo_clues"] if isinstance(g, str) and len(g) == 2])
    return out

# -----------------------
# ads.txt & sellers.json helpers
# -----------------------
def fetch_ads_txt(domain, timeout=10):
    urls = [f'https://{domain}/ads.txt', f'http://{domain}/ads.txt']
    for u in urls:
        try:
            code, text, final = fetch_url(u, timeout=timeout)
            if code == 200 and text:
                return 200, text, final
            if code in (301,302) and text:
                return code, text, final
        except Exception:
            pass
    return None, None, None

def parse_ads_txt_entries(ads_txt):
    """
    Parse simples de ads.txt em (adsystem, seller_id, relationship).
    Ignora linhas comentadas (#) e marca truncamento heurístico.
    """
    if not ads_txt:
        return [], False

    entries = []
    lines = ads_txt.splitlines()
    for ln in lines:
        raw = ln
        ln = ln.strip()
        if not ln:
            continue
        if ln.startswith('#'):
            continue
        if '#' in ln:
            ln = ln.split('#',1)[0].strip()
        parts = [p.strip() for p in ln.split(',')]
        if len(parts) >= 3:
            entries.append((parts[0].lower(), parts[1].lower(), parts[2].upper()))

    # heurística de truncamento: última linha não termina em newline
    truncated = False
    if not ads_txt.endswith('\n') and len(lines) > 0:
        last = lines[-1]
        # se a última linha não tiver vírgulas suficientes, é suspeita
        if last.count(',') < 2 and not last.strip().startswith('#'):
            truncated = True

    return entries, truncated


def try_fetch_sellers_json_for_adsystem(adsystem_domain, timeout=8):
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
                    m = re.search(r'(\{.*\})', text, flags=re.S)
                    if m:
                        try:
                            j = json.loads(m.group(1))
                            return j, u
                        except:
                            pass
        except Exception:
            pass
    return None, None

# -----------------------
# HAR module
# -----------------------
def find_har_file_for_domain(har_dir, domain):
    if not har_dir:
        return None
    if not os.path.isdir(har_dir):
        return None
    candidates = []
    # look for files containing domain string in filename
    for fname in os.listdir(har_dir):
        if domain.replace('.', '_') in fname or domain in fname:
            path = os.path.join(har_dir, fname)
            if os.path.isfile(path):
                candidates.append(path)
    # prefer exact domain.har or domain.json
    for c in candidates:
        b = os.path.basename(c).lower()
        if b.startswith(domain.lower()) and (b.endswith('.har') or b.endswith('.json')):
            return c
    return candidates[0] if candidates else None


def analyze_har_for_domain(har_path):
    """
    Stream a HAR file and extract PubMatic-related requests.
    Returns dict: {
       'total_requests': int,
       'pubmatic_requests': int,
       'fills_by_country': Counter(country->fills),
       'requests_by_country': Counter(country->requests),
       'har_rows': [ {ts, req_url, country_clue, is_fill, status} ... ]
    }
    """
    res = {
        'total_requests': 0,
        'pubmatic_requests': 0,
        'fills_by_country': Counter(),
        'requests_by_country': Counter(),
        'har_rows': []
    }
    if not os.path.isfile(har_path):
        return res
    # streaming if ijson available
    try:
        if IJSON_AVAILABLE:
            with open(har_path, 'rb') as fh:
                # iterate entries
                parser = ijson.items(fh, 'log.entries.item')
                for entry in parser:
                    res['total_requests'] += 1
                    try:
                        req = entry.get('request', {})
                        resp = entry.get('response', {})
                        url = (req.get('url') or '').lower()
                        if 'pubmatic' in url or any(k in (url or '') for k in PUB_KEYWORDS):
                            res['pubmatic_requests'] += 1
                            # look for postData
                            post = req.get('postData', {})
                            text = post.get('text') if isinstance(post, dict) else None
                            # try to extract country clues from post or url
                            country = None
                            
                            if text:
                                # tentar vários campos comuns: country, countryCode, geo.country, device.geo.country
                                m = re.search(r'"country"\s*[:=]\s*"?([A-Za-z]{2})"?', text)
                                if m:
                                    country = m.group(1).upper()
                                else:
                                    m2 = re.search(r'"countryCode"\s*[:=]\s*"?([A-Za-z]{2})"?', text)
                                    if m2:
                                        country = m2.group(1).upper()
                                    else:
                                        m3 = re.search(r'"geo"\s*:\s*\{[^}]*"country"\s*:\s*"?([A-Za-z]{2})"?', text)
                                        if m3:
                                            country = m3.group(1).upper()

                            # check response for fill-like content
                            status = resp.get('status')
                            content = ''
                            cont = resp.get('content', {})
                            if isinstance(cont, dict):
                                content = cont.get('text') or ''
                            is_fill = False
                            if content and isinstance(content, str):
                                lo = content.lower()
                                if 'adm' in lo or 'creative' in lo or 'cpm' in lo or 'price' in lo:
                                    is_fill = True
                            # fallback: status 204 or 204-like may mean no fill
                            if is_fill:
                                if country:
                                    res['fills_by_country'][country] += 1
                                else:
                                    res['fills_by_country']['UNKNOWN'] += 1
                            if country:
                                res['requests_by_country'][country] += 1
                            else:
                                res['requests_by_country']['UNKNOWN'] += 1
                            res['har_rows'].append({'url': url, 'country': country or '', 'is_fill': is_fill, 'status': status})
                    except Exception:
                        continue
        else:
            # fallback: load incrementally but careful with memory
            with open(har_path, 'r', encoding='utf-8', errors='replace') as fh:
                data = json.load(fh)
                entries = data.get('log', {}).get('entries', [])
                for entry in entries:
                    res['total_requests'] += 1
                    req = entry.get('request', {})
                    resp = entry.get('response', {})
                    url = (req.get('url') or '').lower()
                    if 'pubmatic' in url or any(k in (url or '') for k in PUB_KEYWORDS):
                        res['pubmatic_requests'] += 1
                        post = req.get('postData', {})
                        text = post.get('text') if isinstance(post, dict) else None
                        country = None
                        if text:
                            # tentar vários campos comuns: country, countryCode, geo.country
                            m = re.search(r'"country"\s*[:=]\s*"?([A-Za-z]{2})"?', text)
                            if m:
                                country = m.group(1).upper()
                            else:
                                m2 = re.search(r'"countryCode"\s*[:=]\s*"?([A-Za-z]{2})"?', text)
                                if m2:
                                    country = m2.group(1).upper()
                                else:
                                    m3 = re.search(r'"geo"\s*:\s*\{[^}]*"country"\s*:\s*"?([A-Za-z]{2})"?', text)
                                    if m3:
                                        country = m3.group(1).upper()

                        status = resp.get('status')
                        content = ''
                        cont = resp.get('content', {})
                        if isinstance(cont, dict):
                            content = cont.get('text') or ''
                        is_fill = False
                        if content and isinstance(content, str):
                            lo = content.lower()
                            if 'adm' in lo or 'creative' in lo or 'cpm' in lo or 'price' in lo:
                                is_fill = True
                        if is_fill:
                            if country:
                                res['fills_by_country'][country] += 1
                            else:
                                res['fills_by_country']['UNKNOWN'] += 1
                        if country:
                            res['requests_by_country'][country] += 1
                        else:
                            res['requests_by_country']['UNKNOWN'] += 1
                        res['har_rows'].append({'url': url, 'country': country or '', 'is_fill': is_fill, 'status': status})
    except Exception as e:
        print(f"[WARN] HAR parse error for {har_path}: {e}", file=sys.stderr)
    return res

# -----------------------
# Revenue-weighting logic (extended with HAR)
# -----------------------
def compute_revenue_scores(domain_signals, total_requests, priors_for_domain=None, alpha=SMOOTH_ALPHA):
    """
    Extended revenue scoring that also returns a reliability meta object.
    Returns: (posterior, est_by_country, score_dict, meta)
    meta = {
      'confidence_score': int(0..100),
      'reliability_label': 'High (87)',
      'breakdown': { 'har':x, 'prebid':y, 'ads_txt':z, 'infra':w, 'sim':v }
    }
    """
    score = defaultdict(float)
    observed = domain_signals.get('observed_countries', Counter())
    prebid = domain_signals.get('prebid', {})
    pubmatic_ids = domain_signals.get('ads_txt_pubmatic_ids', [])
    sellers_valid = domain_signals.get('sellers_validation', {})
    sim_variants = domain_signals.get('simulation_variants', [])
    har_signals = domain_signals.get('har', None)

    # --- Keep same raw score construction as before (component aggregation) ---
    # Component HAR (Highest confidence)
    har_contrib = 0.0
    if har_signals and (har_signals.get('pubmatic_requests', 0) > 0):
        fills = har_signals.get('fills_by_country', {})
        reqs = har_signals.get('requests_by_country', {})
        total_fills = sum(fills.values())
        total_reqs = sum(reqs.values())
        if total_fills > 0:
            for cc, cnt in fills.items():
                score[cc] += cnt * WEIGHT_HAR_SIGNAL
        else:
            for cc, cnt in reqs.items():
                score[cc] += cnt * (WEIGHT_HAR_SIGNAL * 0.5)
        # a numeric measure of HAR strength
        har_contrib = min(1.0, (total_fills + 0.2 * total_reqs) / max(1.0, total_reqs))

    # Component A: IP observed counts (low weight)
    infra_contrib = 0.0
    observed_total = sum(observed.values())
    if observed_total > 0:
        for cc, cnt in observed.items():
            score[cc] += cnt * 0.6
        # infra signal strength scaled to [0..1]
        infra_contrib = min(1.0, observed_total / 4.0)

    # Component B: Prebid signals
    prebid_contrib = 0.0
    try:
        adunit_count = int(prebid.get('adunit_count', 0))
    except:
        adunit_count = 0
    floors = prebid.get('floors', [])
    geo_clues = set(prebid.get('geo_clues', []))
    flo_val = 0.0
    flo_count = 0
    for f, c in floors:
        try:
            if f and float(f) > 0:
                flo_val += float(f)
                flo_count += 1
        except:
            pass
    avg_floor = (flo_val / flo_count) if flo_count else 0.0
    if geo_clues:
        for cc in geo_clues:
            score[cc] += WEIGHT_PREBID_ADUNIT * max(1, adunit_count) + WEIGHT_PREBID_FLOOR * avg_floor * (1 + flo_count / 2)
        prebid_contrib = min(1.0, (adunit_count / 5.0) + min(1.0, avg_floor / 5.0))
    else:
        currencies = prebid.get('currencies', set())
        for cur in currencies:
            if cur == 'JPY':
                score['JP'] += WEIGHT_PREBID_FLOOR * avg_floor * 1.5
            elif cur == 'GBP':
                score['GB'] += WEIGHT_PREBID_FLOOR * avg_floor * 1.2
            elif cur == 'AUD':
                score['AU'] += WEIGHT_PREBID_FLOOR * avg_floor * 1.2
        if observed_total > 0:
            for cc, cnt in observed.items():
                score[cc] += (cnt / observed_total) * WEIGHT_PREBID_ADUNIT * adunit_count
        prebid_contrib = min(1.0, (adunit_count / 5.0) + min(1.0, avg_floor / 5.0))

    # Component C: ads.txt pubmatic ids and sellers.json validation
    ads_contrib = 0.0
    total_pubmatic_ids = len(pubmatic_ids)
    validated_count = 0
    direct_count = 0
    for seller_id, role in pubmatic_ids:
        validated_cc = sellers_valid.get(seller_id)
        weight = WEIGHT_ADSTXT_DIRECT if role == 'DIRECT' else WEIGHT_ADSTXT_RESELLER
        if role == 'DIRECT':
            direct_count += 1
        if validated_cc:
            validated_count += 1
            score[validated_cc] += weight * (2.0 if role == 'DIRECT' else 1.0) + WEIGHT_SELLERS_VALIDATION
        else:
            if observed_total > 0:
                for cc, cnt in observed.items():
                    score[cc] += (cnt / observed_total) * weight
    if total_pubmatic_ids > 0:
        ads_contrib = min(1.0, (validated_count / total_pubmatic_ids) + (direct_count / (total_pubmatic_ids + 0.0001)) * 0.5)

    # Component D: simulation variants
    sim_contrib = 0.0
    sim_positive = 0
    for var in sim_variants:
        v_obs = var.get('observed_countries', Counter())
        v_pre = var.get('prebid', {})
        v_pub = var.get('ads_txt_pubmatic_ids', [])
        for cc, cnt in v_obs.items():
            score[cc] += cnt * 0.8 * WEIGHT_SIMULATION_VARIANT
        g = set(v_pre.get('geo_clues', []))
        if g:
            for cc in g:
                score[cc] += WEIGHT_PREBID_ADUNIT * 0.7 * WEIGHT_SIMULATION_VARIANT
            sim_positive += 1
        for sid, role in v_pub:
            vc = sellers_valid.get(sid)
            if vc:
                score[vc] += WEIGHT_ADSTXT_DIRECT * 0.5 * WEIGHT_SIMULATION_VARIANT
    if simulate_variants:
        sim_contrib = min(1.0, sim_positive / max(1.0, len(simulate_variants)))

    # Fallbacks
    if not score:
        if priors_for_domain:
            for cc, frac in priors_for_domain.items():
                score[cc] = frac * 10.0
        elif observed_total > 0:
            for cc, cnt in observed.items():
                score[cc] = float(cnt)
        else:
            score['UNKNOWN'] = 1.0

    score_dict = dict(score)
    total_score = sum(score_dict.values()) if score_dict else 0.0
    if total_score <= 0:
        keys = list(score_dict.keys()) or ['UNKNOWN']
        for k in keys:
            score_dict[k] = 1.0
        total_score = sum(score_dict.values())
    posterior = {k: (v / total_score) for k, v in score_dict.items()}

    # smoothing
    if priors_for_domain:
        for k in list(posterior.keys()):
            if k not in priors_for_domain:
                priors_for_domain[k] = 0.0
        smooth_counts = {}
        for k in posterior.keys():
            smooth_counts[k] = alpha * priors_for_domain.get(k, 0.0) + posterior.get(k, 0.0)
        s = sum(smooth_counts.values()) or 1.0
        posterior = {k: smooth_counts[k] / s for k in smooth_counts}

    est_by_country = {k: int(round(v * total_requests)) for k, v in posterior.items()}
    s_est = sum(est_by_country.values())
    if s_est != total_requests:
        diff = total_requests - s_est
        if posterior:
            largest = max(posterior.items(), key=lambda t: t[1])[0]
            est_by_country[largest] += diff

    # --- Build a reliability/confidence score (0..100) ---
    # weights for the confidence composition (tunable)
    W_HAR = 0.40
    W_PREBID = 0.25
    W_ADS = 0.20
    W_INFRA = 0.10
    W_SIM = 0.05

    comp_har = har_contrib
    comp_prebid = prebid_contrib
    comp_ads = ads_contrib
    comp_infra = infra_contrib
    comp_sim = sim_contrib

    raw_confidence = (W_HAR * comp_har + W_PREBID * comp_prebid + W_ADS * comp_ads + W_INFRA * comp_infra + W_SIM * comp_sim)
    # penalização leve se ads.txt estiver truncado/suspeito
    ads_trunc = domain_signals.get('ads_truncated', False)
    if ads_trunc:
        raw_confidence *= 0.85

    confidence_score = int(round(max(0.0, min(1.0, raw_confidence)) * 100))

    # label mapping
    if confidence_score >= 80:
        label = f'High ({confidence_score})'
        trusted = True
    elif confidence_score >= 50:
        label = f'Medium ({confidence_score})'
        trusted = False
    else:
        label = f'Low ({confidence_score})'
        trusted = False

    meta = {
        'confidence_score': confidence_score,
        'reliability_label': label,
        'trusted': trusted,
        'breakdown': {
            'har': round(comp_har, 3),
            'prebid': round(comp_prebid, 3),
            'ads_txt': round(comp_ads, 3),
            'infra': round(comp_infra, 3),
            'sim': round(comp_sim, 3)
        }
    }

    return posterior, est_by_country, score_dict, meta

# -----------------------
# Orchestrator per-domain (integrates HAR)
# -----------------------
def analyze_domain_full(domain, priors_map, geo_resolver, total_requests=1000, alpha=SMOOTH_ALPHA, timeout=10, simulate_variants=None, har_dir=None):
    simulate_variants = simulate_variants or []
    prior_for_domain = priors_map.get(domain, None)
    # 0) try HAR first (authoritative)
    har_data = None
    har_path = find_har_file_for_domain(har_dir, domain) if har_dir else None
    if har_path:
        try:
            har_data = analyze_har_for_domain(har_path)
        except Exception as e:
            print(f"[WARN] HAR processing failed for {domain}: {e}", file=sys.stderr)
            har_data = None
    # If HAR provides country fills -> we will inject into domain_signals['har'] and rely heavily on it
    # 1) fetch base homepage
    code, html, final_url = fetch_url(f"https://{domain}", timeout=timeout)
    if code is None:
        code, html, final_url = fetch_url(f"http://{domain}", timeout=timeout)
    time.sleep(FETCH_DELAY)
    prebid = extract_prebid_signals(html)
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
    ads_status, ads_text, ads_final = fetch_ads_txt(domain, timeout=timeout)
    time.sleep(FETCH_DELAY)
    ads_entries, ads_truncated = parse_ads_txt_entries(ads_text) if ads_text else ([], False)

    pubmatic_ids = []
    for adsys, seller, rel in ads_entries:
        if 'pubmatic' in adsys:
            role = 'DIRECT' if rel.startswith('DIRECT') else 'RESELLER'
            pubmatic_ids.append((seller, role))

    sellers_validation = {}
    adsystems = set([adsys for adsys,_,_ in ads_entries])
    for adsys in adsystems:
        if not adsys:
            continue
        try:
            j, src = try_fetch_sellers_json_for_adsystem(adsys)
            if j and isinstance(j, dict):
                sellers = j.get('sellers') or j.get('nodes') or []
                for s in sellers:
                    sid = str(s.get('seller_id') or s.get('id') or '').lower()
                    cc = s.get('country') or s.get('country_code') or s.get('countryCode') or None
                    if sid:
                        sellers_validation[sid.lower()] = (cc.upper() if cc else None)
        except Exception:
            pass
    domain_signals = {
        'domain': domain,
        'observed_countries': observed,
        'hosts_detail': hosts_detail,
        'prebid': prebid,
        'ads_txt_entries': ads_entries,
        'ads_txt_pubmatic_ids': pubmatic_ids,
        'sellers_validation': sellers_validation,
        'simulation_variants': [],
        'har': har_data,
        'ads_truncated': ads_truncated
    }
    # simulation variants
    for sv in simulate_variants:
        label, sim_ip, accept_lang = sv.get('label'), sv.get('ip'), sv.get('al', '')
        headers = {'User-Agent': 'Mozilla/5.0 (compatible; PubMaticEstimator/1.0)'}
        if accept_lang:
            headers['Accept-Language'] = accept_lang
        if sim_ip:
            headers['X-Forwarded-For'] = sim_ip
        try:
            status, html_sim, final_sim = fetch_url(f"https://{domain}", timeout=timeout, headers=headers)
            if status is None:
                status, html_sim, final_sim = fetch_url(f"http://{domain}", timeout=timeout, headers=headers)
            time.sleep(FETCH_DELAY)
        except Exception:
            html_sim = None
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
    posterior, est_by_country, raw_score, reliability_meta = compute_revenue_scores(domain_signals, total_requests, priors_for_domain=priors_map.get(domain), alpha=alpha)
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
    ads_ids_rows = []
    for sid, role in pubmatic_ids:
        ads_ids_rows.append({'domain': domain, 'seller_id': sid, 'role': role, 'validated_country': domain_signals.get('sellers_validation', {}).get(sid)})
    sellers_rows = []
    for sid, cc in sellers_validation.items():
        sellers_rows.append({'domain': domain, 'seller_id': sid, 'country': cc})
    sim_rows = []
    for sv in domain_signals.get('simulation_variants', []):
        obs = sv.get('observed_countries', {})
        sim_rows.append({
            'domain': domain,
            'variant': sv.get('label'),
            'observed_countries': json.dumps({k:v for k,v in obs.items()}),
            'adunit_count': sv.get('prebid',{}).get('adunit_count',0),
            'floors': json.dumps(sv.get('prebid',{}).get('floors',[]))
        })
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
        'observed_countries': dict(observed),
        'har': har_data,
        'reliability': reliability_meta
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
    except Exception as e:
        print(f"[WARN] priors load error: {e}", file=sys.stderr)
    return priors


def parse_simulate_args(sim_list):
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
    parser.add_argument('--out', default='pubmatic_country_revenue_estimates_with_har.xlsx')
    parser.add_argument('--total-requests', type=int, default=1000)
    parser.add_argument('--alpha', type=float, default=DEFAULT_ALPHA, help='prior strength for smoothing')
    parser.add_argument('--timeout', type=int, default=10)
    parser.add_argument('--priors-file', default='priors.csv', help='optional priors CSV domain,<country codes>')
    parser.add_argument('--simulate', nargs='*', help='simulation variants: "CC:IP:Accept-Language" or "IP:AL" or "IP"')
    parser.add_argument('--maxmind-db', default=None, help='optional path to GeoLite2-City.mmdb (requires geoip2)')
    parser.add_argument('--har-dir', default=None, help='optional directory containing HAR files (per-domain)')
    args = parser.parse_args()

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
    har_analysis_rows = []

    for dom in domains:
        try:
            print(f"[INFO] Processing {dom}...", file=sys.stderr)
            res = analyze_domain_full(dom, priors_map, geo_resolver, total_requests=args.total_requests, alpha=args.alpha, timeout=args.timeout, simulate_variants=simulate_variants, har_dir=args.har_dir)
            results.append({
                'domain': dom,
                'pubmatic_signals_found': bool(hosts_detail),
                'num_hosts_detected': len({h['host'] for h in hosts_detail}),
                'observed_signal_sum': sum(observed.values()),
                'confidence': round(meta['confidence_score'],3),
                'reliability_label': meta['reliability_label'],
                'ads_truncated': domain_signals.get('ads_truncated', False),
                'geo_clues': list(domain_signals.get('prebid', {}).get('geo_clues', [])),
                'confidence_har': meta['breakdown']['har'],
                'confidence_prebid': meta['breakdown']['prebid'],
                'confidence_ads': meta['breakdown']['ads_txt'],
                'confidence_infra': meta['breakdown']['infra'],
                'confidence_sim': meta['breakdown']['sim'],
                'posterior_json': json.dumps(posterior),
                'est_requests_json': json.dumps(est_by_country),
                'hosts_detail_json': json.dumps(hosts_detail)
            })

            hosts_rows_all.extend(res.get('hosts_rows', []))
            prebid_rows.append(res.get('prebid_row', {}))
            adsids_rows.extend(res.get('ads_ids_rows', []))
            sellers_rows.extend(res.get('sellers_rows', []))
            sim_rows_all.extend(res.get('simulation_rows', []))
            for cc, pct in res.get('posterior', {}).items():
                bycountry_rows.append({'domain': dom, 'country': cc, 'posterior_pct': round(pct*100,4), 'est_requests': res.get('est_by_country', {}).get(cc,0)})
            # HAR analysis rows
            hard = res.get('har')
            if hard:
                har_analysis_rows.append({
                    'domain': dom,
                    'har_path': find_har_file_for_domain(args.har_dir, dom) if args.har_dir else None,
                    'total_requests_in_har': hard.get('total_requests',0),
                    'pubmatic_requests': hard.get('pubmatic_requests',0),
                    'total_fills': sum(hard.get('fills_by_country',{}).values()) if isinstance(hard.get('fills_by_country',{}), dict) else 0,
                    'requests_by_country': json.dumps(dict(hard.get('requests_by_country',{}))) if hard else None,
                    'fills_by_country': json.dumps(dict(hard.get('fills_by_country',{}))) if hard else None
                })
                # also expand per-har rows
                for hr in hard.get('har_rows', []):
                    har_row = {'domain': dom, 'url': hr.get('url'), 'country': hr.get('country'), 'is_fill': hr.get('is_fill'), 'status': hr.get('status')}
                    har_analysis_rows.append(har_row)
            time.sleep(FETCH_DELAY)
        except Exception as e:
            print(f"[ERR] {dom} -> {e}\n{traceback.format_exc()}", file=sys.stderr)
            continue

    df_summary = pd.DataFrame(results)
    df_hosts = pd.DataFrame(hosts_rows_all)
    df_prebid = pd.DataFrame(prebid_rows)
    df_adsids = pd.DataFrame(adsids_rows)
    df_sellers = pd.DataFrame(sellers_rows)
    df_sim = pd.DataFrame(sim_rows_all)
    df_bycountry = pd.DataFrame(bycountry_rows)
    df_har = pd.DataFrame(har_analysis_rows)

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
        if not df_har.empty:
            df_har.to_excel(writer, sheet_name='HAR_Analysis', index=False)

    print(f"[DONE] Wrote {args.out}")

if __name__ == '__main__':
    main()
