#!/usr/bin/env python3
"""
estimate_pubmatic_country_percentages.py (AGGRESSIVE scanning, multi-country)

- Aggressive HTML scanning to find PubMatic-related hosts (prebid inline configs, script src, inline JSON, URLs).
- Resolves hosts -> IPs, geolocates IPs, aggregates by countryCode (any country).
- Supports optional priors.csv with dynamic country columns:
    priors.csv header: domain,US,GB,AU,DE,IN,...  (values are fractions, ideally sum ~1)
- Outputs XLSX with sheets:
    - Estimates (summary per domain)
    - ByCountry  (domain,country,posterior_pct,est_requests,observed_count,prior_frac)
    - Detected_Hosts (domain,host,ip,country)
"""
import argparse, requests, socket, re, time, csv, json, sys
from collections import Counter, defaultdict
from urllib.parse import urlparse
import pandas as pd

# -----------------------
# Config
# -----------------------
# Regex to capture domain-like tokens (more permissive)
DOMAIN_RE = re.compile(r'([a-z0-9\-_\.]+\.[a-z]{2,})', re.IGNORECASE)
# Patterns that hint to PubMatic / OpenWrap / bidder config -> increased chance
KEYWORDS = ['pubmatic', 'openwrap', 'openwrapsdk', 'hb.pubmatic', 'ads.pubmatic', 'prebid', 'pbjs', 'bidder']
# default geo API (free)
GEO_API = "http://ip-api.com/json/{ip}?fields=status,countryCode,query,message"
# default small delay between geo lookups to avoid quick rate-limit
GEO_DELAY = 0.45

# -----------------------
# Helpers
# -----------------------
def fetch_url(url, timeout=10):
    headers = {'User-Agent': 'Mozilla/5.0 (compatible; PubMaticEstimator/1.0)'}
    try:
        r = requests.get(url, headers=headers, timeout=timeout)
        return r.status_code, r.text
    except Exception:
        return None, ""

def extract_hosts_aggressive(html, base_domain=None):
    """
    Aggressively extract hosts and domain-like tokens from HTML and inline scripts.
    Returns list of host strings (lowercased, deduped).
    """
    if not html:
        return []
    hosts = []
    lower = html.lower()

    # 1) direct occurrences of pubmatic-like hostnames
    for m in re.finditer(r'([a-z0-9\-_\.]*pubmatic[a-z0-9\-_\.]*\.[a-z]{2,6})', html, flags=re.I):
        hosts.append(m.group(1).lower())

    # 2) all script src / url occurrences
    for m in re.finditer(r'(https?://[^\s"\'>]+)', html, flags=re.I):
        url = m.group(1)
        try:
            p = urlparse(url)
            host = p.hostname
            if host:
                host = host.lower()
                # if host contains pubmatic or other keywords keep
                if any(k in host for k in KEYWORDS):
                    hosts.append(host)
                else:
                    # also keep hosts near known keywords in the URL path
                    if 'pubmatic' in url.lower():
                        hosts.append(host)
        except:
            pass

    # 3) extract JSON-like blocks near "pbjs" or "bidder" occurrences
    # find segments up to 600 chars around keywords and search for domain-like tokens
    for kw in ['pbjs','bidder','bid','adUnit','adUnitPath','publisherId','params']:
        for m in re.finditer(r'.{0,300}'+re.escape(kw)+r'.{0,300}', html, flags=re.I):
            seg = m.group(0)
            for mm in DOMAIN_RE.finditer(seg):
                h = mm.group(1).lower()
                if any(c.isalpha() for c in h):
                    hosts.append(h)

    # 4) general domain tokens anywhere but give them lower priority
    for mm in DOMAIN_RE.finditer(html):
        h = mm.group(1).lower()
        # keep if contains a KEYWORD or if it's a known ad host pattern (heuristic)
        if any(k in h for k in KEYWORDS):
            hosts.append(h)

    # 5) dedupe but preserve order
    seen = set()
    out = []
    for h in hosts:
        if not h:
            continue
        # strip port if present
        if ':' in h:
            h = h.split(':',1)[0]
        if h not in seen:
            seen.add(h)
            out.append(h)
    # Optional: if none found but base_domain provided, include base_domain as fallback
    if not out and base_domain:
        out.append(base_domain.lower())
    return out

def resolve_host(host):
    """Resolve host to IPv4 addresses (list). Return empty list if none."""
    ips = set()
    try:
        infos = socket.getaddrinfo(host, None)
        for info in infos:
            addr = info[4][0]
            if ':' in addr:  # skip IPv6 for now
                continue
            ips.add(addr)
    except Exception:
        pass
    return list(ips)

# geo cache
GEO_CACHE = {}
def geolocate_ip(ip, delay=GEO_DELAY):
    if not ip:
        return ''
    if ip in GEO_CACHE:
        return GEO_CACHE[ip]
    try:
        url = GEO_API.format(ip=ip)
        r = requests.get(url, timeout=8)
        if r.status_code == 200:
            j = r.json()
            if j.get('status') == 'success':
                c = j.get('countryCode','') or ''
                GEO_CACHE[ip] = c
                time.sleep(delay)
                return c
    except Exception:
        pass
    GEO_CACHE[ip] = ''
    time.sleep(delay)
    return ''

def load_priors_flexible(priors_file):
    """
    Load priors CSV if exists.
    Format: header must include 'domain' and then country codes as columns (US,GB,AU,...)
    Values are fractions (0..1). If a domain row lacks some country columns, treat missing as 0.
    Returns dict: priors[domain] = {country: fraction, ...}
    """
    priors = {}
    try:
        with open(priors_file, newline='', encoding='utf-8') as f:
            rdr = csv.DictReader(f)
            fields = rdr.fieldnames or []
            country_cols = [c for c in fields if c and c.lower()!='domain']
            for r in rdr:
                d = r.get('domain','').strip()
                if not d:
                    continue
                p = {}
                for c in country_cols:
                    try:
                        p[c.upper()] = float(r.get(c,0) or 0.0)
                    except:
                        p[c.upper()] = 0.0
                # Normalize sum to 1 if >0
                s = sum(p.values())
                if s > 0:
                    for k in p:
                        p[k] = p[k] / s
                priors[d] = p
    except FileNotFoundError:
        return {}
    except Exception:
        return {}
    return priors

# -----------------------
# Main analysis per domain
# -----------------------
def analyze_domain(dom, priors_for_domain=None, alpha=10.0, timeout=10):
    """
    Returns:
      - observed_counts: Counter(countryCode -> count)
      - hosts_detail: list of dicts {host, ip, country}
      - posterior: dict country -> posterior fraction
      - est_requests_by_country: dict country -> est_count (rounded)
      - confidence: heuristic 0..1
    """
    base = dom if dom.startswith('http') else f"https://{dom}"
    code, html = fetch_url(base, timeout=timeout)
    if code is None:
        # fallback to http
        base = f"http://{dom}"
        code, html = fetch_url(base, timeout=timeout)

    # aggressive host extraction (use domain as fallback)
    hosts = extract_hosts_aggressive(html, base_domain=dom)

    hosts_detail = []
    observed = Counter()
    for h in hosts:
        ips = resolve_host(h)
        if not ips:
            # still include host with empty ip
            hosts_detail.append({'host': h, 'ip': '', 'country': ''})
            continue
        for ip in ips:
            c = geolocate_ip(ip)
            if c:
                observed[c.upper()] += 1
            else:
                observed[''] += 1
            hosts_detail.append({'host': h, 'ip': ip, 'country': c.upper() if c else ''})

    observed_total = sum(v for k,v in observed.items() if k)  # only count known country codes
    # build domain list of candidate countries
    observed_countries = sorted([c for c in observed.keys() if c and c.strip()])
    # if priors provided, use their country set; else combine observed + 'OTHER'
    if priors_for_domain:
        # priors_for_domain is dict country->frac (sum to 1)
        countries = sorted(list(priors_for_domain.keys()))
    else:
        # if observed countries exist, use them + OTHER bucket; else we will fallback to UNKNOWN
        if observed_countries:
            countries = observed_countries + ['OTHER']
        else:
            countries = []  # will be handled below

    # if no countries and no priors => nothing observed -> return empty posterior
    if not countries:
        return observed, hosts_detail, {}, {}, 0.10  # very low confidence

    # build prior vector
    prior = {}
    if priors_for_domain:
        # use priors_for_domain (already normalized)
        for c in countries:
            prior[c] = priors_for_domain.get(c, 0.0)
        # ensure sum>0
        s = sum(prior.values())
        if s <= 0:
            # fallback to uniform
            for c in countries:
                prior[c] = 1.0/len(countries)
        else:
            for c in prior:
                prior[c] = prior[c]/s
    else:
        # uniform prior across countries
        for c in countries:
            prior[c] = 1.0/len(countries)

    # observed counts per those countries (map missing observed -> 0)
    observed_counts = {}
    for c in countries:
        if c == 'OTHER':
            # other = counts not in observed_countries
            observed_counts['OTHER'] = sum(v for k,v in observed.items() if k not in observed_countries)
        else:
            observed_counts[c] = observed.get(c, 0)

    # Dirichlet-like posterior counts: alpha * prior + observed
    a = {}
    for c in countries:
        a[c] = alpha * prior.get(c, 0.0) + observed_counts.get(c, 0)

    s = sum(a.values())
    if s <= 0:
        posterior = {c: 0.0 for c in countries}
    else:
        posterior = {c: a[c]/s for c in countries}

    # confidence heuristic: based on observed_total signals and host count
    confidence = min(0.95, 0.15 + 0.12 * max(0, observed_total) + 0.05 * len(hosts))

    return observed, hosts_detail, posterior, observed_counts, confidence

# -----------------------
# Orchestrator
# -----------------------
def analyze_domains(domains, priors_map, total_requests=1000, alpha=10.0, timeout=10):
    results = []
    bycountry_rows = []
    detected_hosts_rows = []

    for dom in domains:
        dom = dom.strip()
        if not dom:
            continue
        priors_for_domain = priors_map.get(dom, None)
        observed, hosts_detail, posterior, observed_counts, confidence = analyze_domain(dom, priors_for_domain, alpha=alpha, timeout=timeout)

        # if posterior empty -> fallback: if priors exist use priors_for_domain as posterior; else mark UNKNOWN
        if not posterior:
            if priors_for_domain:
                posterior = priors_for_domain.copy()
            else:
                posterior = {'UNKNOWN': 1.0}

        # compute estimated requests per country
        est_by_country = {}
        for c, frac in posterior.items():
            est_by_country[c] = int(round(frac * total_requests))

        # append results summary row
        results.append({
            'domain': dom,
            'pubmatic_signals_found': bool(hosts_detail),
            'num_hosts_detected': len({h['host'] for h in hosts_detail}),
            'observed_signal_sum': sum(observed.values()),
            'confidence': round(confidence,3),
            'posterior_json': json.dumps(posterior),
            'est_requests_json': json.dumps(est_by_country),
            'hosts_detail_json': json.dumps(hosts_detail)
        })

        # by-country rows
        for c, frac in posterior.items():
            bycountry_rows.append({
                'domain': dom,
                'country': c,
                'posterior_pct': round(frac*100,4),
                'est_requests': est_by_country.get(c,0),
                'observed_count': observed_counts.get(c,0) if isinstance(observed_counts, dict) else 0,
                'prior_frac': (priors_for_domain.get(c,0.0) if priors_for_domain else None)
            })

        # detected hosts
        for hd in hosts_detail:
            detected_hosts_rows.append({
                'domain': dom,
                'host': hd.get('host',''),
                'ip': hd.get('ip',''),
                'country': hd.get('country','')
            })

    df_summary = pd.DataFrame(results)
    df_bycountry = pd.DataFrame(bycountry_rows)
    df_hosts = pd.DataFrame(detected_hosts_rows)
    return df_summary, df_bycountry, df_hosts

# -----------------------
# CLI
# -----------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--domains-file', required=True, help='plain text file with domains, one per line (principaldomains)')
    parser.add_argument('--out', default='pubmatic_country_estimates.xlsx', help='xlsx output filename')
    parser.add_argument('--total-requests', type=int, default=1000, help='assumed total PubMatic requests per domain (for absolute estimates)')
    parser.add_argument('--alpha', type=float, default=5.0, help='prior strength (Dirichlet) for smoothing; smaller -> rely more on observations')
    parser.add_argument('--timeout', type=int, default=10, help='HTTP timeout seconds for fetching homepages')
    parser.add_argument('--priors-file', default='priors.csv', help='optional priors CSV with header domain,<country codes>')
    args = parser.parse_args()

    # load domains
    with open(args.domains_file, 'r', encoding='utf-8') as f:
        domains = [line.strip() for line in f if line.strip()]

    # load priors (flexible)
    priors_map = {}
    temp = load_priors_flexible(args.priors_file)
    # normalise priors per domain (safety)
    for d, p in temp.items():
        s = sum(p.values())
        if s>0:
            priors_map[d] = {k: (v/s) for k,v in p.items()}
        else:
            priors_map[d] = {}

    df_summary, df_bycountry, df_hosts = analyze_domains(domains, priors_map, total_requests=args.total_requests, alpha=args.alpha, timeout=args.timeout)

    # Write Excel with multiple sheets
    with pd.ExcelWriter(args.out, engine='openpyxl') as writer:
        df_summary.to_excel(writer, sheet_name='Estimates', index=False)
        df_bycountry.to_excel(writer, sheet_name='ByCountry', index=False)
        df_hosts.to_excel(writer, sheet_name='Detected_Hosts', index=False)

    print(f"Wrote {args.out}")

if __name__ == '__main__':
    main()
