# scripts/compare_and_email.py
# Compare current feeds_summary.xlsx vs previous Excel (if available) OR fallback to sent_ids,
# send email for NEW titles only. Canonical UID: SHA1(normalized_title).

import os, sys, json, re, hashlib
from email.message import EmailMessage
import smtplib
import unicodedata

def getenv_first(*names, default=''):
    for n in names:
        v = os.environ.get(n)
        if v is not None and str(v).strip() != '':
            return v
    return default

SMTP_HOST = getenv_first('SMTP_HOST','SMTP_SERVER','')
SMTP_PORT = getenv_first('SMTP_PORT','')
SMTP_USER = getenv_first('SMTP_USER','SMTP_USERNAME','')
SMTP_PASS = getenv_first('SMTP_PASS','SMTP_PASSWORD','')
EMAIL_FROM = getenv_first('EMAIL_FROM','')
EMAIL_TO = getenv_first('EMAIL_TO','')
SMTP_USE_SSL = getenv_first('SMTP_USE_SSL','').lower() in ('1','true','yes','on')

FEEDS_XLSX = os.environ.get('FEEDS_XLSX','feeds_summary.xlsx')
PREV_FEEDS_XLSX = os.environ.get('PREV_FEEDS_XLSX','prev_feeds_summary.xlsx')
SENT_IDS_FILE = os.environ.get('SENT_IDS_FILE','.github/data/sent_ids.json')

print("SMTP/EMAIL environment presence (not values):")
for (k,v) in [('SMTP_HOST',SMTP_HOST),('SMTP_PORT',SMTP_PORT),('SMTP_USER',SMTP_USER),('SMTP_PASS','***' if SMTP_PASS else ''),('EMAIL_FROM',EMAIL_FROM),('EMAIL_TO',EMAIL_TO)]:
    print(f"  {k}: {'SET' if v else 'UNSET'}")

essential_missing = False
missing = []
if not SMTP_HOST or not SMTP_PORT or not EMAIL_FROM or not EMAIL_TO:
    essential_missing = True
    if not SMTP_HOST: missing.append('SMTP_HOST')
    if not SMTP_PORT: missing.append('SMTP_PORT')
    if not EMAIL_FROM: missing.append('EMAIL_FROM')
    if not EMAIL_TO: missing.append('EMAIL_TO')

if essential_missing:
    print("Email not sent: missing required env vars:", ",".join(missing))
    os.environ['EMAIL_READY'] = '0'
else:
    os.environ['EMAIL_READY'] = '1'

def send_email(subject, body_text, attach_path=None, html=None):
    if os.environ.get('EMAIL_READY') != '1':
        print("send_email() chamado mas EMAIL_READY != 1 -> não enviar")
        return False
    port = int(SMTP_PORT or 0)
    try:
        if SMTP_USE_SSL or port == 465:
            server = smtplib.SMTP_SSL(SMTP_HOST, port, timeout=30)
        else:
            server = smtplib.SMTP(SMTP_HOST, port, timeout=30)
            server.ehlo()
            try:
                server.starttls()
                server.ehlo()
            except Exception:
                pass

        if SMTP_USER and SMTP_PASS:
            server.login(SMTP_USER, SMTP_PASS)

        msg = EmailMessage()
        msg['From'] = EMAIL_FROM
        msg['To'] = EMAIL_TO
        msg['Subject'] = subject
        if html:
            msg.set_content(body_text)
            msg.add_alternative(html, subtype='html')
        else:
            msg.set_content(body_text)

        if attach_path and os.path.exists(attach_path):
            with open(attach_path,'rb') as f:
                data = f.read()
            msg.add_attachment(data, maintype='application', subtype='octet-stream', filename=os.path.basename(attach_path))

        server.send_message(msg)
        server.quit()
        print("Email sent successfully.")
        return True
    except Exception as e:
        print("SMTP login/send failed:", repr(e))
        return False

# ---------- normalization helpers ----------
def normalize_text_for_compare(s):
    """
    Normalize text for robust equality/comparison and display:
    - ensure string
    - unicode-normalize (NFKC)
    - replace NBSP with regular space
    - collapse whitespace
    - strip (but do NOT lowercase here — UID generation uses normalize_title)
    """
    if s is None:
        return ''
    t = str(s)
    try:
        t = unicodedata.normalize('NFKC', t)
    except Exception:
        pass
    t = t.replace('\u00A0', ' ')
    # remove zero-width/control characters that may differ between runs
    t = re.sub(r'[\u200B-\u200F\uFEFF]', '', t)
    # collapse whitespace
    t = re.sub(r'\s+', ' ', t)
    return t.strip()

# ---------- normalization and uid ----------
def normalize_title(t):
    if not t:
        return ''
    t = str(t).strip().lower()
    try:
        import unicodedata as _ud
        t = _ud.normalize('NFC', t)
    except Exception:
        pass
    # remove zero-width and control characters
    t = re.sub(r'[\u200B-\u200F\uFEFF]', '', t)
    # remove punctuation
    t = re.sub(r'[^\w\s]', ' ', t, flags=re.UNICODE)
    t = re.sub(r'\s+', ' ', t).strip()
    return t

def sha1_of_text(s):
    return hashlib.sha1(s.encode('utf-8')).hexdigest()

def make_uid_from_title(title):
    return sha1_of_text(normalize_title(title))

# ---------- read excel ----------
def read_feed_summary(path):
    try:
        import pandas as pd
    except Exception as e:
        print("Error: pandas not installed:", e)
        return []
    if not os.path.exists(path):
        print("Excel not found at", path)
        return []
    try:
        df = pd.read_excel(path, engine="openpyxl")
    except Exception as e:
        print("Error reading Excel:", e)
        return []
    rows = []
    for _, r in df.iterrows():
        # descrição: tenta múltiplas chaves (case-sensitive & variants) porque o excel pode ter "description (short)"
        descr = (
            r.get("description")
            or r.get("Description")
            or r.get("description (short)")
            or r.get("Description (short)")
            or r.get("desc")
            or r.get("Desc")
            or ""
        )

        site_raw = r.get("site") or r.get("Site") or ""
        title_raw = r.get("title") or r.get("Title") or ""
        pub_raw = r.get("pubDate") or r.get("date") or r.get("pubDate") or r.get("Date") or ""
        link_raw = r.get("link (source)") or r.get("link") or r.get("Link") or ""
        match_raw = r.get("match") or r.get("matched_reason") or ""

        # normalize for consistent comparison/display but keep raw title too
        site_norm = normalize_text_for_compare(site_raw)[:120]
        title_norm = normalize_text_for_compare(title_raw)[:400]
        desc_norm = normalize_text_for_compare(descr)[:500]

        rows.append({
            "site": site_norm,
            "title": title_norm,
            "title_raw": str(title_raw),
            "description": desc_norm,
            "pubDate": str(pub_raw),
            "link (source)": str(link_raw),
            "match": str(match_raw)
        })
    return rows


def rows_to_html_table(rows):
    html = "<table border='1' cellpadding='6' cellspacing='0' style='border-collapse:collapse;font-family:Arial,sans-serif;font-size:13px;'>"
    html += "<tr style='background:#efefef'><th>site</th><th>title</th><th>description</th><th>pubDate</th><th>link</th><th>match</th></tr>"
    for r in rows:
        site = (r.get("site") or "")[:120]
        title = (r.get("title") or "")[:400]
        desc = (r.get("description") or "")[:500]
        pub = (r.get("pubDate") or "")[:80]
        link = (r.get("link (source)") or "")[:350]
        match = (r.get("match") or "")[:300]
        html += "<tr>"
        html += "<td>{}</td>".format(site.replace("<","&lt;").replace(">","&gt;"))
        html += "<td>{}</td>".format(title.replace("<","&lt;").replace(">","&gt;"))
        html += "<td>{}</td>".format(desc.replace("<","&lt;").replace(">","&gt;"))
        html += "<td>{}</td>".format(pub.replace("<","&lt;").replace(">","&gt;"))
        html += "<td><a href='{0}'>{0}</a></td>".format(link.replace("'", "%27").replace("<","&lt;").replace(">","&gt;"))
        html += "<td>{}</td>".format(match.replace("<","&lt;").replace(">","&gt;"))
        html += "</tr>"
    html += "</table>"
    return html

# ---------- sent ids helpers (backwards compat) ----------
def load_sent_ids(path):
    try:
        if not os.path.exists(path):
            return []
        with open(path,'r',encoding='utf-8') as fh:
            data = json.load(fh)
            if not isinstance(data, list):
                if isinstance(data, dict):
                    data = list(data.keys())
                else:
                    data = list(data)
    except Exception as e:
        print("Error loading sent ids:", e)
        return []
    canonical = set()
    converted = 0
    kept = 0
    for entry in data:
        try:
            s = str(entry)
        except Exception:
            continue
        s = s.strip()
        if re.fullmatch(r'[0-9a-f]{40}', s, flags=re.IGNORECASE):
            canonical.add(s.lower()); kept += 1
        elif s.startswith('title:'):
            tail = s[len('title:'):]
            uid = make_uid_from_title(tail)
            canonical.add(uid); converted += 1
        else:
            uid = make_uid_from_title(s)
            canonical.add(uid); converted += 1
    print(f"DEBUG: loaded sent_ids count (raw) = {len(data)} -> canonical SHA1 count = {len(canonical)} (kept={kept} converted={converted})")
    return sorted(list(canonical))

def save_sent_ids(path, ids):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    try:
        with open(path,'w',encoding='utf-8') as fh:
            json.dump(sorted(ids), fh, indent=2, ensure_ascii=False)
        print("Saved sent ids to", path)
        print("Saved sent_ids count:", len(ids))
    except Exception as e:
        print("Error saving sent ids:", e)

# ---------- main logic ----------
def main():
    current_rows = read_feed_summary(FEEDS_XLSX)
    print("DEBUG: all_rows length =", len(current_rows))
    if not current_rows:
        print("No rows found in current feed summary -> nothing to send")
        return 0

    # prefer previous Excel if present
    prev_uids_set = set()
    prev_mode = None
    if PREV_FEEDS_XLSX and os.path.exists(PREV_FEEDS_XLSX):
        prev_rows = read_feed_summary(PREV_FEEDS_XLSX)
        for r in prev_rows:
            # use the title (normalized) from prev_rows to generate uid
            uid = make_uid_from_title(r.get("title") or r.get("title_raw") or "")
            if uid:
                prev_uids_set.add(uid)
        prev_mode = 'prev_excel'
        print(f"DEBUG: prev Excel found at {PREV_FEEDS_XLSX} -> prev UIDs count = {len(prev_uids_set)}")
    else:
        # fallback to sent_ids
        sent_ids = load_sent_ids(SENT_IDS_FILE)
        prev_uids_set = set(sent_ids)
        prev_mode = 'sent_ids'
        print(f"DEBUG: using sent_ids fallback -> count = {len(prev_uids_set)}")

    new_rows = []
    new_ids = []
    for r in current_rows:
        # use title_raw if available, else title (both are present from read_feed_summary)
        title_for_uid = r.get("title_raw") or r.get("title") or ""
        uid = make_uid_from_title(title_for_uid)
        if uid and uid not in prev_uids_set:
            new_rows.append(r)
            new_ids.append(uid)
            prev_uids_set.add(uid)

    print("DEBUG: new_rows count =", len(new_rows))
    if len(new_rows) > 0:
        print("DEBUG: new UIDs (first 50):", new_ids[:50])

    if not new_rows:
        print("No new rows to email (all already sent previously by chosen baseline).")
        # Ensure we persist canonical sent ids (merge with existing saved)
        save_sent_ids(SENT_IDS_FILE, sorted(list(prev_uids_set)))
        return 0

    subj = f"[RSS FEEDS] {len(new_rows)} new item(s)"
    plain_lines = []
    for r in new_rows:
        plain_lines.append(f"- {r.get('title')} ({r.get('site')})\n  {r.get('link (source)')}\n  match: {r.get('match')}\n  desc: {r.get('description')}\n")
    plain = "\n".join(plain_lines)
    html = "<html><body>"
    html += f"<p>{len(new_rows)} new item(s) detected (baseline: {prev_mode}):</p>"
    html += rows_to_html_table(new_rows)
    html += "</body></html>"

    sent_ok = False
    if os.environ.get('EMAIL_READY') == '1':
        sent_ok = send_email(subj, plain, attach_path=FEEDS_XLSX, html=html)
        if not sent_ok:
            print("Failed to send email (see logs above).")
            return 2
    else:
        print("EMAIL_READY != 1 -> skipping actual send (but will save sent ids).")

    # save merged sent ids
    try:
        save_sent_ids(SENT_IDS_FILE, sorted(list(prev_uids_set)))
    except Exception as e:
        print("Error saving sent ids file:", e)
        return 3

    return 0

if __name__ == "__main__":
    sys.exit(main())
