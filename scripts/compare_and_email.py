# scripts/compare_and_email.py
# Compare feeds_summary.xlsx vs saved sent ids and send email for NEW titles only.
# Canonical UID: SHA1(normalized_title)
# Backwards compatible with older saved ids that may be:
#  - raw 40-char hex SHA1 strings
#  - strings starting with "title:..." (old format)
# The script will normalize old entries into canonical SHA1 form when saving.

import os, sys, json, re, hashlib
from email.message import EmailMessage
import smtplib

# ---------- env helpers ----------
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

# ---------- helper: normalize title -> canonical SHA1 UID ----------
def normalize_title(t):
    if not t:
        return ''
    t = str(t).strip().lower()
    # normalize unicode (NFC) to reduce differences
    try:
        import unicodedata
        t = unicodedata.normalize('NFC', t)
    except Exception:
        pass
    # remove punctuation (keep word chars and spaces), collapse whitespace
    t = re.sub(r'[^\w\s]', ' ', t, flags=re.UNICODE)
    t = re.sub(r'\s+', ' ', t).strip()
    return t

def sha1_of_text(s):
    return hashlib.sha1(s.encode('utf-8')).hexdigest()

def make_uid_from_title(title):
    norm = normalize_title(title)
    return sha1_of_text(norm)

# ---------- read/write sent ids ----------
def load_sent_ids(path):
    try:
        if not os.path.exists(path):
            return []
        with open(path,'r',encoding='utf-8') as fh:
            data = json.load(fh)
            if not isinstance(data, list):
                print("Warning: sent_ids file parsed but top-level is not a list. Attempting best-effort handling.")
                # try to coerce
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
        # If it looks like a 40-char hex -> assume it's already SHA1
        if re.fullmatch(r'[0-9a-f]{40}', s, flags=re.IGNORECASE):
            canonical.add(s.lower())
            kept += 1
        elif s.startswith('title:'):
            # old format: compute SHA1(normalize(title-tail))
            tail = s[len('title:'):]
            uid = make_uid_from_title(tail)
            canonical.add(uid)
            converted += 1
        else:
            # fallback: try to interpret as raw title string -> hash it
            # This ensures older heterogeneous formats are handled.
            uid = make_uid_from_title(s)
            canonical.add(uid)
            converted += 1

    print(f"DEBUG: loaded sent_ids count (raw) = {len(data)} -> canonical SHA1 count = {len(canonical)} (kept={kept} converted={converted})")
    return sorted(list(canonical))

def save_sent_ids(path, ids):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    # ids expected as list of canonical sha1 hex strings
    try:
        with open(path,'w',encoding='utf-8') as fh:
            json.dump(sorted(ids), fh, indent=2, ensure_ascii=False)
        print("Saved sent ids to", path)
        print("Saved sent_ids count:", len(ids))
    except Exception as e:
        print("Error saving sent ids:", e)

# ---------- read Excel (pandas used) ----------
def read_feed_summary(path):
    try:
        import pandas as pd
    except Exception as e:
        print("Error: pandas not installed:", e)
        return []
    if not os.path.exists(path):
        print("feeds_summary.xlsx not found at", path)
        return []
    try:
        df = pd.read_excel(path, engine="openpyxl")
    except Exception as e:
        print("Error reading Excel:", e)
        return []
    rows = []
    # normalize column names access without being too strict:
    for _, r in df.iterrows():
        rows.append({
            "site": str(r.get("site") or r.get("Site") or "") ,
            "title": str(r.get("title") or r.get("Title") or ""),
            "description": str(r.get("description") or r.get("desc") or r.get("Description") or ""),
            "pubDate": str(r.get("pubDate") or r.get("date") or r.get("pubDate") or ""),
            "link (source)": str(r.get("link (source)") or r.get("link") or r.get("Link") or ""),
            "match": str(r.get("match") or "")
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

def main():
    rows = read_feed_summary(FEEDS_XLSX)
    print("DEBUG: all_rows length =", len(rows))
    if not rows:
        print("No rows found in feed summary -> nothing to send")
        return 0

    sent_ids = load_sent_ids(SENT_IDS_FILE)
    sent_set = set([s.lower() for s in sent_ids])

    new_rows = []
    new_ids = []
    for r in rows:
        title = r.get("title") or ""
        uid = make_uid_from_title(title)
        if uid not in sent_set:
            new_rows.append(r)
            new_ids.append(uid)
            sent_set.add(uid)

    print("DEBUG: new_rows count =", len(new_rows))
    if len(new_rows) > 0:
        print("DEBUG: new UIDs (first 50):", new_ids[:50])

    if not new_rows:
        print("No new rows to email (all already sent previously).")
        # still save canonical sent ids to persist any conversions
        save_sent_ids(SENT_IDS_FILE, sorted(list(sent_set)))
        return 0

    subj = f"[RSS FEEDS] {len(new_rows)} new item(s)"
    plain_lines = []
    for r in new_rows:
        plain_lines.append(f"- {r.get('title')} ({r.get('site')})\n  {r.get('link (source)')}\n  match: {r.get('match')}\n  desc: {r.get('description')}\n")
    plain = "\n".join(plain_lines)
    html = "<html><body>"
    html += f"<p>{len(new_rows)} new item(s) detected:</p>"
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

    # update sent ids file (save canonical SHA1 list)
    try:
        save_sent_ids(SENT_IDS_FILE, sorted(list(sent_set)))
    except Exception as e:
        print("Error saving sent ids file:", e)
        return 3

    return 0

if __name__ == "__main__":
    sys.exit(main())
