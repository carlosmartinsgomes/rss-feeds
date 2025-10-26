# scripts/compare_and_email.py
# Compare feeds_summary.xlsx vs saved sent ids and send email for NEW titles only.
# Uses title-only UID normalization and includes 'description' in the email body/table.
# Saves .github/data/sent_ids.json (ensure workflow persists it after run).

import os, sys, json, re
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

# ---------- helper: normalize title -> UID ----------
def normalize_title(t):
    if not t:
        return ''
    t = str(t).strip().lower()
    # remove punctuation, multiple whitespace
    t = re.sub(r'[^\w\s]', ' ', t, flags=re.UNICODE)
    t = re.sub(r'\s+', ' ', t).strip()
    return t

def make_id_from_title(title):
    return "title:" + normalize_title(title)

# ---------- read/write sent ids ----------
def load_sent_ids(path):
    try:
        if not os.path.exists(path):
            return []
        with open(path,'r',encoding='utf-8') as fh:
            return json.load(fh)
    except Exception as e:
        print("Error loading sent ids:", e)
        return []

def save_sent_ids(path, ids):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path,'w',encoding='utf-8') as fh:
        json.dump(ids, fh, indent=2, ensure_ascii=False)
    print("Saved sent ids to", path)

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
    for _, r in df.iterrows():
        # try multiple possible column names for description
        desc = None
        for k in ("description","desc_preview","summary","description (source)","summary_tail"):
            if k in r.index and r.get(k) is not None:
                desc = str(r.get(k) or "")
                break
        # fallback to match column if no separate description
        if not desc:
            desc = str(r.get("match") or "")
        rows.append({
            "site": str(r.get("site") or ""),
            "title": str(r.get("title") or ""),
            "description": desc,
            "pubDate": str(r.get("pubDate") or ""),
            "link (source)": str(r.get("link (source)") or r.get("link") or ""),
            "match": str(r.get("match") or "")
        })
    return rows

def rows_to_html_table(rows):
    html = "<table border='1' cellpadding='6' cellspacing='0'>"
    html += "<tr><th>site</th><th>title</th><th>description</th><th>pubDate</th><th>link</th><th>match</th></tr>"
    for r in rows:
        html += "<tr>"
        html += "<td>{}</td>".format((r.get("site") or "")[:100])
        html += "<td>{}</td>".format((r.get("title") or "")[:400])
        html += "<td>{}</td>".format((r.get("description") or "")[:400])
        html += "<td>{}</td>".format((r.get("pubDate") or "")[:60])
        html += "<td>{}</td>".format((r.get("link (source)") or r.get("link") or "")[:300])
        html += "<td>{}</td>".format((r.get("match") or "")[:300])
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
    print("DEBUG: loaded sent_ids count =", len(sent_ids))
    sent_set = set(sent_ids)

    new_rows = []
    new_ids = []
    for r in rows:
        title = r.get("title") or ""
        uid = make_id_from_title(title)
        if uid not in sent_set:
            new_rows.append(r)
            new_ids.append(uid)
            sent_set.add(uid)

    print("DEBUG: new_rows count =", len(new_rows))

    if not new_rows:
        print("No new rows to email (all already sent previously).")
        save_sent_ids(SENT_IDS_FILE, sorted(list(sent_set)))
        # print snippet of saved ids for debug
        print("Saved sent_ids sample:", json.dumps(sorted(list(sent_set))[:20], ensure_ascii=False))
        return 0

    subj = f"[RSS FEEDS] {len(new_rows)} new item(s)"
    plain_lines = []
    for r in new_rows:
        plain_lines.append(f"- {r.get('title')} ({r.get('site')})\n  {r.get('link (source)')}\n  desc: {r.get('description')}\n  match: {r.get('match')}\n")
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
            # still save sent ids? No: if sending failed we may prefer not to mark them as sent.
            return 2
    else:
        print("EMAIL_READY != 1 -> skipping actual send (but will save sent ids).")

    # update sent ids file (save the new state)
    try:
        save_sent_ids(SENT_IDS_FILE, sorted(list(sent_set)))
        print("Saved sent_ids count:", len(sent_set))
        print("Saved sent_ids sample:", json.dumps(sorted(list(sent_set))[:20], ensure_ascii=False))
    except Exception as e:
        print("Error saving sent ids file:", e)
        return 3

    return 0

if __name__ == "__main__":
    sys.exit(main())
