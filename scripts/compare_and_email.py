#!/usr/bin/env python3
# scripts/compare_and_email.py
# Versão ajustada: envia HTML no corpo (sem anexos por omissão). Configurar via GitHub Secrets.

import os
import sys
import json
import hashlib
from email.message import EmailMessage
import smtplib

# Import pandas somente quando necessário
try:
    import pandas as pd
except Exception:
    pd = None

# --- util ---
def getenv_first(*names, default=''):
    for n in names:
        v = os.environ.get(n)
        if v is not None and str(v).strip() != '':
            return v
    return default

# --- read envs (compatibilidade com vários nomes) ---
SMTP_HOST = getenv_first('SMTP_HOST', 'SMTP_SERVER', '')
SMTP_PORT = getenv_first('SMTP_PORT', '')
SMTP_USER = getenv_first('SMTP_USER', 'SMTP_USERNAME', '')
SMTP_PASS = getenv_first('SMTP_PASS', 'SMTP_PASSWORD', '')
EMAIL_FROM = getenv_first('EMAIL_FROM', '')
EMAIL_TO = getenv_first('EMAIL_TO', '')
SMTP_USE_SSL = getenv_first('SMTP_USE_SSL', '').lower() in ('1','true','yes','on')
SEND_ATTACHMENT = getenv_first('SEND_ATTACHMENT', '0') in ('1','true','yes','on')

FEEDS_XLSX = os.environ.get('FEEDS_XLSX', 'feeds_summary.xlsx')
SENT_IDS_FILE = os.environ.get('SENT_IDS_FILE', '.github/data/sent_ids.json')

# --- diagnostic presence (no values) ---
print("SMTP/EMAIL environment presence (not values):")
for (k, v) in [
    ('SMTP_HOST', SMTP_HOST),
    ('SMTP_PORT', SMTP_PORT),
    ('SMTP_USER', SMTP_USER),
    ('SMTP_PASS', '***' if SMTP_PASS else ''),
    ('EMAIL_FROM', EMAIL_FROM),
    ('EMAIL_TO', EMAIL_TO),
]:
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

def send_email(subject, plain_text, html_text=None, attach_path=None):
    if os.environ.get('EMAIL_READY') != '1':
        print("send_email() called but EMAIL_READY != 1 -> skipping send")
        return False

    port = int(SMTP_PORT or 0)
    try:
        if SMTP_USE_SSL or port == 465:
            server = smtplib.SMTP_SSL(SMTP_HOST, port, timeout=30)
        else:
            server = smtplib.SMTP(SMTP_HOST, port, timeout=30)
            server.ehlo()
            # tentar STARTTLS se possível
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
        msg.set_content(plain_text or " ")

        if html_text:
            msg.add_alternative(html_text, subtype='html')

        if attach_path and SEND_ATTACHMENT:
            try:
                with open(attach_path, 'rb') as f:
                    data = f.read()
                msg.add_attachment(data, maintype='application', subtype='octet-stream', filename=os.path.basename(attach_path))
            except Exception as e:
                print("Failed to attach file:", e)

        server.send_message(msg)
        server.quit()
        print("Email sent successfully.")
        return True
    except Exception as e:
        print("SMTP login/send failed:", repr(e))
        return False

# --- helpers para ids ---
def make_id(link, title, pubDate):
    raw = (str(link or "") + "|" + str(title or "") + "|" + str(pubDate or ""))
    return hashlib.sha1(raw.encode('utf-8')).hexdigest()

def load_sent_ids(path):
    try:
        if os.path.exists(path):
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            return set(data if isinstance(data, list) else [])
    except Exception:
        pass
    return set()

def save_sent_ids(path, ids_list):
    os.makedirs(os.path.dirname(path) or '.', exist_ok=True)
    try:
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(ids_list, f, ensure_ascii=False, indent=2)
        print("Saved sent ids to", path)
    except Exception as e:
        print("Error saving sent ids file:", e)

def rows_to_html_table(rows):
    html = "<table border='1' cellpadding='6' cellspacing='0' style='border-collapse:collapse;'>"
    html += "<tr><th>site</th><th>title</th><th>pubDate</th><th>link</th><th>match</th></tr>"
    for r in rows:
        html += "<tr>"
        html += "<td>{}</td>".format((r.get("site") or "")[:100])
        title = (r.get("title") or "")[:400]
        html += "<td>{}</td>".format(title.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;"))
        html += "<td>{}</td>".format((r.get("pubDate") or "")[:60])
        link = (r.get("link (source)") or r.get("link") or "")
        html += "<td><a href='{0}'>{0}</a></td>".format(link)
        html += "<td>{}</td>".format((r.get("match") or "")[:300])
        html += "</tr>"
    html += "</table>"
    return html

def read_feed_summary(path: str):
    if not os.path.exists(path):
        print("feeds_summary.xlsx not found at", path)
        return []
    if pd is None:
        print("pandas not importable -> cannot read Excel")
        return []
    try:
        df = pd.read_excel(path, engine="openpyxl")
    except Exception as e:
        print("Error reading Excel:", e)
        return []
    rows = []
    for _, r in df.iterrows():
        rows.append({
            "site": str(r.get("site") or ""),
            "title": str(r.get("title") or ""),
            "pubDate": str(r.get("pubDate") or ""),
            "link (source)": str(r.get("link (source)") or ""),
            "match": str(r.get("match") or "")
        })
    return rows

def main():
    rows = read_feed_summary(FEEDS_XLSX)
    if not rows:
        print("No rows found in feed summary -> nothing to send")
        return 0

    sent_ids = load_sent_ids(SENT_IDS_FILE)
    all_ids = set(sent_ids)
    new_rows = []
    new_ids = []

    for r in rows:
        uid = make_id(r.get("link (source)"), r.get("title"), r.get("pubDate"))
        if uid not in sent_ids:
            new_rows.append(r)
            new_ids.append(uid)
            all_ids.add(uid)

    if not new_rows:
        print("No new rows to email (all already sent previously).")
        save_sent_ids(SENT_IDS_FILE, sorted(list(all_ids)))
        return 0

    subj = f"[RSS FEEDS] {len(new_rows)} new item(s)"
    plain_lines = []
    for r in new_rows:
        plain_lines.append(f"- {r.get('title')} ({r.get('site')})\n  {r.get('link (source)')}\n  match: {r.get('match')}\n")
    plain = "\n".join(plain_lines)

    html = "<html><body>"
    html += f"<p>{len(new_rows)} new item(s) detected:</p>"
    html += rows_to_html_table(new_rows)
    html += "<p>Artifact (download) URL (requires GitHub login): see workflow artifacts in run details.</p>"
    html += "</body></html>"

    ok = send_email(subj, plain, html, attach_path=FEEDS_XLSX if SEND_ATTACHMENT else None)
    if not ok:
        print("Failed to send email (see logs above).")
        return 2

    try:
        save_sent_ids(SENT_IDS_FILE, sorted(list(all_ids)))
    except Exception as e:
        print("Error saving sent ids file:", e)
        return 3

    return 0

if __name__ == "__main__":
    sys.exit(main())
