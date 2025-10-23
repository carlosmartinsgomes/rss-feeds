#!/usr/bin/env python3
# scripts/compare_and_email.py

import os
import sys
import json
import hashlib
from email.message import EmailMessage
import smtplib
import pandas as pd
from typing import List

# ---------- helpers for env reading ----------
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

# debug: presence
print("SMTP/EMAIL environment presence (not values):")
for (k,v) in [
    ('SMTP_HOST', SMTP_HOST),
    ('SMTP_PORT', SMTP_PORT),
    ('SMTP_USER', SMTP_USER),
    ('SMTP_PASS', '***' if SMTP_PASS else ''),
    ('EMAIL_FROM', EMAIL_FROM),
    ('EMAIL_TO', EMAIL_TO),
]:
    print(f"  {k}: {'SET' if v else 'UNSET'}")

EMAIL_READY = bool(SMTP_HOST and SMTP_PORT and EMAIL_FROM and EMAIL_TO)

def send_email(subject: str, plain_text: str, html_text: str = None) -> bool:
    if not EMAIL_READY:
        print("send_email() called but EMAIL_READY is False -> skipping send")
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
        msg.set_content(plain_text or '')

        if html_text:
            msg.add_alternative(html_text, subtype='html')

        server.send_message(msg)
        server.quit()
        print("Email sent successfully.")
        return True
    except Exception as e:
        print("SMTP login/send failed:", e)
        return False

# ---------- ID helpers ----------
def make_id(link, title, pubDate) -> str:
    s = "|".join([str(link or ''), str(title or ''), str(pubDate or '')])
    return hashlib.sha256(s.encode('utf-8')).hexdigest()

def load_sent_ids(path: str) -> List[str]:
    try:
        if os.path.exists(path):
            with open(path, 'r', encoding='utf-8') as f:
                return json.load(f)
    except Exception as e:
        print("load_sent_ids error:", e)
    return []

def save_sent_ids(path: str, ids: List[str]):
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(ids, f, ensure_ascii=False, indent=2)
        print("Saved sent ids:", path)
    except Exception as e:
        print("Error saving sent ids:", e)
        raise

# ---------- read feeds summary ----------
def read_feed_summary(path: str):
    if not os.path.exists(path):
        print("feeds_summary not found at", path)
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
            "link (source)": str(r.get("link (source)") or r.get("link") or ""),
            "match": str(r.get("match") or "")
        })
    return rows

def rows_to_html_table(rows) -> str:
    html = "<table border='1' cellpadding='6' cellspacing='0'>"
    html += "<tr><th>site</th><th>title</th><th>pubDate</th><th>link</th><th>match</th></tr>"
    for r in rows:
        link = (r.get("link (source)") or r.get("link") or "")
        html += "<tr>"
        html += "<td>{}</td>".format((r.get("site") or "")[:120])
        html += "<td>{}</td>".format((r.get("title") or "")[:400])
        html += "<td>{}</td>".format((r.get("pubDate") or "")[:60])
        html += "<td><a href='{0}'>{0}</a></td>".format(link)
        html += "<td>{}</td>".format((r.get("match") or "")[:300])
        html += "</tr>"
    html += "</table>"
    return html

def main():
    rows = read_feed_summary(FEEDS_XLSX)
    if not rows:
        print("No rows found in feed summary -> nothing to send")
        return 0

    sent_ids = set(load_sent_ids(SENT_IDS_FILE) or [])
    new_rows = []
    new_ids = []

    for r in rows:
        uid = make_id(r.get("link (source)"), r.get("title"), r.get("pubDate"))
        if uid not in sent_ids:
            new_rows.append(r)
            new_ids.append(uid)

    if not new_rows:
        print("No new rows to email (all already sent previously).")
        # ensure file exists
        save_sent_ids(SENT_IDS_FILE, sorted(list(sent_ids)))
        return 0

    subj = f"[RSS FEEDS] {len(new_rows)} new item(s)"
    plain_lines = []
    for r in new_rows:
        plain_lines.append(f"- {r.get('title')} ({r.get('site')})\n  {r.get('link (source)')}\n  match: {r.get('match')}\n")
    plain = "\n".join(plain_lines)
    html = "<html><body>"
    html += f"<p>{len(new_rows)} new item(s) detected:</p>"
    html += rows_to_html_table(new_rows)
    html += "</body></html>"

    # send email
    sent_ok = False
    if EMAIL_READY:
        sent_ok = send_email(subj, plain, html)
        if not sent_ok:
            print("Failed to send email (see logs above).")
            # Do NOT mark as sent if email failed -> so it can be retried
            return 0
    else:
        print("EMAIL_READY is False -> skipping actual send. (No SMTP creds configured)")

    # if sent_ok, add new ids to sent_ids and save
    if sent_ok:
        all_ids = sorted(list(sent_ids.union(set(new_ids))))
        try:
            save_sent_ids(SENT_IDS_FILE, all_ids)
        except Exception as e:
            print("Error saving sent ids file:", e)
            return 0

    return 0

if __name__ == "__main__":
    sys.exit(main())
