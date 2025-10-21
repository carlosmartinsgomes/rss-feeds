#!/usr/bin/env python3
# .github/scripts/compare_and_email.py
# Lê feeds_summary.xlsx, compara com .github/data/sent_ids.json e envia email com novas rows.
# Requisitos: pandas, openpyxl

import os
import json
import hashlib
import smtplib
import sys
from email.message import EmailMessage
from typing import List
import pandas as pd

# Config via env (coloca nos Secrets do GitHub)
SMTP_HOST = os.getenv("SMTP_HOST")
SMTP_PORT = int(os.getenv("SMTP_PORT") or 0)
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASS = os.getenv("SMTP_PASS")
EMAIL_FROM = os.getenv("EMAIL_FROM")           # ex: "RSS Bot <bot@example.com>"
EMAIL_TO = os.getenv("EMAIL_TO")               # ex: "carlosmartins.gomes@hotmail.com"
USE_SSL = os.getenv("SMTP_USE_SSL", "true").lower() in ("1","true","yes")

# paths
FEEDS_XLSX = os.getenv("FEEDS_XLSX", "feeds_summary.xlsx")
SENT_IDS_FILE = os.getenv("SENT_IDS_FILE", ".github/data/sent_ids.json")

def make_id(link: str, title: str, pubdate: str) -> str:
    key = ( (link or "") + "||" + (title or "") + "||" + (pubdate or "") ).encode("utf-8")
    return hashlib.sha1(key).hexdigest()

def load_sent_ids(path: str) -> set:
    if not os.path.exists(path):
        return set()
    try:
        with open(path, "r", encoding="utf-8") as f:
            arr = json.load(f)
            if isinstance(arr, list):
                return set(arr)
    except Exception:
        pass
    return set()

def save_sent_ids(path: str, ids: List[str]) -> None:
    ddir = os.path.dirname(path)
    if ddir and not os.path.exists(ddir):
        os.makedirs(ddir, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(list(ids), f, indent=2, ensure_ascii=False)

def send_email(subject: str, plain: str, html: str) -> None:
    if not SMTP_HOST or not SMTP_PORT or not EMAIL_FROM or not EMAIL_TO:
        print("Email not sent: missing SMTP_HOST/SMTP_PORT/EMAIL_FROM/EMAIL_TO env vars")
        return

    msg = EmailMessage()
    msg["From"] = EMAIL_FROM
    msg["To"] = EMAIL_TO
    msg["Subject"] = subject
    msg.set_content(plain)
    if html:
        msg.add_alternative(html, subtype="html")

    print("Sending email to", EMAIL_TO, "via", SMTP_HOST, SMTP_PORT, "ssl:", USE_SSL)
    if USE_SSL:
        with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT) as s:
            if SMTP_USER and SMTP_PASS:
                s.login(SMTP_USER, SMTP_PASS)
            s.send_message(msg)
    else:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as s:
            s.ehlo()
            try:
                s.starttls()
            except Exception:
                pass
            if SMTP_USER and SMTP_PASS:
                s.login(SMTP_USER, SMTP_PASS)
            s.send_message(msg)

def rows_to_html_table(rows) -> str:
    html = "<table border='1' cellpadding='6' cellspacing='0'>"
    html += "<tr><th>site</th><th>title</th><th>pubDate</th><th>link</th><th>match</th></tr>"
    for r in rows:
        html += "<tr>"
        html += "<td>{}</td>".format((r.get("site") or "")[:100])
        html += "<td>{}</td>".format((r.get("title") or "")[:400])
        html += "<td>{}</td>".format((r.get("pubDate") or "")[:60])
        html += "<td><a href='{0}'>{0}</a></td>".format((r.get("link (source)") or r.get("link") or ""))
        html += "<td>{}</td>".format((r.get("match") or "")[:300])
        html += "</tr>"
    html += "</table>"
    return html

def read_feed_summary(path: str):
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
        # still ensure file exists
        save_sent_ids(SENT_IDS_FILE, sorted(list(all_ids)))
        return 0

    # Compose email
    subj = f"[RSS FEEDS] {len(new_rows)} new item(s)"
    plain_lines = []
    for r in new_rows:
        plain_lines.append(f"- {r.get('title')} ({r.get('site')})\n  {r.get('link (source)')}\n  match: {r.get('match')}\n")
    plain = "\n".join(plain_lines)
    html = "<html><body>"
    html += f"<p>{len(new_rows)} new item(s) detected:</p>"
    html += rows_to_html_table(new_rows)
    html += "</body></html>"

    try:
        send_email(subj, plain, html)
        print("Email sent successfully.")
    except Exception as e:
        print("Error sending email:", e)
        return 2

    # update sent ids
    try:
        save_sent_ids(SENT_IDS_FILE, sorted(list(all_ids)))
    except Exception as e:
        print("Error saving sent ids file:", e)
        return 3

    return 0

if __name__ == "__main__":
    sys.exit(main())
