# .github/scripts/compare_and_email.py
import os
import sys
import pandas as pd
from email.message import EmailMessage
import smtplib
import ssl
import traceback

"""
Env expected:
 EMAIL_TO (single address)
 SMTP_SERVER
 SMTP_PORT
 SMTP_USERNAME
 SMTP_PASSWORD
 EMAIL_FROM
"""

CUR_FILE = "feeds_summary.xlsx"
PREV_FILE = "prev_feeds_summary.xlsx"

def load_df(path):
    try:
        df = pd.read_excel(path, engine="openpyxl")
        return df
    except Exception as e:
        print("Failed to read", path, ":", e)
        return None

def make_key(row):
    # prefer link (source), fallback to title+pubDate
    link = (row.get('link (source)') or row.get('link') or "")
    link = str(link).strip()
    if link:
        return "L:" + link
    title = str(row.get('title') or "").strip()
    pub = str(row.get('pubDate') or row.get('date') or "").strip()
    return "T:" + title + "||" + pub

def build_email_body(new_rows):
    # HTML body with simple table
    rows_html = ""
    for _, r in new_rows.iterrows():
        title = str(r.get('title') or '')[:400]
        link = str(r.get('link (source)') or r.get('link') or '')
        site = str(r.get('site') or '')
        pub = str(r.get('pubDate') or r.get('date') or '')
        desc = str(r.get('description (short)') or '')[:800]
        rows_html += f"<tr><td><a href='{link}'>{title}</a></td><td>{site}</td><td>{pub}</td><td>{desc}</td></tr>\n"
    html = f"""<html><body>
<h2>Novas notícias detectadas: {len(new_rows)}</h2>
<table border="1" cellpadding="4" cellspacing="0">
<tr><th>Title</th><th>Site</th><th>PubDate</th><th>Summary</th></tr>
{rows_html}
</table>
</body></html>
"""
    return html

def send_email(subject, html_body, to_addr):
    smtp_server = os.environ.get("SMTP_SERVER")
    smtp_port = int(os.environ.get("SMTP_PORT") or 587)
    smtp_user = os.environ.get("SMTP_USERNAME")
    smtp_pass = os.environ.get("SMTP_PASSWORD")
    from_addr = os.environ.get("EMAIL_FROM") or smtp_user
    if not smtp_server or not smtp_user or not smtp_pass:
        print("SMTP config missing. Skipping email send.")
        return False, "missing-smtp-config"

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = from_addr
    msg["To"] = to_addr
    msg.set_content("Novas notícias no feed (ver HTML).")
    msg.add_alternative(html_body, subtype="html")

    try:
        # use SSL for 465, otherwise STARTTLS
        if smtp_port == 465:
            context = ssl.create_default_context()
            with smtplib.SMTP_SSL(smtp_server, smtp_port, context=context) as s:
                s.login(smtp_user, smtp_pass)
                s.send_message(msg)
        else:
            with smtplib.SMTP(smtp_server, smtp_port, timeout=60) as s:
                s.ehlo()
                try:
                    s.starttls()
                except Exception:
                    pass
                s.login(smtp_user, smtp_pass)
                s.send_message(msg)
        print("Email sent to", to_addr)
        return True, None
    except Exception as e:
        print("Failed to send email:", e)
        traceback.print_exc()
        return False, str(e)

def main():
    to_addr = os.environ.get("EMAIL_TO") or os.environ.get("EMAIL_RECIPIENT") or "carlosmartins.gomes@hotmail.com"
    df_cur = load_df(CUR_FILE)
    if df_cur is None:
        print("Current file not found or invalid:", CUR_FILE)
        return 0

    df_prev = None
    if os.path.exists(PREV_FILE):
        df_prev = load_df(PREV_FILE)

    # ensure columns exist
    for col in ["site", "title", "link (source)", "pubDate", "description (short)"]:
        if col not in df_cur.columns:
            df_cur[col] = ""

    df_cur['_key'] = df_cur.apply(make_key, axis=1)
    prev_keys = set()
    if df_prev is not None:
        # guard against different column names
        if "link (source)" not in df_prev.columns and "link" in df_prev.columns:
            df_prev["link (source)"] = df_prev["link"]
        df_prev['_key'] = df_prev.apply(make_key, axis=1)
        prev_keys = set(df_prev['_key'].astype(str).tolist())

    # determine new rows
    new_mask = ~df_cur['_key'].isin(prev_keys)
    new_rows = df_cur[new_mask].copy()

    if new_rows.empty:
        print("No new items to notify.")
        return 0

    # Build email
    subj = f"[Feeds] {len(new_rows)} novas notícias"
    html = build_email_body(new_rows)

    ok, err = send_email(subj, html, to_addr)
    if not ok:
        print("Email failed:", err)
        return 1

    print(f"Notified {len(new_rows)} new items to {to_addr}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
