# scripts/compare_and_email.py
# Versão com diagnóstico explícito:
# - UID = sha1(normalized title)
# - carrega/normaliza formatos antigos de sent_ids
# - preserva a ordem/nomes das colunas do Excel no email HTML
# - imprime DIAG_PER_ROW para cada linha nova (útil para debugging)
# - salva .github/data/sent_ids.json no final

import os, sys, json, re, hashlib, unicodedata
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

# ---------- normalization and uid helpers ----------
def normalize_title_for_uid(t):
    if not t:
        return ''
    t = str(t).strip()
    # Unicode normalize and remove diacritics
    t = unicodedata.normalize('NFKD', t)
    t = ''.join(ch for ch in t if unicodedata.category(ch) != 'Mn')
    # remove invisible characters
    t = re.sub(r'[\u200B-\u200F\uFEFF]', ' ', t)
    t = t.lower()
    # remove punctuation, keep letters/numbers/space
    t = re.sub(r'[^\w\s]', ' ', t, flags=re.UNICODE)
    t = re.sub(r'\s+', ' ', t).strip()
    return t

def make_uid_from_title(title):
    norm = normalize_title_for_uid(title)
    if norm == '':
        return ''
    return hashlib.sha1(norm.encode('utf-8')).hexdigest()

# ---------- sent ids read/write ----------
def load_sent_ids(path):
    try:
        if not os.path.exists(path):
            print("DEBUG: sent_ids file not found at", path)
            return set()
        with open(path,'r',encoding='utf-8') as fh:
            data = json.load(fh)
        out = set()
        for e in data:
            if not e:
                continue
            if isinstance(e, str):
                s = e.strip()
                if re.fullmatch(r'[0-9a-fA-F]{40}', s):
                    out.add(s.lower())
                elif s.startswith("title:"):
                    old_title = s[len("title:"):].strip()
                    uid = make_uid_from_title(old_title)
                    if uid:
                        out.add(uid)
                else:
                    uid = make_uid_from_title(s)
                    if uid:
                        out.add(uid)
        print("DEBUG: loaded sent_ids count =", len(out))
        return out
    except Exception as e:
        print("Error loading sent ids:", e)
        return set()

def save_sent_ids(path, ids_set):
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        lst = sorted(list(ids_set))
        with open(path,'w',encoding='utf-8') as fh:
            json.dump(lst, fh, indent=2, ensure_ascii=False)
        print("Saved sent ids to", path)
        print("Saved sent_ids count:", len(lst))
        if len(lst) > 20:
            print("Saved sent_ids sample:", lst[:20])
        else:
            print("Saved sent_ids:", lst)
    except Exception as e:
        print("Error saving sent ids:", e)
        raise

# ---------- read Excel preserving columns ----------
def read_feed_summary(path):
    try:
        import pandas as pd
    except Exception as e:
        print("Error: pandas not installed:", e)
        return [], []
    if not os.path.exists(path):
        print("feeds_summary.xlsx not found at", path)
        return [], []
    try:
        df = pd.read_excel(path, engine="openpyxl")
    except Exception as e:
        print("Error reading Excel:", e)
        return [], []
    cols = list(df.columns)
    rows = []
    for _, r in df.iterrows():
        row = {}
        for c in cols:
            val = r.get(c)
            row[c] = "" if (val is None or (hasattr(val, 'isna') and val.isna())) else str(val)
        rows.append(row)
    return rows, cols

def rows_to_html_table_dynamic(rows, cols):
    html = "<table border='1' cellpadding='6' cellspacing='0'>"
    # header in original order
    html += "<tr>"
    for c in cols:
        html += f"<th>{c}</th>"
    html += "</tr>"
    for r in rows:
        html += "<tr>"
        for c in cols:
            cell = (r.get(c) or "")[:800]
            # escape minor HTML characters
            cell = cell.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            html += f"<td>{cell}</td>"
        html += "</tr>"
    html += "</table>"
    return html

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

def main():
    rows, cols = read_feed_summary(FEEDS_XLSX)
    print("DEBUG: all_rows length =", len(rows))
    if not rows:
        print("No rows found in feed summary -> nothing to send")
        return 0

    sent_set = load_sent_ids(SENT_IDS_FILE)

    new_rows = []
    added_uids = []
    # We'll also print DIAG_PER_ROW for every row we will send
    for r in rows:
        # Try to determine a title column: common names: 'title', 'Title'
        title = ""
        for cand in ("title","Title","headline","Headline"):
            if cand in r and r[cand]:
                title = r[cand]
                break
        # fallback: try to find first column that looks like a title
        if not title:
            # choose the column named 'title' if present, else first non-empty string column
            for c in cols:
                if r.get(c):
                    title = r.get(c)
                    break
        title = (title or "").strip()
        uid = make_uid_from_title(title)
        existed = (uid in sent_set) if uid else False
        # DIAGNOSTIC LINE (very important)
        print(f"DIAG_PER_ROW: title={title[:200]!r} uid={uid!r} existed_before={existed}")
        if uid and not existed:
            new_rows.append(r)
            added_uids.append(uid)
            sent_set.add(uid)

    print("DEBUG: new_rows count =", len(new_rows))
    if len(new_rows) > 0:
        print("DEBUG: new UIDs (first 50):", added_uids[:50])

    if not new_rows:
        print("No new rows to email (all already sent previously).")
        save_sent_ids(SENT_IDS_FILE, sent_set)
        return 0

    subj = f"[RSS FEEDS] {len(new_rows)} new item(s)"
    plain_lines = []
    for r in new_rows:
        # pick columns link if present
        link = r.get("link (source)") or r.get("link") or r.get("Link") or ""
        site = r.get("site") or ""
        title = r.get("title") or ""
        match = r.get("match") or ""
        desc = r.get("description") or r.get("desc") or ""
        plain_lines.append(f"- {title} ({site})\n  {link}\n  match: {match}\n  desc: {desc}\n")
    plain = "\n".join(plain_lines)
    html = "<html><body>"
    html += f"<p>{len(new_rows)} new item(s) detected:</p>"
    html += rows_to_html_table_dynamic(new_rows, cols)
    html += "</body></html>"

    sent_ok = False
    if os.environ.get('EMAIL_READY') == '1':
        sent_ok = send_email(subj, plain, attach_path=FEEDS_XLSX, html=html)
        if not sent_ok:
            print("Failed to send email (see logs above).")
            return 2
    else:
        print("EMAIL_READY != 1 -> skipping actual send (but will save sent ids).")

    try:
        save_sent_ids(SENT_IDS_FILE, sent_set)
    except Exception as e:
        print("Error saving sent ids file:", e)
        return 3

    return 0

if __name__ == "__main__":
    sys.exit(main())
