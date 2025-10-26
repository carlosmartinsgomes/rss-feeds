# scripts/compare_and_email.py
# Robust compare & email: UIDs = sha1(normalized title)
# - normaliza unicode, remove diacríticos, pontuação e espaços extras
# - converte formatos antigos de sent_ids (ex: "title:...") para o novo sha1
# - apresenta logs detalhados para diagnóstico
# - anexa feeds_summary.xlsx se existir
# - inclui 'description'/desc_preview na tabela HTML se a coluna existir

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
    # Normalize unicode (NFKD), remove diacritics, control chars, zero-width, etc.
    t = str(t)
    t = t.strip()
    t = unicodedata.normalize('NFKD', t)
    # remove combining marks (diacritics)
    t = ''.join(ch for ch in t if unicodedata.category(ch) != 'Mn')
    # remove zero-width & other invisible
    t = re.sub(r'[\u200B-\u200F\uFEFF]', ' ', t)
    # lower
    t = t.lower()
    # replace non-word (keep letters/numbers/space)
    t = re.sub(r'[^\w\s]', ' ', t, flags=re.UNICODE)
    # collapse whitespace
    t = re.sub(r'\s+', ' ', t).strip()
    return t

def make_uid_from_title(title):
    norm = normalize_title_for_uid(title)
    if norm == '':
        return ''
    h = hashlib.sha1(norm.encode('utf-8')).hexdigest()  # 40 hex chars
    return h

# ---------- read/write sent ids (canonicalized as sha1 hex strings) ----------
def load_sent_ids(path):
    try:
        if not os.path.exists(path):
            print("DEBUG: sent_ids file not found at", path)
            return set()
        with open(path,'r',encoding='utf-8') as fh:
            data = json.load(fh)
        # data could be list of strings, possibly old format like "title:..."
        out = set()
        for e in data:
            if not e: 
                continue
            if isinstance(e, str):
                s = e.strip()
                # if looks like hex sha1 (40 hex chars)
                if re.fullmatch(r'[0-9a-fA-F]{40}', s):
                    out.add(s.lower())
                elif s.startswith("title:"):
                    # old format: title:... -> convert to sha1 of normalized part
                    old_title = s[len("title:"):].strip()
                    uid = make_uid_from_title(old_title)
                    if uid:
                        out.add(uid)
                else:
                    # fallback: treat as raw title and hash it
                    uid = make_uid_from_title(s)
                    if uid:
                        out.add(uid)
            else:
                # ignore non-strings
                continue
        print("DEBUG: loaded sent_ids count =", len(out))
        return out
    except Exception as e:
        print("Error loading sent ids:", e)
        return set()

def save_sent_ids(path, ids_set):
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        # save sorted list for determinism
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
        # try multiple possible description column names
        desc = ""
        for cname in ("description", "desc", "desc_preview", "summary", "summary_tail"):
            if cname in r and not pd.isna(r.get(cname)):
                desc = str(r.get(cname))
                break
        rows.append({
            "site": str(r.get("site") or ""),
            "title": str(r.get("title") or ""),
            "pubDate": str(r.get("pubDate") or ""),
            "link (source)": str(r.get("link (source)") or r.get("link") or ""),
            "match": str(r.get("match") or ""),
            "description": desc
        })
    return rows

def rows_to_html_table(rows):
    html = "<table border='1' cellpadding='6' cellspacing='0'>"
    html += "<tr><th>site</th><th>title</th><th>pubDate</th><th>link</th><th>match</th><th>description</th></tr>"
    for r in rows:
        html += "<tr>"
        html += "<td>{}</td>".format((r.get("site") or "")[:100])
        html += "<td>{}</td>".format((r.get("title") or "")[:400])
        html += "<td>{}</td>".format((r.get("pubDate") or "")[:60])
        html += "<td>{}</td>".format((r.get("link (source)") or r.get("link") or "")[:300])
        html += "<td>{}</td>".format((r.get("match") or "")[:300])
        html += "<td>{}</td>".format((r.get("description") or "")[:500])
        html += "</tr>"
    html += "</table>"
    return html

# ---------- send email ----------
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

# ---------- main ----------
def main():
    rows = read_feed_summary(FEEDS_XLSX)
    print("DEBUG: all_rows length =", len(rows))
    if not rows:
        print("No rows found in feed summary -> nothing to send")
        return 0

    sent_set = load_sent_ids(SENT_IDS_FILE)

    new_rows = []
    added_uids = []
    for r in rows:
        title = (r.get("title") or "").strip()
        if not title:
            continue
        uid = make_uid_from_title(title)
        if not uid:
            continue
        if uid not in sent_set:
            new_rows.append(r)
            added_uids.append(uid)
            sent_set.add(uid)

    print("DEBUG: new_rows count =", len(new_rows))
    if len(new_rows) > 0:
        print("DEBUG: new UIDs (first 20):", added_uids[:20])

    if not new_rows:
        print("No new rows to email (all already sent previously).")
        # still save sent ids to persist possible conversions
        save_sent_ids(SENT_IDS_FILE, sent_set)
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

    sent_ok = False
    if os.environ.get('EMAIL_READY') == '1':
        sent_ok = send_email(subj, plain, attach_path=FEEDS_XLSX, html=html)
        if not sent_ok:
            print("Failed to send email (see logs above).")
            return 2
    else:
        print("EMAIL_READY != 1 -> skipping actual send (but will save sent ids).")

    # update sent ids file
    try:
        save_sent_ids(SENT_IDS_FILE, sent_set)
    except Exception as e:
        print("Error saving sent ids file:", e)
        return 3

    return 0

if __name__ == "__main__":
    sys.exit(main())
