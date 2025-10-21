# --- início do bloco de diagnóstico / leitura robusta de env vars ---
import os, sys
from email.message import EmailMessage
import smtplib

# Função utilitária: ler env com vários nomes possíveis
def getenv_first(*names, default=''):
    for n in names:
        v = os.environ.get(n)
        if v is not None and str(v).strip() != '':
            return v
    return default

# Lê variantes (compatibilidade)
SMTP_HOST = getenv_first('SMTP_HOST', 'SMTP_SERVER', '')
SMTP_PORT = getenv_first('SMTP_PORT', '')
SMTP_USER = getenv_first('SMTP_USER', 'SMTP_USERNAME', '')
SMTP_PASS = getenv_first('SMTP_PASS', 'SMTP_PASSWORD', '')
EMAIL_FROM = getenv_first('EMAIL_FROM', '')
EMAIL_TO = getenv_first('EMAIL_TO', '')
SMTP_USE_SSL = getenv_first('SMTP_USE_SSL', '').lower() in ('1','true','yes','on')

# Variáveis de ficheiro (padrões)
FEEDS_XLSX = os.environ.get('FEEDS_XLSX', 'feeds_summary.xlsx')
SENT_IDS_FILE = os.environ.get('SENT_IDS_FILE', '.github/data/sent_ids.json')

# Debug seguro: diz se var está definida sem mostrar valor
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

# Verificação mínima: se faltar o essencial, não tentamos enviar e saímos limpo
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
    # aqui o script pode continuar a executar a lógica de comparação sem enviar email
    # para evitar comportamento inesperado, definimos uma flag que o resto do script pode verificar:
    os.environ['EMAIL_READY'] = '0'
else:
    os.environ['EMAIL_READY'] = '1'

# função simples de envio (o resto do script pode chamar)
def send_email(subject, body_text, attach_path=None):
    # só envia se EMAIL_READY == '1'
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
            # tenta STARTTLS se porta 587
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
        msg.set_content(body_text)

        if attach_path and os.path.exists(attach_path):
            with open(attach_path, 'rb') as f:
                data = f.read()
            msg.add_attachment(data, maintype='application', subtype='octet-stream', filename=os.path.basename(attach_path))

        server.send_message(msg)
        server.quit()
        print("Email sent successfully.")
        return True
    except Exception as e:
        print("Email sending failed:", str(e))
        return False

# --- fim do bloco diagnóstico ---


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
