# salvar como check_ads_txt.py
# Requisitos: python3, pip install requests
import requests
import csv
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse

DOMAINS_FILE = "domains.txt"   # um domínio/URL por linha
OUTPUT_CSV = "ads_txt_pubmatic.csv"
TIMEOUT = 8
WORKERS = 20

def normalize_host(entry: str) -> str:
    """
    Recebe uma linha do domains.txt e devolve apenas o host (ex: 'www.cmg.com').
    Aceita formas como:
      - example.com
      - www.example.com/path
      - https://www.example.com/
      - http://example.com/some/page
    """
    entry = entry.strip()
    if not entry:
        return ""
    # Se houver scheme (http/https) usamos urlparse normalmente
    if "://" in entry:
        p = urlparse(entry)
    else:
        # prefix '//' permite ao urlparse colocar o host em netloc
        p = urlparse("//" + entry)
    host = p.netloc or p.path
    # remover portas e barras finais
    host = host.split('/')[0].strip()
    return host

def check_domain(entry):
    """entry é a linha original do domains.txt (pode ter https://...)."""
    host = normalize_host(entry)
    if not host:
        return entry, False, "invalid", "empty host"

    tried_urls = []
    # Tentamos primeiro HTTPS, depois HTTP como fallback
    candidates = [f"https://{host}/ads.txt", f"http://{host}/ads.txt"]

    for url in candidates:
        tried_urls.append(url)
        try:
            r = requests.get(url, timeout=TIMEOUT)
            # se obtivermos 200, analisamos o conteúdo
            if r.status_code == 200:
                body = r.text.lower()
                has_pubmatic = "pubmatic" in body
                snippet = body[:2000].replace("\n", " ")
                return host, has_pubmatic, 200, snippet
            else:
                # se não for 200 continua para o próximo candidato (http fallback)
                # mas regista o status caso ambos falhem
                last_status = r.status_code
        except Exception as e:
            # guardar a mensagem do erro e tentar o próximo candidato
            last_error = str(e)
            # continuação para o next candidate
            continue

    # se chegou aqui, nenhum candidato retornou 200
    # prioriza devolver código de status numérico se existe, senão a string de erro
    status = locals().get("last_status", locals().get("last_error", "err"))
    err_details = f"tried: {', '.join(tried_urls)}; last: {status}"
    return host, False, status, err_details

def main():
    with open(DOMAINS_FILE, encoding="utf-8") as f:
        entries = [line.strip() for line in f if line.strip()]

    results = []
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        for host, has, status, snippet in ex.map(check_domain, entries):
            results.append((host, has, status, snippet))
            print(host, "=>", has, status)

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as out:
        w = csv.writer(out)
        w.writerow(["domain","has_pubmatic","status","snippet"])
        w.writerows(results)

    print("Wrote", OUTPUT_CSV)

if __name__ == "__main__":
    main()
