# scripts/diag_sites.py
# Diagnóstico robusto para scripts/sites.json
# - aceita top-level list ou dict
# - tenta "adivinhar" mapeamentos name->siteobj
# - reporta render_file declarado, se o ficheiro existe, e procura variantes em scripts/ e scripts/rendered/
# - imprime um preview dos primeiros sites
#
# Executa: python3 scripts/diag_sites.py

import json
import os
import sys
import pprint

cfg_path = "scripts/sites.json"

def safe_load(path):
    try:
        with open(path, encoding="utf-8") as fh:
            return json.load(fh)
    except Exception as e:
        print("Error reading sites.json:", repr(e))
        return None

def normalize_cfg(cfg):
    """
    Retorna uma lista de site-objects (dicts) independentemente
    do formato original (list ou dict).
    Se for dict, tenta:
      - usar cfg['sites'] se existir e for lista
      - interpretar dict como mapping name->siteobj
    """
    sites = []
    if isinstance(cfg, list):
        sites = cfg
    elif isinstance(cfg, dict):
        # caso comum: {"sites": [ ... ]}
        if "sites" in cfg and isinstance(cfg["sites"], list):
            sites = cfg["sites"]
        else:
            # interpretar como mapping name->siteobj
            for k, v in cfg.items():
                if isinstance(v, dict):
                    obj = v.copy()
                    if "name" not in obj:
                        obj["name"] = k
                    sites.append(obj)
                else:
                    # valor não-dict -> colocar aviso mas ignorar
                    print(f"NOTE: key {k!r} has non-dict value of type {type(v).__name__} - skipping")
    else:
        # tipo inesperado
        print("Unexpected top-level type:", type(cfg).__name__)
    return sites

def find_render_file(rf):
    """Procura render_file nos caminhos comuns e devolve (found_path or None, size)"""
    if not rf:
        return None, 0
    candidates = [
        rf,
        os.path.join("scripts", rf),
        os.path.join("scripts", "rendered", rf),
    ]
    for p in candidates:
        if os.path.exists(p):
            try:
                return p, os.path.getsize(p)
            except Exception:
                return p, 0
    return None, 0

def find_alternate_render(name):
    """Procura scripts/rendered/<name>.html e variantes simples"""
    if not name:
        return None
    candidates = [
        os.path.join("scripts", "rendered", f"{name}.html"),
        os.path.join("scripts", "rendered", f"{name.replace(' ', '-').lower()}.html"),
        os.path.join("scripts", "rendered", f"{name.replace(' ', '_').lower()}.html"),
    ]
    for p in candidates:
        if os.path.exists(p):
            return p
    return None

def main():
    print("Diag: looking for", cfg_path)
    if not os.path.exists(cfg_path):
        print("scripts/sites.json NOT FOUND")
        # não falhar o workflow — apenas aviso
        return 0

    cfg = safe_load(cfg_path)
    if cfg is None:
        print("Failed to parse sites.json -> aborting diag (no crash).")
        return 0

    top_type = type(cfg).__name__
    print("Top-level type:", top_type)

    sites = normalize_cfg(cfg)
    print("Interpreted sites count:", len(sites))

    if isinstance(cfg, dict):
        # mostrar as primeiras keys do dict para ajudar o diagnóstico
        print("Top-level dict keys (first 30):", list(cfg.keys())[:30])

    # imprimir sumário por site
    for i, s in enumerate(sites):
        if not isinstance(s, dict):
            print(f"Site[{i}] is not a dict (type {type(s).__name__}) -> SKIP")
            continue
        name = s.get("name") or s.get("site") or s.get("title") or f"<site_{i}>"
        rf = s.get("render_file")
        selectors_preview = None
        # extrair alguns selectors habituais para preview
        for k in ("item_container", "title", "link", "date", "topic"):
            if k in s:
                selectors_preview = s.get(k)
                break
        found_path, size = find_render_file(rf)
        alt_render = find_alternate_render(name)
        print("-" * 80)
        print(f"[{i}] name: {name!r}")
        print(f"     render_file (declared): {rf!r}")
        print(f"     render_file found at: {found_path!r}  size={size}")
        print(f"     alt_render (scripts/rendered/...): {alt_render!r}")
        print(f"     selectors preview: {str(selectors_preview)[:140]!r}")
        # opcional: se existirem keys uteis, mostrar um pequeno preview
        useful_keys = {k: s.get(k) for k in ("item_container", "title", "link", "date", "topic", "filters")}
        print("     useful keys:", {k:v for k,v in useful_keys.items() if v})
    print("-" * 80)
    print("First 3 site objects (pretty):")
    pprint.pprint(sites[:3], width=120)
    print("Diag finished.")
    # terminar com sucesso para não abortar workflow
    return 0

if __name__ == "__main__":
    sys.exit(main())
