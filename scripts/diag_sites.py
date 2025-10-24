# scripts/diag_sites.py
# Diagnóstico simples: lê scripts/sites.json (se existir) e reporta render_file declarado e se o ficheiro existe.
# Executa: python3 scripts/diag_sites.py

import json
import os
import sys

cfg_path = "scripts/sites.json"

def main():
    print("Diag: looking for", cfg_path)
    if not os.path.exists(cfg_path):
        print("scripts/sites.json NOT FOUND")
        return 0

    try:
        with open(cfg_path, encoding="utf-8") as fh:
            cfg = json.load(fh)
    except Exception as e:
        print("Error reading sites.json:", repr(e))
        return 2

    if not isinstance(cfg, list):
        print("sites.json parsed but top-level is not a list (found type {})".format(type(cfg).__name__))
        return 3

    for s in cfg:
        name = s.get("name", "<noname>")
        rf = s.get("render_file")
        exists = False
        size = 0
        if rf:
            # check both raw path and scripts/<path>
            if os.path.exists(rf):
                exists = True
                size = os.path.getsize(rf)
                found_at = rf
            elif os.path.exists(os.path.join("scripts", rf)):
                exists = True
                size = os.path.getsize(os.path.join("scripts", rf))
                found_at = os.path.join("scripts", rf)
            else:
                found_at = None
        else:
            found_at = None

        print(f"{name}: render_file={rf!r} exists={exists} size={size} found_at={found_at!r}")

    return 0

if __name__ == "__main__":
    sys.exit(main())
