#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Appelle POST /api/refresh-indicateurs pour chaque code_insee d'un CSV (1 par ligne).

Exemples:
  py webapp-foncier/scripts/refresh_indicateurs_from_csv.py --csv "C:/tmp/codes.csv"
  py webapp-foncier/scripts/refresh_indicateurs_from_csv.py --csv "C:/tmp/codes.csv" --base-url "http://127.0.0.1:8000"
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path


def _read_codes_from_csv(csv_path: Path) -> list[str]:
    """Lit un CSV simple: 1 code_insee par ligne (tolère ; et ,)."""
    raw = csv_path.read_text(encoding="utf-8", errors="replace")
    lines = [ln.strip() for ln in raw.splitlines() if ln.strip()]
    if not lines:
        return []

    # Tolère un éventuel BOM + en-tête "code_insee".
    first = lines[0].lstrip("\ufeff").strip().lower()
    if first in {"code_insee", "insee", "code"}:
        lines = lines[1:]

    codes: list[str] = []
    for line in lines:
        # Garde le 1er champ uniquement, compatible "13026;..." ou "13026,..."
        if ";" in line:
            token = line.split(";", 1)[0].strip()
        elif "," in line:
            token = line.split(",", 1)[0].strip()
        else:
            token = line.strip()
        if token:
            codes.append(token)
    return codes


def _call_refresh_one(base_url: str, code_insee: str, timeout: float) -> dict:
    params = urllib.parse.urlencode({"code_insee_list": code_insee})
    url = f"{base_url.rstrip('/')}/api/refresh-indicateurs?{params}"
    req = urllib.request.Request(url, method="POST", headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        payload = resp.read().decode("utf-8", errors="replace")
    data = json.loads(payload) if payload else {}
    return data if isinstance(data, dict) else {"raw": data}


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Refresh indicateurs commune par commune depuis un CSV."
    )
    parser.add_argument("--csv", required=True, help="Chemin du CSV (1 code_insee par ligne).")
    parser.add_argument(
        "--base-url",
        default="http://127.0.0.1:8000",
        help="Base URL de l'API backend (sans slash final).",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=60.0,
        help="Timeout HTTP (secondes) par appel.",
    )
    parser.add_argument(
        "--sleep-ms",
        type=int,
        default=0,
        help="Pause (ms) entre deux appels (optionnel).",
    )
    args = parser.parse_args()

    csv_path = Path(args.csv).expanduser().resolve()
    if not csv_path.exists():
        print(f"Fichier introuvable: {csv_path}", file=sys.stderr)
        return 2

    codes = _read_codes_from_csv(csv_path)
    if not codes:
        print("Aucun code_insee trouvé dans le CSV.", file=sys.stderr)
        return 2

    total = len(codes)
    ok_count = 0
    err_count = 0
    refreshed_sum = 0
    requested_sum = 0
    failed_codes: list[str] = []

    print(f"Traitement de {total} code_insee depuis: {csv_path}")
    for idx, code in enumerate(codes, start=1):
        try:
            result = _call_refresh_one(args.base_url, code, args.timeout)
            refreshed = int(result.get("refreshed") or 0)
            requested = int(result.get("requested") or 0)
            refreshed_sum += refreshed
            requested_sum += requested

            if refreshed >= 1:
                ok_count += 1
                status = "OK"
            else:
                err_count += 1
                failed_codes.append(code)
                status = "KO"
            print(f"[{idx}/{total}] {code} -> {status} (refreshed={refreshed}, requested={requested})")
        except urllib.error.HTTPError as e:
            err_count += 1
            failed_codes.append(code)
            body = e.read().decode("utf-8", errors="replace") if e.fp else ""
            print(f"[{idx}/{total}] {code} -> KO HTTP {e.code} {body[:300]}", file=sys.stderr)
        except urllib.error.URLError as e:
            err_count += 1
            failed_codes.append(code)
            print(f"[{idx}/{total}] {code} -> KO URL {e.reason}", file=sys.stderr)
        except Exception as e:
            err_count += 1
            failed_codes.append(code)
            print(f"[{idx}/{total}] {code} -> KO {e!r}", file=sys.stderr)

        if args.sleep_ms > 0:
            time.sleep(args.sleep_ms / 1000.0)

    print("")
    print("=== Résumé ===")
    print(f"codes_total: {total}")
    print(f"codes_ok: {ok_count}")
    print(f"codes_ko: {err_count}")
    print(f"requested_sum: {requested_sum}")
    print(f"refreshed_sum: {refreshed_sum}")
    if failed_codes:
        print(f"codes_en_echec: {','.join(failed_codes)}")

    return 0 if err_count == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())

