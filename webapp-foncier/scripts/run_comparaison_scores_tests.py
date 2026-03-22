#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tests HTTP de l'endpoint GET /api/comparaison_scores (mode communes).

Usage (depuis la racine du repo ou ce dossier) :
  py scripts/run_comparaison_scores_tests.py
  py scripts/run_comparaison_scores_tests.py --base-url http://127.0.0.1:8000

Sortie (par défaut) :
  webapp-foncier/test_output/comparaison_scores_report.json
  webapp-foncier/test_output/comparaison_scores_report.md

L'agent Cursor peut relire ces fichiers sans que vous colliez le JSON dans le chat.

Note : les paramètres type_logt / type_surf / nb_pieces sont acceptés par l'API
mais n'filtrent pas encore la ligne renvoyée (voir docstring de get_comparaison_scores
dans main.py) ; le tri et la « colonne active » passent par score_principal.
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

# Communes par défaut : Cognac (16) + Poitiers (86)
DEFAULT_COMMUNES = [
    ("16", "16100", "Cognac"),
    ("86", "86000", "Poitiers"),
]

# Scénarios alignés sur A.1–A.5 (score_principal = colonne utilisée pour le tri / lecture)
SCENARIOS = [
    (
        "A.1",
        "Données agrégées (3 sélecteurs : TOUS / TOUTES / TOUS) — lecture renta_nette globale",
        {
            "score_principal": "renta_nette",
            "type_logt": "TOUS",
            "type_surf": "TOUTES",
            "nb_pieces": "TOUS",
        },
    ),
    (
        "A.2",
        "Détail type de local (ex. Maisons), autres dimensions agrégées — score_principal=renta_nette_maisons",
        {
            "score_principal": "renta_nette_maisons",
            "type_logt": "Maisons",
            "type_surf": "TOUTES",
            "nb_pieces": "TOUS",
        },
    ),
    (
        "A.3",
        "Détail catégorie de surface (ex. S3), autres agrégés — score_principal=renta_nette_agg_s3",
        {
            "score_principal": "renta_nette_agg_s3",
            "type_logt": "TOUS",
            "type_surf": "S3",
            "nb_pieces": "TOUS",
        },
    ),
    (
        "A.4",
        "Détail nb de pièces (ex. T2), autres agrégés — score_principal=renta_nette_agg_t2",
        {
            "score_principal": "renta_nette_agg_t2",
            "type_logt": "TOUS",
            "type_surf": "TOUTES",
            "nb_pieces": "T2",
        },
    ),
    (
        "A.5",
        "Granularité max côté colonnes API : type × tranche (ex. Appartements × S3) — renta_nette_appts_s3",
        {
            "score_principal": "renta_nette_appts_s3",
            "type_logt": "Appartements",
            "type_surf": "S3",
            "nb_pieces": "TOUS",
        },
    ),
]


def _build_url(base: str, extra_params: dict) -> str:
    params = [
        ("mode", "communes"),
        ("n_max", "20"),
    ]
    for dept, postal, nom in DEFAULT_COMMUNES:
        params.append(("code_dept", dept))
        params.append(("code_postal", postal))
        params.append(("commune", nom))
    for k, v in extra_params.items():
        if v is not None:
            params.append((k, str(v)))
    qs = urllib.parse.urlencode(params, doseq=True)
    return f"{base.rstrip('/')}/api/comparaison_scores?{qs}"


def _fetch_json(url: str, timeout: float = 60.0) -> dict:
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read().decode("utf-8", errors="replace")
    return json.loads(raw)


def _summarize_row(row: dict, score_key: str) -> dict:
    return {
        "commune": row.get("commune"),
        "code_dept": row.get("code_dept"),
        "code_postal": row.get("code_postal"),
        "score_principal": score_key,
        "valeur_tri": row.get(score_key),
        "renta_nette": row.get("renta_nette"),
    }


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    out_dir = root / "test_output"
    out_dir.mkdir(parents=True, exist_ok=True)

    parser = argparse.ArgumentParser(description="Tests API comparaison_scores")
    parser.add_argument(
        "--base-url",
        default="http://127.0.0.1:8000",
        help="URL du backend FastAPI (sans slash final)",
    )
    args = parser.parse_args()
    base = args.base_url

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "base_url": base,
        "communes": DEFAULT_COMMUNES,
        "scenarios": [],
        "errors": [],
    }

    for code, title, extra in SCENARIOS:
        url = _build_url(base, extra)
        sp = extra.get("score_principal", "renta_nette")
        entry = {
            "id": code,
            "title": title,
            "request_url": url,
            "params": extra,
            "http_ok": False,
            "row_count": 0,
            "summary_per_commune": [],
            "raw_rows": None,
            "error": None,
        }
        try:
            data = _fetch_json(url)
            rows = data.get("rows") or []
            entry["http_ok"] = True
            entry["row_count"] = len(rows)
            entry["raw_rows"] = rows
            for r in rows:
                entry["summary_per_commune"].append(_summarize_row(r, sp))
            # Ordre de tri attendu : décroissant sur score_principal (comme le backend)
            vals = [r.get(sp) for r in rows]
            entry["sort_values_for_score_principal"] = vals
        except urllib.error.HTTPError as e:
            err_body = e.read().decode("utf-8", errors="replace") if e.fp else ""
            entry["error"] = f"HTTP {e.code}: {err_body[:500]}"
            report["errors"].append({"scenario": code, "error": entry["error"]})
        except urllib.error.URLError as e:
            entry["error"] = f"URL error: {e.reason}"
            report["errors"].append({"scenario": code, "error": entry["error"]})
        except Exception as e:
            entry["error"] = repr(e)
            report["errors"].append({"scenario": code, "error": entry["error"]})

        report["scenarios"].append(entry)

    json_path = out_dir / "comparaison_scores_report.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    # Markdown court pour lecture humaine
    md_lines = [
        f"# Rapport API comparaison_scores",
        f"",
        f"- Généré : `{report['generated_at']}`",
        f"- Base URL : `{base}`",
        f"- JSON complet : `{json_path.relative_to(root)}`",
        f"",
    ]
    for s in report["scenarios"]:
        md_lines.append(f"## {s['id']} — {s['title']}")
        md_lines.append("")
        md_lines.append(f"- OK HTTP : {s['http_ok']}")
        md_lines.append(f"- Lignes : {s['row_count']}")
        if s.get("error"):
            md_lines.append(f"- Erreur : `{s['error']}`")
        else:
            sp = s["params"].get("score_principal", "renta_nette")
            md_lines.append(f"- score_principal : `{sp}`")
            for sm in s.get("summary_per_commune") or []:
                md_lines.append(
                    f"  - **{sm.get('commune')}** : `{sp}` = {sm.get('valeur_tri')} (renta_nette globale = {sm.get('renta_nette')})"
                )
        md_lines.append("")

    if report["errors"]:
        md_lines.append("## Erreurs")
        md_lines.append("")
        for e in report["errors"]:
            md_lines.append(f"- **{e['scenario']}** : {e['error']}")
        md_lines.append("")

    md_path = out_dir / "comparaison_scores_report.md"
    md_path.write_text("\n".join(md_lines), encoding="utf-8")

    print(f"OK — écrit : {json_path}", file=sys.stderr)
    print(f"OK — écrit : {md_path}", file=sys.stderr)
    return 0 if not report["errors"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
