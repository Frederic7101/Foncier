# -*- coding: utf-8 -*-
"""
Remplit les colonnes G à J du fichier grille « Tests comparaison scores » à partir
d’un rapport JUnit produit par pytest (ex. pytest e2e/ --junitxml=...).

  python scripts/e2e_junit_to_tests_txt.py
  python scripts/e2e_junit_to_tests_txt.py --junit test_output/e2e_junit.xml

Sortie par défaut : « tests comparaison scores 2.txt » (même contenu logique que le .csv).
"""
from __future__ import annotations

import argparse
import ast
import csv
import io
import re
import sys
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path

# Racine webapp-foncier
ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SOURCE = ROOT / "Tests comparaison scores 2.csv"
DEFAULT_JUNIT = ROOT / "test_output" / "e2e_junit.xml"
DEFAULT_OUT = ROOT / "tests comparaison scores 2.txt"
TEST_FILE = ROOT / "e2e" / "test_comparaison_scores_2.py"

COL_G, COL_H, COL_I, COL_J = 6, 7, 8, 9
MIN_COLS = 11
# Limite pratique pour la colonne H (docstring + résultat), lisible dans Excel / grille
MAX_COL_H_LEN = 2000


def _decorator_csv_id(dec: ast.expr) -> str | None:
    """Repère @pytest.mark.csv("…") sur un décorateur ast.Call."""
    if not isinstance(dec, ast.Call):
        return None
    f = dec.func
    if not isinstance(f, ast.Attribute) or f.attr != "csv":
        return None
    inner = f.value
    if not isinstance(inner, ast.Attribute) or inner.attr != "mark":
        return None
    if not isinstance(inner.value, ast.Name) or inner.value.id != "pytest":
        return None
    if not dec.args:
        return None
    arg0 = dec.args[0]
    if isinstance(arg0, ast.Constant) and isinstance(arg0.value, str):
        return arg0.value
    return None


def parse_tests_py_metadata(path: Path) -> tuple[dict[str, str], dict[str, str]]:
    """
    Retourne (fn_to_csv, csv_to_docstring).
    csv_to_docstring : id de pas -> docstring brute de la fonction de test (dernier gagnant si doublon d’id).
    """
    text = path.read_text(encoding="utf-8")
    tree = ast.parse(text)
    fn_to_csv: dict[str, str] = {}
    csv_to_doc: dict[str, str] = {}

    for node in tree.body:
        if not isinstance(node, ast.FunctionDef) or not node.name.startswith("test_"):
            continue
        csv_id = None
        for dec in node.decorator_list:
            cid = _decorator_csv_id(dec)
            if cid is not None:
                csv_id = cid
                break
        if not csv_id:
            continue
        fn_to_csv[node.name] = csv_id
        csv_to_doc[csv_id] = ast.get_docstring(node) or ""

    return fn_to_csv, csv_to_doc


def _docstring_one_line(s: str) -> str:
    return " ".join((s or "").strip().split())


def _combine_col_h(doc: str, statut: str, detail: str, duration_s: float) -> str:
    """
    Colonne H = docstring (une ligne) + séparateur + libellé pytest + détail.
    statut : OK | KO | N/A
    detail : message d’erreur, skip, ou vide pour OK
    """
    intro = _docstring_one_line(doc)
    if statut == "OK":
        tail = f"Pytest passed"
    elif statut == "KO":
        err = (detail or "échec").strip()
        err = " ".join(err.split())
        if len(err) > 400:
            err = err[:397] + "…"
        tail = f"Pytest failed — {err}"
    else:
        sk = (detail or "skipped").strip()
        sk = " ".join(sk.split())
        if len(sk) > 400:
            sk = sk[:397] + "…"
        tail = f"Pytest skipped — {sk}"

    if intro:
        combined = f"{intro}\n———\n{tail}"
    else:
        combined = tail
    if len(combined) > MAX_COL_H_LEN:
        combined = combined[: MAX_COL_H_LEN - 1] + "…"
    return combined


def junit_basename(name: str) -> str:
    """test_2_1_foo[chromium] -> test_2_1_foo"""
    return re.sub(r"\[.+]$", "", name.strip())


def parse_junit(
    path: Path,
    fn_to_csv: dict[str, str],
    csv_to_doc: dict[str, str],
) -> dict[str, tuple[str, str, float]]:
    """
    Retourne csv_id -> (statut_I, message_H, durée_s).
    statut_I : OK | KO | N/A (skip)
    message_H : docstring (résumé) + « — pytest passed|failed|skipped — … »
    """
    if not path.is_file():
        return {}
    tree = ET.parse(path)
    root = tree.getroot()
    results: dict[str, tuple[str, str, float]] = {}

    # pytest : <testsuites><testsuite><testcase>...
    for suite in root.iter("testsuite"):
        for case in suite.iter("testcase"):
            name = case.get("name") or ""
            fn = junit_basename(name)
            csv_id = fn_to_csv.get(fn)
            if not csv_id:
                continue
            t = float(case.get("time") or 0)
            doc = csv_to_doc.get(csv_id, "")

            failure = case.find("failure")
            error = case.find("error")
            skipped = case.find("skipped")

            if failure is not None:
                msg = (failure.get("message") or failure.text or "").strip()
                msg = " ".join(msg.split())[:500]
                detail = msg or "Échec assertion / erreur Playwright"
                results[csv_id] = ("KO", _combine_col_h(doc, "KO", detail, t), t)
            elif error is not None:
                msg = (error.get("message") or error.text or "").strip()
                msg = " ".join(msg.split())[:500]
                detail = msg or "Erreur"
                results[csv_id] = ("KO", _combine_col_h(doc, "KO", detail, t), t)
            elif skipped is not None:
                msg = (skipped.get("message") or skipped.text or "skipped").strip()
                msg = " ".join(msg.split())[:500]
                results[csv_id] = ("N/A", _combine_col_h(doc, "N/A", msg, t), t)
            else:
                results[csv_id] = ("OK", _combine_col_h(doc, "OK", "", t), t)
    return results


# Numéros de pas type 1.1 / 2.3.3.1 (exclut les lignes de section « 1 », « 2 », « 2.4 » seuls)
_PAS_STEP = re.compile(r"^\d+(?:\.\d+)+$")
# Lignes de regroupement du CSV (pas de cas E2E dédié sur cette ligne)
_PAS_GROUP_ONLY = frozenset({"2.4"})


def line_to_csv_key(line_full: str, parts: list[str], state: dict) -> str | None:
    """Clé de rapprochement avec les ids @pytest.mark.csv (dont -crit / -ind)."""
    if len(parts) < 2:
        return None
    pas = parts[1].strip()
    if not pas or pas in _PAS_GROUP_ONLY:
        return None
    if not _PAS_STEP.match(pas):
        return None
    if pas == "2.3.3":
        if "Indicateurs" in line_full:
            return "2.3.3-ind"
        if "Critères" in line_full or "Critres" in line_full:
            return "2.3.3-crit"
        return None
    # Même numéro de pas sur 3 lignes distinctes (régions / 1 dept / plusieurs depts)
    if pas == "2.3.3.2.3":
        if "complète" in line_full and "départements disponibles" in line_full:
            return "2.3.3.2.3-a"
        if "plusieurs départements" in line_full.lower():
            return "2.3.3.2.3-c"
        if "un département" in line_full.lower():
            return "2.3.3.2.3-b"
        return None
    return pas


def pad_row(row: list[str]) -> list[str]:
    while len(row) < MIN_COLS:
        row.append("")
    return row


def apply_results(
    rows: list[list[str]],
    results: dict[str, tuple[str, str, float]],
    date_str: str,
) -> list[list[str]]:
    state: dict = {}
    for row in rows:
        if len(row) < 2:
            continue
        line_repr = "|".join(row)
        key = line_to_csv_key(line_repr, row, state)
        if key is None:
            continue
        pad_row(row)
        if key in results:
            statut, msg, t = results[key]
            row[COL_G] = "Oui"
            row[COL_H] = msg
            row[COL_I] = statut
            row[COL_J] = date_str
        else:
            row[COL_G] = "Non"
            row[COL_H] = "Pas de test E2E automatisé (voir e2e/test_comparaison_scores.py)"
            row[COL_I] = "N/A"
            row[COL_J] = date_str
    return rows


def write_pipe_csv(path: Path, rows: list[list[str]]) -> None:
    buf = io.StringIO()
    w = csv.writer(buf, delimiter="|", lineterminator="\n", quoting=csv.QUOTE_MINIMAL)
    for row in rows:
        w.writerow(row)
    # newline="" : évite la conversion \n -> \r\n sous Windows (csv a déjà lineterminator="\n").
    # open(..., newline="") au lieu de write_text(..., newline="") : write_text n'accepte newline qu'à partir de Python 3.10.
    with path.open("w", encoding="utf-8", newline="") as f:
        f.write(buf.getvalue())


def read_pipe_csv(path: Path) -> list[list[str]]:
    with path.open(encoding="utf-8", newline="") as f:
        return list(csv.reader(f, delimiter="|"))


def main() -> int:
    p = argparse.ArgumentParser(description="JUnit pytest -> colonnes G-J grille comparaison scores")
    p.add_argument("--source", type=Path, default=DEFAULT_SOURCE, help="Fichier grille source (.csv)")
    p.add_argument("--junit", type=Path, default=DEFAULT_JUNIT, help="Fichier XML JUnit (pytest --junitxml=...)")
    p.add_argument("--out", type=Path, default=DEFAULT_OUT, help="Fichier sortie (.txt)")
    p.add_argument("--date", default="", help="Date colonne J ( défaut : aujourd’hui JJ/MM/AAAA )")
    args = p.parse_args()
    date_str = args.date.strip() or datetime.now().strftime("%d/%m/%Y")

    if not args.source.is_file():
        print("Fichier source introuvable:", args.source, file=sys.stderr)
        return 1

    fn_to_csv, csv_to_doc = parse_tests_py_metadata(TEST_FILE)
    if not fn_to_csv:
        print("Aucun @pytest.mark.csv trouvé dans", TEST_FILE, file=sys.stderr)
        return 1

    results = parse_junit(args.junit, fn_to_csv, csv_to_doc)
    rows = read_pipe_csv(args.source)
    rows = apply_results(rows, results, date_str)

    # Métadonnée ligne 2 : date d’exécution (colonne C = index 2 si présent)
    if len(rows) >= 2 and len(rows[1]) > 2:
        rows[1] = pad_row(rows[1])
        rows[1][2] = date_str

    args.out.parent.mkdir(parents=True, exist_ok=True)
    write_pipe_csv(args.out, rows)
    print("Écrit:", args.out)
    print("JUnit lu:", args.junit, "—", len(results), "résultat(s) mappé(s)")
    if not args.junit.is_file():
        print(
            "Astuce : lancer d’abord\n  python -m pytest e2e/ -v --junitxml=test_output/e2e_junit.xml",
            file=sys.stderr,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
