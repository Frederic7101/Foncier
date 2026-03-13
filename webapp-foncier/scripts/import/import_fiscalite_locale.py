#!/usr/bin/env python3
import csv
import json
import os
import re
import sys
from pathlib import Path
from typing import Iterable, Dict, Any, List, Optional
from concurrent.futures import ProcessPoolExecutor

import psycopg2
from psycopg2.extras import execute_batch


# Répertoire du script
SCRIPT_DIR = Path(__file__).resolve().parent

# Dossiers où chercher config.postgres.json / config.json
_CONFIG_DIRS = (
    SCRIPT_DIR,
    SCRIPT_DIR.parent,
    SCRIPT_DIR.parent.parent,
)

DEBUG = False
DEBUG_LEVEL = 0  # 1 = afficher champs attendus au 1er rejet puis arrêt


class DebugFirstRejectError(Exception):
    """Levée en mode --debug 1 après affichage du premier enregistrement rejeté."""
    pass


# -------------------------------------------------------------------
# Config / Connexion DB
# -------------------------------------------------------------------
def get_db_config() -> dict:
    """
    Charge la config DB depuis config.postgres.json ou config.json.
    Ne JAMAIS stocker les paramètres en dur dans le code.
    """
    candidates = ("config.postgres.json", "config.json")
    for base in _CONFIG_DIRS:
        for name in candidates:
            config_path = base / name
            if config_path.is_file():
                with open(config_path, encoding="utf-8") as f:
                    data = json.load(f)
                db = data.get("database") or data
                return {
                    "host": db.get("host"),
                    "port": int(db.get("port") or 5432),
                    "dbname": db.get("database", "foncier"),
                    "user": db.get("user"),
                    "password": db.get("password") or "",
                }
    raise RuntimeError("Aucun fichier config.postgres.json ou config.json trouvé pour la configuration PostgreSQL.")


def get_db_connection():
    return psycopg2.connect(**get_db_config())


# -------------------------------------------------------------------
# Parsing utilitaires
# -------------------------------------------------------------------
def parse_int(value: str) -> Optional[int]:
    if value is None:
        return None
    value = value.strip()
    if not value:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def parse_float(value: str) -> Optional[float]:
    """
    Convertit une chaîne en float en tolérant les virgules décimales.
    Retourne None si la valeur est vide ou invalide.
    """
    if value is None:
        return None
    value = value.strip()
    if not value:
        return None
    value = value.replace(",", ".")
    try:
        return float(value)
    except ValueError:
        return None


def detect_delimiter(sample_line: str) -> str:
    """Détecte rapidement le séparateur le plus probable entre ';' et ','."""
    count_semicolon = sample_line.count(";")
    count_comma = sample_line.count(",")
    return ";" if count_semicolon >= count_comma else ","


# BOM UTF-8 (EF BB BF) lu en latin1 donne ce préfixe sur le 1er en-tête
BOM_LATIN1_PREFIX = "ï»¿"


def strip_bom_header(name: str) -> str:
    """
    Retire le BOM UTF-8 du premier en-tête si le fichier est lu en latin1.
    Retire aussi le caractère BOM Unicode (U+FEFF) si présent.
    """
    if name.startswith(BOM_LATIN1_PREFIX):
        name = name[len(BOM_LATIN1_PREFIX) :]
    return name.lstrip("\ufeff")


def infer_year_from_filename(path: str) -> Optional[int]:
    """
    Déduit l'année depuis le nom de fichier, en cherchant une séquence de 4 chiffres.
    Exemple : fiscalite-locale-des-particuliers-2024.csv -> 2024
    """
    base = os.path.basename(path)
    m = re.search(r"(\d{4})", base)
    if not m:
        return None
    return int(m.group(1))


def extract_year_from_row(row: Dict[str, Any], fallback_year: Optional[int]) -> Optional[int]:
    """
    Recherche une colonne année dans la ligne :
      - 'annee', 'ANNEE'
      - 'exercice', 'EXERCICE'
      - 'an', 'AN'
    Sinon retourne fallback_year (année déduite du nom de fichier).
    """
    year_keys = ["annee", "ANNEE", "exercice", "EXERCICE", "an", "AN"]
    for key in year_keys:
        if key in row and row.get(key) not in (None, ""):
            return parse_int(row.get(key))
    return fallback_year


# Champs attendus (fichier -> table) pour affichage en mode debug niveau 1
FISCALITE_ATTENDED_HEADERS = [
    ("REG", "code_reg"),
    ("DEP", "code_dep"),
    ("INSEE COM", "code_insee"),
    ("EXERCICE", "annee"),
    ("LIBCOM", "commune_majuscule"),
    ("Taux_Global_TFNB", "Taux_Global_TFNB"),
    ("Taux_Global_TFB", "Taux_Global_TFB"),
    ("Taux_Plein_TEOM", "Taux_TEOM"),
    ("Taux_Global_TH", "Taux_Global_TH"),
]


# -------------------------------------------------------------------
# Lecture CSV -> lignes pour la table foncier.fiscalite_locale
# -------------------------------------------------------------------
def iter_fiscalite_from_csv(csv_path: str, debug_level: int = 0) -> Iterable[Dict[str, Any]]:
    """
    Retourne les lignes prêtes à être upsertées dans foncier.fiscalite_locale.

    Clé d'unicité : (code_reg, code_dep, code_insee, annee)
    - code_reg  : champs 'REG' ou 'reg'
    - code_dep  : champs 'DEP' ou 'dep'
    - code_insee: champ 'INSEE COM' (ou variantes)
    - annee     : champ 'annee' / 'exercice' / 'an' (et variantes de casse) ou, à défaut, année du nom de fichier
    """
    filename = os.path.basename(csv_path)
    year_from_name = infer_year_from_filename(csv_path)

    if DEBUG:
        print(f"[DEBUG][PID {os.getpid()}] Lecture de {csv_path}, année fichier={year_from_name}")

    with open(csv_path, "r", encoding="latin1", newline="") as f:
        first_line = f.readline()
        if not first_line:
            return
        delimiter = detect_delimiter(first_line)
        if DEBUG:
            print(f"[DEBUG][PID {os.getpid()}] Délimiteur détecté pour {csv_path}: '{delimiter}'")

        f.seek(0)
        reader = csv.DictReader(f, delimiter=delimiter)
        # Normaliser les noms de colonnes (BOM UTF-8 lu en latin1 -> "ï»¿" en tête du 1er champ)
        if reader.fieldnames:
            reader.fieldnames = [strip_bom_header(fn) for fn in reader.fieldnames]

        # Fichier de rejets
        base_dir = os.path.dirname(csv_path)
        prefix, _ = os.path.splitext(filename)
        reject_path = os.path.join(base_dir, f"{prefix}.rejets.csv")
        reject_file = open(reject_path, "w", encoding="latin1", newline="")
        try:
            reject_writer = csv.DictWriter(reject_file, fieldnames=reader.fieldnames or [])
            reject_writer.writeheader()

            nb_total = 0
            nb_valid = 0
            nb_skipped = 0

            for row in reader:
                nb_total += 1

                # Année: priorité à une colonne explicite, sinon année du nom de fichier
                annee = extract_year_from_row(row, year_from_name)
                reg = (row.get("REG") or row.get("reg") or row.get("REG_CODE") or row.get("reg_code") or "").strip()
                dep = (row.get("DEP") or row.get("dep") or row.get("DEP_CODE") or row.get("dep_code") or "").strip()
                code_insee = (row.get("INSEE COM") or row.get("INSEE_COM") or row.get("insee com") or row.get("insee_com") or "").strip()

                if not reg or not dep or not code_insee or annee is None:
                    nb_skipped += 1
                    reject_writer.writerow(row)
                    if DEBUG:
                        print(
                            f"[DEBUG][PID {os.getpid()}] Ligne ignorée (clé incomplète) "
                            f"REG='{reg}' DEP='{dep}' INSEE='{code_insee}' ANNEE='{annee}' dans {csv_path}: {row}"
                        )
                    if debug_level >= 1:
                        print(
                            f"[DEBUG niveau 1] Premier enregistrement invalide (ligne {nb_total}) dans {csv_path}.",
                            file=sys.stderr,
                        )
                        print("Champs attendus pour valider un enregistrement (fichier -> table):", file=sys.stderr)
                        for h_fichier, h_table in FISCALITE_ATTENDED_HEADERS:
                            print(f"  {h_fichier!r} -> {h_table}", file=sys.stderr)
                        print("En-têtes réels du fichier:", file=sys.stderr)
                        print(f"  {list(reader.fieldnames or [])}", file=sys.stderr)
                        print(f"Ligne rejetée (numéro {nb_total}): {row}", file=sys.stderr)
                        raise DebugFirstRejectError(
                            f"Arrêt après premier rejet (ligne {nb_total}) en mode --debug 1."
                        )
                    continue

                commune_maj = (row.get("LIBCOM") or row.get("libcom") or "").strip()
                taux_tfnb = parse_float(row.get("Taux_Global_TFNB") or row.get("TAUX_GLOBAL_TFNB"))
                taux_tfb = parse_float(row.get("Taux_Global_TFB") or row.get("TAUX_GLOBAL_TFB"))
                taux_teom = parse_float(row.get("Taux_Plein_TEOM") or row.get("TAUX_PLEIN_TEOM"))
                taux_th = parse_float(row.get("Taux_Global_TH") or row.get("TAUX_GLOBAL_TH"))

                nb_valid += 1

                yield {
                    "code_reg": reg,
                    "code_dep": dep,
                    "code_insee": code_insee,
                    "annee": annee,
                    "commune_majuscule": commune_maj,
                    "Taux_Global_TFNB": taux_tfnb,
                    "Taux_Global_TFB": taux_tfb,
                    "Taux_TEOM": taux_teom,
                    "Taux_Global_TH": taux_th,
                }

            print(
                f"[INFO][PID {os.getpid()}] Fichier {csv_path}: "
                f"{nb_total} lignes lues, {nb_valid} valides, {nb_skipped} ignorées (rejets dans {reject_path})."
            )
        finally:
            reject_file.close()


# -------------------------------------------------------------------
# SQL d'upsert
# -------------------------------------------------------------------
INSERT_SQL = """
    INSERT INTO foncier.fiscalite_locale (
        code_reg,
        code_dep,
        code_insee,
        annee,
        commune_majuscule,
        Taux_Global_TFNB,
        Taux_Global_TFB,
        Taux_TEOM,
        Taux_Global_TH
    )
    VALUES (
        %(code_reg)s,
        %(code_dep)s,
        %(code_insee)s,
        %(annee)s,
        %(commune_majuscule)s,
        %(Taux_Global_TFNB)s,
        %(Taux_Global_TFB)s,
        %(Taux_TEOM)s,
        %(Taux_Global_TH)s
    )
    ON CONFLICT (code_reg, code_dep, code_insee, annee) DO UPDATE SET
        commune_majuscule = EXCLUDED.commune_majuscule,
        Taux_Global_TFNB  = EXCLUDED.Taux_Global_TFNB,
        Taux_Global_TFB   = EXCLUDED.Taux_Global_TFB,
        Taux_TEOM         = EXCLUDED.Taux_TEOM,
        Taux_Global_TH    = EXCLUDED.Taux_Global_TH;
"""


# -------------------------------------------------------------------
# Import d'un fichier (un process par fichier)
# -------------------------------------------------------------------
def load_csv_into_db(
    csv_path: str,
    batch_size: int = 5_000,
    debug: bool = False,
    debug_level: int = 0,
    total_files: int = 1,
) -> None:
    conn = get_db_connection()
    try:
        filename = os.path.basename(csv_path)
        if debug:
            print(f"[DEBUG][PID {os.getpid()}] Import de {filename} avec batch_size={batch_size}")

        with conn.cursor() as cur:
            batch: List[Dict[str, Any]] = []
            total = 0

            for data in iter_fiscalite_from_csv(csv_path, debug_level=debug_level):
                batch.append(data)
                total += 1

                if len(batch) >= batch_size:
                    execute_batch(cur, INSERT_SQL, batch, page_size=batch_size)
                    conn.commit()
                    # Progression même sans DEBUG
                    if total_files == 1:
                        print(f"[INFO][PID {os.getpid()}] {total} lignes cumulées insérées")
                    else:
                        print(f"[INFO][PID {os.getpid()}] {total} lignes cumulées insérées pour {filename}")
                    if debug:
                        print(f"[DEBUG][PID {os.getpid()}] Batch exécuté pour {filename} ({batch_size} lignes).")
                    batch.clear()

            if batch:
                execute_batch(cur, INSERT_SQL, batch, page_size=len(batch))
                conn.commit()
                if total_files == 1:
                    print(f"[INFO][PID {os.getpid()}] {total} lignes cumulées insérées (fin, batch final)")
                else:
                    print(
                        f"[INFO][PID {os.getpid()}] {total} lignes cumulées insérées "
                        f"(fin, batch final) pour {filename}"
                    )
                if debug:
                    print(f"[DEBUG][PID {os.getpid()}] Batch final exécuté pour {filename} ({len(batch)} lignes).")

    except DebugFirstRejectError as e:
        print(str(e), file=sys.stderr)
        sys.exit(1)
    finally:
        conn.close()


def _worker_import_one(path_str: str, batch_size: int, debug: bool, debug_level: int, total_files: int):
    try:
        load_csv_into_db(
            path_str,
            batch_size=batch_size,
            debug=debug,
            debug_level=debug_level,
            total_files=total_files,
        )
    except Exception as e:
        print(f"[PID {os.getpid()}] ERREUR lors de l'import de {path_str} : {e}", file=sys.stderr)
        raise


# -------------------------------------------------------------------
# CLI / Gestion des fichiers (années, plage, all, etc.)
# -------------------------------------------------------------------
def main(argv: list[str]) -> None:
    """
    Modes d'appel possibles :

      1) Fichiers explicites :
         python import_fiscalite_locale.py [--debug] [--batch-size N] fiscalite-locale-des-particuliers-2024.csv ...

      2) Une année unique :
         python import_fiscalite_locale.py [--debug] [--batch-size N] 2024
         -> cherche fiscalite-locale-des-particuliers*2024*.csv dans le répertoire courant.

      3) Plage d'années [annee_debut, annee_fin] (incluses) :
         python import_fiscalite_locale.py [--debug] [--batch-size N] 2018 2024
         -> pour chaque année Y, cherche fiscalite-locale-des-particuliers*Y*.csv.

      4) Option all avec préfixe :
         python import_fiscalite_locale.py [--debug] [--batch-size N] all fiscalite-locale-des-particuliers
         -> tous les fichiers dont le nom commence par ce préfixe, année quelconque.

      5) Option all sans préfixe (par défaut sur prefix = 'fiscalite-locale-des-particuliers'):
         python import_fiscalite_locale.py [--debug] [--batch-size N] all
    """
    global DEBUG, DEBUG_LEVEL

    if len(argv) < 2:
        print(
            "Usage :\n"
            "  python import_fiscalite_locale.py [--debug [1]] [--batch-size N] <fichier1.csv> [fichier2.csv...]\n"
            "  python import_fiscalite_locale.py [--debug [1]] [--batch-size N] <annee>\n"
            "  python import_fiscalite_locale.py [--debug] [--batch-size N] <annee_debut> <annee_fin>\n"
            "  python import_fiscalite_locale.py [--debug] [--batch-size N] all [prefixe]\n\n"
            "Exemples :\n"
            "  python import_fiscalite_locale.py fiscalite-locale-des-particuliers-2024.csv\n"
            "  python import_fiscalite_locale.py --debug 2024\n"
            "  python import_fiscalite_locale.py 2018 2024\n"
            "  python import_fiscalite_locale.py all fiscalite-locale-des-particuliers\n",
            file=sys.stderr,
        )
        sys.exit(1)

    args = argv[1:]
    batch_size = 5_000

    # Parsing options globales
    i = 0
    new_args: list[str] = []
    while i < len(args):
        if args[i] == "--debug":
            DEBUG = True
            DEBUG_LEVEL = 1
            i += 1
            if i < len(args) and args[i].isdigit():
                DEBUG_LEVEL = int(args[i])
                i += 1
        elif args[i] == "--batch-size" and i + 1 < len(args):
            try:
                batch_size = int(args[i + 1])
            except ValueError:
                print("Valeur invalide pour --batch-size (entier attendu).", file=sys.stderr)
                sys.exit(1)
            i += 2
        else:
            new_args.append(args[i])
            i += 1

    args = new_args
    if not args:
        print("Aucun argument (fichier/année/all) après les options.", file=sys.stderr)
        sys.exit(1)

    base_dir = Path.cwd()
    files: list[Path] = []

    # Option all (avec ou sans préfixe)
    if args[0].lower() == "all":
        prefix = args[1] if len(args) > 1 else "fiscalite-locale-des-particuliers"
        pattern = f"{prefix}*.csv"
        files = sorted(base_dir.glob(pattern))
        if not files:
            print(f"Aucun fichier trouvé pour le pattern '{pattern}' dans {base_dir}.", file=sys.stderr)
            sys.exit(1)

    # Plage d'années
    elif len(args) == 2 and args[0].isdigit() and args[1].isdigit():
        start_year = int(args[0])
        end_year = int(args[1])
        if start_year > end_year:
            start_year, end_year = end_year, start_year

        prefix = "fiscalite-locale-des-particuliers"
        for year in range(start_year, end_year + 1):
            pattern = f"{prefix}*{year}*.csv"
            year_files = sorted(base_dir.glob(pattern))
            if not year_files:
                print(
                    f"[WARN] Aucun fichier trouvé pour l'année {year} (pattern {pattern}).",
                    file=sys.stderr,
                )
                continue
            files.extend(year_files)

        if not files:
            print("Aucun fichier correspondant aux années demandées n'a été trouvé.", file=sys.stderr)
            sys.exit(1)

    # Année unique
    elif len(args) == 1 and args[0].isdigit():
        year = int(args[0])
        prefix = "fiscalite-locale-des-particuliers"
        pattern = f"{prefix}*{year}*.csv"
        files = sorted(base_dir.glob(pattern))
        if not files:
            print(f"Aucun fichier trouvé pour l'année {year} (pattern {pattern}).", file=sys.stderr)
            sys.exit(1)

    # Fichiers explicites
    else:
        for raw in args:
            p = Path(raw)
            if not p.is_absolute():
                p = base_dir / p
            if not p.exists():
                print(f"Fichier introuvable : {p}", file=sys.stderr)
                sys.exit(1)
            files.append(p)

    total_files = len(files)
    print(f"{total_files} fichier(s) à traiter.")

    # Exécution parallèle : un process par fichier
    with ProcessPoolExecutor() as executor:
        futures = [
            executor.submit(_worker_import_one, str(path), batch_size, DEBUG, DEBUG_LEVEL, total_files)
            for path in files
        ]
        for f in futures:
            f.result()


if __name__ == "__main__":
    main(sys.argv)