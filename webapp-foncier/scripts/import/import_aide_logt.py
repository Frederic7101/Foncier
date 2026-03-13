#!/usr/bin/env python3
import csv
import json
import os
import sys
from pathlib import Path
from typing import Iterable, Dict, Any, List, Optional
from concurrent.futures import ProcessPoolExecutor

import psycopg2
from psycopg2.extras import execute_batch


SCRIPT_DIR = Path(__file__).resolve().parent

_CONFIG_DIRS = (
    SCRIPT_DIR,
    SCRIPT_DIR.parent,
    SCRIPT_DIR.parent.parent,
)

DEBUG = False


def get_db_config() -> dict:
    """Charge la config DB depuis config.postgres.json."""
    for base in _CONFIG_DIRS:
        config_path = base / "config.postgres.json"
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
    raise RuntimeError("Aucun fichier config.postgres.json trouvé pour la configuration PostgreSQL.")


def get_db_connection():
    return psycopg2.connect(**get_db_config())


def parse_float(value: str) -> Optional[float]:
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


def detect_delimiter(sample_line: str) -> str:
    """Détecte rapidement le séparateur le plus probable entre ';' et ','."""
    count_semicolon = sample_line.count(";")
    count_comma = sample_line.count(",")
    return ";" if count_semicolon >= count_comma else ","


def iter_rows_from_csv(csv_path: str) -> Iterable[Dict[str, Any]]:
    """
    Retourne les lignes prêtes à être upsertées dans foncier.aides_logement_part.
    Détecte automatiquement si le fichier est :
      - aide_logt_part_APL*.csv            -> remplit perc_apl depuis part_apl
      - aide_logt_part_aide_logement*.csv  -> remplit perc_aide_logt depuis part_log
    Gère les séparateurs ';' et ','.
    """
    filename = os.path.basename(csv_path).lower()
    is_apl_file = "apl" in filename and "aide_logement" not in filename
    is_aide_logt_file = "aide_logement" in filename

    if not (is_apl_file or is_aide_logt_file):
        raise ValueError(
            f"Impossible de déduire le type de fichier pour {csv_path} "
            "(attendu aide_logt_part_APL*.csv ou aide_logt_part_aide_logement*.csv)."
        )

    if DEBUG:
        kind = "APL" if is_apl_file else "AIDE_LOGT"
        print(f"[DEBUG][PID {os.getpid()}] Fichier {csv_path} détecté comme type {kind}")

    with open(csv_path, "r", encoding="latin1", newline="") as f:
        first_line = f.readline()
        if not first_line:
            return
        delimiter = detect_delimiter(first_line)
        if DEBUG:
            print(f"[DEBUG][PID {os.getpid()}] Délimiteur détecté pour {csv_path}: '{delimiter}'")

        f.seek(0)
        reader = csv.DictReader(f, delimiter=delimiter)

        # Préparation fichier de rejets
        base_dir = os.path.dirname(csv_path)
        reject_name = "aide_logt_part_APL.rejets.csv" if is_apl_file else "aide_logt_part_aide_logement.rejets.csv"
        reject_path = os.path.join(base_dir, reject_name)
        reject_file = open(reject_path, "w", encoding="latin1", newline="")
        try:
            reject_writer = csv.DictWriter(reject_file, fieldnames=reader.fieldnames or [])
            reject_writer.writeheader()

            nb_total = 0
            nb_valid = 0
            nb_skipped = 0

            for row in reader:
                nb_total += 1

                # Gérer noms de colonnes en minuscules / majuscules (codgeo dans tes fichiers)
                codegeo = (row.get("codegeo") or row.get("codgeo") or row.get("CODEGEO") or row.get("CODGEO") or "").strip()
                libgeo = (row.get("libgeo") or row.get("LIBGEO") or "").strip()
                an = parse_int(row.get("an") or row.get("AN"))

                if not codegeo or not libgeo or an is None:
                    nb_skipped += 1
                    reject_writer.writerow(row)
                    if DEBUG:
                        print(f"[DEBUG][PID {os.getpid()}] Ligne ignorée (clé incomplète) dans {csv_path}: {row}")
                    continue

                perc_apl = None
                perc_aide_logt = None

                if is_apl_file:
                    # part_apl ou PART_APL
                    perc_apl = parse_float(row.get("part_apl") or row.get("PART_APL"))
                if is_aide_logt_file:
                    # part_log ou PART_LOG
                    perc_aide_logt = parse_float(row.get("part_log") or row.get("PART_LOG"))

                nb_valid += 1

                yield {
                    "codegeo": codegeo,
                    "libgeo": libgeo,
                    "an": an,
                    "perc_apl": perc_apl,
                    "perc_aide_logt": perc_aide_logt,
                }

            # Log de synthèse pour comprendre les imports vides
            msg = (
                f"[INFO][PID {os.getpid()}] Fichier {csv_path}: "
                f"{nb_total} lignes lues, {nb_valid} valides, {nb_skipped} ignorées "
                f"(rejets dans {reject_path})."
            )
            print(msg)
        finally:
            reject_file.close()


INSERT_SQL = """
    INSERT INTO foncier.aides_logement_part (
        codegeo,
        libgeo,
        an,
        perc_apl,
        perc_aide_logt
    )
    VALUES (
        %(codegeo)s,
        %(libgeo)s,
        %(an)s,
        %(perc_apl)s,
        %(perc_aide_logt)s
    )
    ON CONFLICT (codegeo, libgeo, an) DO UPDATE SET
        perc_apl       = COALESCE(EXCLUDED.perc_apl, foncier.aides_logement_part.perc_apl),
        perc_aide_logt = COALESCE(EXCLUDED.perc_aide_logt, foncier.aides_logement_part.perc_aide_logt);
"""


def load_csv_into_db(csv_path: str, batch_size: int = 5_000, debug: bool = False) -> None:
    """Import d'un fichier (connexion propre à chaque process)."""
    conn = get_db_connection()
    try:
        if debug:
            print(f"[DEBUG][PID {os.getpid()}] Import de {csv_path} avec batch_size={batch_size}")

        with conn.cursor() as cur:
            batch: List[Dict[str, Any]] = []
            total = 0
            first_batch_done = False

            for data in iter_rows_from_csv(csv_path):
                batch.append(data)
                total += 1

                if len(batch) >= batch_size:
                    if debug and not first_batch_done:
                        print(f"[DEBUG][PID {os.getpid()}] Premier batch pour {csv_path} ({len(batch)} lignes) :")
                        for i, row in enumerate(batch, start=1):
                            print(f"[DEBUG][PID {os.getpid()}]   ligne {i}: {row}")
                        print(f"[DEBUG][PID {os.getpid()}] Requête SQL utilisée :")
                        print(INSERT_SQL)

                    execute_batch(cur, INSERT_SQL, batch, page_size=batch_size)
                    conn.commit()

                    # Trace de progression même sans DEBUG
                    print(f"[INFO][PID {os.getpid()}] {total} lignes cumulées insérées pour {csv_path}.")

                    if debug:
                        print(f"[DEBUG][PID {os.getpid()}] Batch exécuté pour {csv_path} ({batch_size} lignes).")

                    batch.clear()

                    if debug and not first_batch_done:
                        # On s'arrête volontairement après le premier batch en mode debug
                        print(f"[DEBUG][PID {os.getpid()}] Arrêt après le premier batch en mode debug pour inspection.")
                        first_batch_done = True
                        break

            # Si on n'a jamais atteint batch_size mais qu'il reste des lignes
            if batch and not first_batch_done:
                if debug:
                    print(f"[DEBUG][PID {os.getpid()}] Batch final (< batch_size) pour {csv_path} ({len(batch)} lignes) :")
                    for i, row in enumerate(batch, start=1):
                        print(f"[DEBUG][PID {os.getpid()}]   ligne {i}: {row}")
                    print(f"[DEBUG][PID {os.getpid()}] Requête SQL utilisée :")
                    print(INSERT_SQL)

                execute_batch(cur, INSERT_SQL, batch, page_size=len(batch))
                conn.commit()

                print(f"[INFO][PID {os.getpid()}] {total} lignes cumulées insérées (fin, batch final) pour {csv_path}.")

                if debug:
                    print(f"[DEBUG][PID {os.getpid()}] Batch final exécuté pour {csv_path} ({len(batch)} lignes).")
    finally:
        conn.close()


def _worker_import_one(path_str: str, batch_size: int, debug: bool):
    try:
        load_csv_into_db(path_str, batch_size=batch_size, debug=debug)
    except Exception as e:
        print(f"[PID {os.getpid()}] ERREUR lors de l'import de {path_str} : {e}", file=sys.stderr)
        raise


def main(argv: list[str]) -> None:
    """
    Modes d'appel possibles :

      1) Fichiers explicites :
         python import_aide_logt.py [--debug] [--batch-size N] aide_logt_part_APL.csv aide_logt_part_aide_logement.csv

      2) Tous les fichiers attendus du répertoire courant :
         python import_aide_logt.py [--debug] [--batch-size N] all
         -> cherche aide_logt_part_APL*.csv et aide_logt_part_aide_logement*.csv
    """
    global DEBUG

    if len(argv) < 2:
        print(
            "Usage :\n"
            "  python import_aide_logt.py [--debug] [--batch-size N] <fichier1.csv> [fichier2.csv...]\n"
            "  python import_aide_logt.py [--debug] [--batch-size N] all\n\n"
            "Exemples :\n"
            "  python import_aide_logt.py aide_logt_part_APL.csv\n"
            "  python import_aide_logt.py --debug all\n",
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
        print("Aucun argument (fichier ou 'all') après les options.", file=sys.stderr)
        sys.exit(1)

    base_dir = Path.cwd()
    files: list[Path] = []

    # Mode "all" : on cherche les fichiers attendus dans le répertoire courant
    if len(args) == 1 and args[0].lower() == "all":
        patterns = ["aide_logt_part_APL*.csv", "aide_logt_part_aide_logement*.csv"]
        for pattern in patterns:
            found = sorted(base_dir.glob(pattern))
            files.extend(found)
        if not files:
            print("Aucun fichier aide_logt_part_*.csv trouvé dans le répertoire courant.", file=sys.stderr)
            sys.exit(1)
    else:
        # Fichiers explicites
        for raw in args:
            p = Path(raw)
            if not p.is_absolute():
                p = base_dir / p
            if not p.exists():
                print(f"Fichier introuvable : {p}", file=sys.stderr)
                sys.exit(1)
            files.append(p)

    print(f"{len(files)} fichier(s) à traiter.")

    # Lancement des imports en parallèle (un process par fichier)
    with ProcessPoolExecutor() as executor:
        futures = [
            executor.submit(_worker_import_one, str(path), batch_size, DEBUG)
            for path in files
        ]
        for f in futures:
            f.result()


if __name__ == "__main__":
    main(sys.argv)

