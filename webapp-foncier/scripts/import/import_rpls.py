import csv
import json
import os
import re
import sys
from pathlib import Path
from typing import Iterable, Dict, Any, List
import psycopg2
from psycopg2.extras import execute_values
from concurrent.futures import ProcessPoolExecutor
# Répertoire du script
SCRIPT_DIR = Path(__file__).resolve().parent
# Dossiers où chercher config.postgres.json (même logique que les autres scripts)
_CONFIG_DIRS = (
    SCRIPT_DIR,
    SCRIPT_DIR.parent,
    SCRIPT_DIR.parent.parent,
)
DEBUG = False

def extract_year_from_filename(path: str) -> int:
    """
    Exemples supportés :
      - rpls-2024.csv
      - rpls-2024-01.csv (toléré si tu gardes ce format pour certains fichiers)
    """
    filename = os.path.basename(path)
    # Cherche pattern classique -YYYY(.csv ou -MM.csv)
    m = re.search(r'-(\d{4})(?:-[0-9]{2})?\.csv$', filename)
    if not m:
        # fallback : n'importe quel .YYYY(.csv ou -MM.csv)
        m = re.search(r'\.(\d{4})(?:-[0-9]{2})?\.csv$', filename)
    if not m:
        raise ValueError(f"Impossible de trouver l'année dans le nom de fichier : {filename}")
    return int(m.group(1))

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

def iter_logements_from_csv(csv_path: str, annee_rpls: int) -> Iterable[Dict[str, Any]]:
    """
    Mapping des colonnes CSV RPLS vers la table foncier.rpls_logements :
      - reg_code         : 'REG_CODE'
      - dep_code         : 'DEP_CODE'
      - epci_code        : 'EPCI_CODE'
      - code_postal      : 'CODEPOSTAL'
      - commune          : 'LIBCOM'
      - num_voie         : 'NUMVOIE'
      - type_voie        : 'TYPEVOIE'
      - nom_voie         : 'NOMVOIE'
      - etage            : 'ETAGE'
      - surface_habitable_m2 : 'SURFHAB'
      - nombre_pieces    : 'NBPIECE'
      - annee_construction      : 'CONSTRUCT'
      - annee_premiere_location : 'LOCAT'
    """
    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter=";")

        for row in reader:
            try:
                reg_code = row.get("REG_CODE")
                dep_code = row.get("DEP_CODE")

                surface = row.get("SURFHAB")
                nb_pieces = row.get("NBPIECE")
                annee_construction = row.get("CONSTRUCT")
                annee_premiere_location = row.get("LOCAT")

                num_voie = row.get("NUMVOIE")
                type_voie = row.get("TYPVOIE")
                nom_voie = row.get("NOMVOIE")
                etage = row.get("ETAGE")

                code_postal = row.get("CODEPOSTAL")
                commune = row.get("LIBCOM")             # demandé explicitement
                epci_code = row.get("EPCI_CODE")

                def to_int(value):
                    if value is None or value == "":
                        return None
                    return int(value)

                def to_float(value):
                    if value is None or value == "":
                        return None
                    return float(value.replace(",", "."))

                yield {
                    "annee_rpls": annee_rpls,
                    "reg_code": reg_code,
                    "dep_code": dep_code,
                    "epci_code": epci_code,
                    "code_postal": code_postal,
                    "commune": commune,
                    "num_voie": num_voie,
                    "type_voie": type_voie,
                    "nom_voie": nom_voie,
                    "etage": etage,
                    "surface_habitable_m2": to_float(surface),
                    "nombre_pieces": to_int(nb_pieces),
                    "annee_construction": to_int(annee_construction),
                    "annee_premiere_location": to_int(annee_premiere_location),
                }

            except Exception:
                # En pratique, log + continue
                continue


INSERT_SQL = """
    INSERT INTO foncier.rpls_logements (
        annee_rpls,
        reg_code,
        dep_code,
        epci_code,
        code_postal,
        commune,
        num_voie,
        type_voie,
        nom_voie,
        etage,
        surface_habitable_m2,
        nombre_pieces,
        annee_construction,
        annee_premiere_location
    ) VALUES %s
"""


def bulk_insert_logements(conn, rows: Iterable[Dict[str, Any]], batch_size: int = 1_000, debug: bool = False):
    with conn.cursor() as cur:
        batch: List[Dict[str, Any]] = []
        total_inserted = 0

        for row in rows:
            batch.append(row)
            if len(batch) >= batch_size:
                execute_values(
                    cur,
                    INSERT_SQL,
                    [
                        (
                            r["annee_rpls"],
                            r["reg_code"],
                            r["dep_code"],
                            r["epci_code"],
                            r["code_postal"],
                            r["commune"],
                            r["num_voie"],
                            r["type_voie"],
                            r["nom_voie"],
                            r["etage"],
                            r["surface_habitable_m2"],
                            r["nombre_pieces"],
                            r["annee_construction"],
                            r["annee_premiere_location"],
                        )
                        for r in batch
                    ],
                )
                conn.commit()  # commit par batch
                total_inserted += len(batch)
                if debug:
                    print(f"[DEBUG] {total_inserted} lignes insérées pour annee_rpls={batch[0]['annee_rpls']}")
                batch.clear()

        if batch:
            execute_values(
                cur,
                INSERT_SQL,
                [
                    (
                        r["annee_rpls"],
                        r["reg_code"],
                        r["dep_code"],
                        r["epci_code"],
                        r["code_postal"],
                        r["commune"],
                        r["num_voie"],
                        r["type_voie"],
                        r["nom_voie"],
                        r["etage"],
                        r["surface_habitable_m2"],
                        r["nombre_pieces"],
                        r["annee_construction"],
                        r["annee_premiere_location"],
                    )
                    for r in batch
                ],
            )
            conn.commit()
            total_inserted += len(batch)
            if debug:
                print(f"[DEBUG] {total_inserted} lignes insérées (fin) pour annee_rpls={batch[0]['annee_rpls']}")


def import_rpls_file(csv_path: str, debug: bool = False, batch_size: int = 1_000):
    annee = extract_year_from_filename(csv_path)
    print(f"[PID {os.getpid()}] Import du fichier {csv_path} pour l'année {annee}")
    conn = get_db_connection()
    try:
        # 1) Reprise : on supprime toutes les lignes de cette année avant de recharger
        with conn.cursor() as cur:
            if debug:
                print(f"[DEBUG][PID {os.getpid()}] Suppression des lignes existantes pour annee_rpls = {annee}...")
            cur.execute("DELETE FROM foncier.rpls_logements WHERE annee_rpls = %s", (annee,))
        conn.commit()
        if debug:
            print(f"[DEBUG][PID {os.getpid()}] Suppression terminée, début de l'import...")
        # 2) Import batché
        rows_iter = iter_logements_from_csv(csv_path, annee)
        bulk_insert_logements(conn, rows_iter, batch_size=batch_size, debug=debug)
        print(f"[PID {os.getpid()}] Import terminé pour l'année {annee}.")
    finally:
        conn.close()


def _worker_import_one(path_str: str, debug: bool, batch_size: int):
    try:
        import_rpls_file(path_str, debug=debug, batch_size=batch_size)
    except Exception as e:
        print(f"[PID {os.getpid()}] ERREUR lors de l'import de {path_str} : {e}", file=sys.stderr)
        raise

def main(argv: list[str]) -> None:
    """
    Modes d'appel possibles :

      1) Fichiers explicites :
         python import_rpls.py [--debug] rpls-2024.csv rpls-2023.csv

      2) Tous les fichiers rpls-*.csv du répertoire courant :
         python import_rpls.py [--debug] all

      3) Une année unique :
         python import_rpls.py [--debug] 2024
         -> cherche rpls-2024*.csv

      4) Intervalle d'années [annee_debut, annee_fin] (incluses) :
         python import_rpls.py [--debug] 2018 2024
         -> pour chaque année Y, recherche des fichiers rpls-Y*.csv dans le répertoire courant.

    Remarque : on suppose un fichier principal par année (par ex. rpls-2024.csv).
    """

    global DEBUG

    if len(argv) < 2:
        print(
            "Usage :\n"
            "  python import_rpls.py [--debug] [--batch-size N] <rpls-YYYY.csv> [autres.csv...]\n"
            "  python import_rpls.py [--debug] [--batch-size N] all\n"
            "  python import_rpls.py [--debug] [--batch-size N] <annee>\n"
            "  python import_rpls.py [--debug] [--batch-size N] <annee_debut> <annee_fin>\n\n"
            "Exemples :\n"
            "  python import_rpls.py rpls-2024.csv\n"
            "  python import_rpls.py --debug all\n"
            "  python import_rpls.py --batch-size 2000 2024\n"
            "  python import_rpls.py --debug --batch-size 5000 2018 2024",
            file=sys.stderr,
        )
        sys.exit(1)

    args = argv[1:]
    batch_size = 1_000

    # Parsing des options globales (--debug, --batch-size)
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
        print("Aucun argument (fichier/année) après les options.", file=sys.stderr)
        sys.exit(1)

    base_dir = Path.cwd()
    files: list[Path] = []

    # Mode 2 : "all" -> tous les rpls-*.csv du répertoire courant
    if len(args) == 1 and args[0].lower() == "all":
        files = sorted(base_dir.glob("rpls-*.csv"))
        if not files:
            print("Aucun fichier rpls-*.csv trouvé dans le répertoire courant.", file=sys.stderr)
            sys.exit(1)

    # Mode 4 : intervalle d'années
    elif len(args) == 2 and args[0].isdigit() and args[1].isdigit():
        start_year = int(args[0])
        end_year = int(args[1])
        if start_year > end_year:
            start_year, end_year = end_year, start_year

        for year in range(start_year, end_year + 1):
            pattern = f"rpls-{year}*.csv"
            year_files = sorted(base_dir.glob(pattern))
            if not year_files:
                print(
                    f"Attention : aucun fichier trouvé pour l'année {year} (pattern {pattern}).",
                    file=sys.stderr,
                )
                continue
            files.extend(year_files)

        if not files:
            print(
                "Aucun fichier correspondant aux années demandées n'a été trouvé.",
                file=sys.stderr,
            )
            sys.exit(1)

    # Mode 3 : année unique
    elif len(args) == 1 and args[0].isdigit():
        year = int(args[0])
        pattern = f"rpls-{year}*.csv"
        files = sorted(base_dir.glob(pattern))
        if not files:
            print(
                f"Aucun fichier trouvé pour l'année {year} (pattern {pattern}).",
                file=sys.stderr,
            )
            sys.exit(1)

    # Mode 1 : fichiers explicites
    else:
        for raw in args:
            p = Path(raw)
            if not p.is_absolute():
                p = base_dir / p
            if not p.exists():
                print(f"Fichier introuvable : {p}", file=sys.stderr)
                sys.exit(1)
            files.append(p)

    # Lancement des imports en parallèle (un process par fichier / année)
    print(f"{len(files)} fichier(s) à traiter.")
    with ProcessPoolExecutor() as executor:
        # On passe le chemin, le flag debug et la taille de batch à chaque worker
        futures = [
            executor.submit(_worker_import_one, str(path), DEBUG, batch_size)
            for path in files
        ]
        # Attente explicite pour remonter les erreurs si besoin
        for f in futures:
            f.result()


if __name__ == "__main__":
    main(sys.argv)