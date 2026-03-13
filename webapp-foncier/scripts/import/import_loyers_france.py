import csv
import json
import os
import re
import sys
from pathlib import Path
from typing import Optional, Tuple

import psycopg2
from psycopg2.extras import execute_batch


SCRIPT_DIR = Path(__file__).resolve().parent

_CONFIG_DIRS = (
    SCRIPT_DIR,
    SCRIPT_DIR.parent,
    SCRIPT_DIR.parent.parent,
)


def get_db_config() -> dict:
    """Charge la config DB depuis config.postgres.json (même logique que les autres scripts d'import)."""
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


def infer_metadata_from_filename(filename: str) -> Tuple[str, str, int]:
    """
    Déduit (type_bien, segment_surface, annee) depuis le nom de fichier.

    Exemples :
      - pred-app-mef-dhup-2024.csv    -> appartement, all,           2024
      - pred-app12-mef-dhup-2024.csv  -> appartement, 1-2_pieces,    2024
      - pred-app3-mef-dhup-2024.csv   -> appartement, 3_plus_pieces, 2024
      - pred-mai-mef-dhup-2024.csv    -> maison,      all,           2024
    """

    base = os.path.basename(filename)

    # Année
    m_year = re.search(r"-(\d{4})\.csv$", base)
    if not m_year:
        raise ValueError(f"Impossible de déduire l'année depuis le nom de fichier: {base}")
    annee = int(m_year.group(1))

    core = base[: m_year.start(1) - 5]  # retire "-YYYY.csv"

    type_bien: str
    segment_surface: str

    if "pred-app12" in core:
        type_bien = "appartement"
        segment_surface = "1-2_pieces"
    elif "pred-app3" in core:
        type_bien = "appartement"
        segment_surface = "3_plus_pieces"
    elif "pred-app" in core:
        type_bien = "appartement"
        segment_surface = "all"
    elif "pred-mai" in core:
        type_bien = "maison"
        segment_surface = "all"
    else:
        raise ValueError(f"Nom de fichier non reconnu pour type_bien/surface: {base}")

    return type_bien, segment_surface, annee


def parse_float(value: str) -> Optional[float]:
    value = value.strip()
    if not value:
        return None
    # Les CSV DHUP sont souvent au format français (virgule décimale)
    value = value.replace(",", ".")
    try:
        return float(value)
    except ValueError:
        return None


def parse_int(value: str) -> Optional[int]:
    value = value.strip()
    if not value:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def load_csv_into_db(
    conn,
    csv_path: str,
    table_name: str = "loyers_communes",
    batch_size: int = 5000,
) -> None:
    type_bien, segment_surface, annee = infer_metadata_from_filename(csv_path)
    print(f"Import de {csv_path} -> type_bien={type_bien}, segment_surface={segment_surface}, annee={annee}")

    insert_sql = f"""
        INSERT INTO {table_name} (
            id_zone,
            insee_c,
            libgeo,
            epci,
            dep,
            reg,
            loypredm2,
            lwr_ipm2,
            upr_ipm2,
            typpred,
            nbobs_com,
            nbobs_mail,
            r2_adj,
            type_bien,
            segment_surface,
            annee
        )
        VALUES (
            %(id_zone)s,
            %(insee_c)s,
            %(libgeo)s,
            %(epci)s,
            %(dep)s,
            %(reg)s,
            %(loypredm2)s,
            %(lwr_ipm2)s,
            %(upr_ipm2)s,
            %(typpred)s,
            %(nbobs_com)s,
            %(nbobs_mail)s,
            %(r2_adj)s,
            %(type_bien)s,
            %(segment_surface)s,
            %(annee)s
        )
        -- Si tu définis une clé unique, tu peux décommenter pour faire un UPSERT :
        -- ON CONFLICT (annee, insee_c, type_bien, segment_surface, typpred)
        -- DO UPDATE SET
        --   loypredm2 = EXCLUDED.loypredm2,
        --   lwr_ipm2  = EXCLUDED.lwr_ipm2,
        --   upr_ipm2  = EXCLUDED.upr_ipm2,
        --   nbobs_com = EXCLUDED.nbobs_com,
        --   nbobs_mail= EXCLUDED.nbobs_mail,
        --   r2_adj    = EXCLUDED.r2_adj
    """

    # Les fichiers DHUP sont souvent encodés en ISO-8859-1 / cp1252 (accents français).
    # On utilise latin-1 pour éviter les erreurs UnicodeDecodeError et conserver les caractères.
    with conn.cursor() as cur, open(csv_path, "r", encoding="latin-1", newline="") as f:
        reader = csv.DictReader(f, delimiter=";", quotechar='"')
        batch = []
        total = 0

        for row in reader:
            data = {
                "id_zone":       parse_int(row["id_zone"]),
                "insee_c":       row["INSEE_C"].strip(),
                "libgeo":        row["LIBGEO"].strip(),
                "epci":          row["EPCI"].strip(),
                "dep":           row["DEP"].strip(),
                "reg":           row["REG"].strip(),
                "loypredm2":     parse_float(row["loypredm2"]),
                "lwr_ipm2":      parse_float(row["lwr.IPm2"]),
                "upr_ipm2":      parse_float(row["upr.IPm2"]),
                "typpred":       row["TYPPRED"].strip(),
                "nbobs_com":     parse_int(row["nbobs_com"]),
                "nbobs_mail":    parse_int(row["nbobs_mail"]),
                "r2_adj":        parse_float(row["R2_adj"]),
                "type_bien":     type_bien,
                "segment_surface": segment_surface,
                "annee":         annee,
            }
            batch.append(data)
            total += 1

            if len(batch) >= batch_size:
                execute_batch(cur, insert_sql, batch, page_size=batch_size)
                conn.commit()
                print(f"  -> {total} lignes insérées...")
                batch.clear()

        if batch:
            execute_batch(cur, insert_sql, batch, page_size=batch_size)
            conn.commit()
            print(f"  -> {total} lignes insérées (fin).")


def main(argv: list[str]) -> None:
    if len(argv) < 2:
        print(
            "Usage :\n"
            "  python import_loyers_france.py <fichier1.csv> [<fichier2.csv> ...]\n"
            "  python import_loyers_france.py <annee>\n\n"
            "Exemples :\n"
            "  python import_loyers_france.py pred-app-mef-dhup-2024.csv\n"
            "  python import_loyers_france.py 2024  # cherchera les 4 fichiers prédéfinis pour 2024\n",
            file=sys.stderr,
        )
        sys.exit(1)

    arg1 = argv[1]
    use_year_mode = re.fullmatch(r"\d{4}", arg1) is not None and len(argv) == 2

    paths: list[Path]
    if use_year_mode:
        year = arg1
        expected_files = [
            f"pred-app-mef-dhup-{year}.csv",
            f"pred-app12-mef-dhup-{year}.csv",
            f"pred-app3-mef-dhup-{year}.csv",
            f"pred-mai-mef-dhup-{year}.csv",
        ]

        # On cherche les fichiers dans le répertoire courant depuis lequel le script est appelé
        cwd = Path.cwd()
        paths = [cwd / name for name in expected_files]

        missing = [str(p) for p in paths if not p.exists()]
        if missing:
            print("ATTENTION : certains fichiers attendus sont introuvables :", file=sys.stderr)
            for p in missing:
                print(f"  - {p}", file=sys.stderr)
            # Demander si on continue quand même avec les fichiers présents
            ans = input("Voulez-vous quand même continuer avec les fichiers existants ? (o/N) ").strip().lower()
            if ans not in ("o", "oui", "y", "yes"):
                print("Import annulé car des fichiers sont manquants.", file=sys.stderr)
                sys.exit(1)
            # Ne garder que les fichiers existants
            paths = [p for p in paths if p.exists()]
            if not paths:
                print("Aucun fichier disponible, arrêt.", file=sys.stderr)
                sys.exit(1)
    else:
        # Mode fichiers explicites : tous les arguments après le script sont des chemins de CSV
        paths = []
        for raw in argv[1:]:
            p = Path(raw)
            if not p.is_absolute():
                p = Path.cwd() / p
            if not p.exists():
                print(f"Fichier introuvable : {p}", file=sys.stderr)
                sys.exit(1)
            paths.append(p)

    # Connexion Postgres via config.postgres.json (même logique que les autres scripts d'import)
    conn = psycopg2.connect(**get_db_config())

    try:
        for path in paths:
            load_csv_into_db(conn, str(path), table_name="loyers_communes")
    finally:
        conn.close()


if __name__ == "__main__":
    main(sys.argv)