#!/usr/bin/env python3
import csv
import json
import os
import re
import sys
from pathlib import Path
from typing import Optional

import psycopg2
from psycopg2.extras import execute_batch


# Répertoire du script
SCRIPT_DIR = Path(__file__).resolve().parent

# Dossiers où chercher config.postgres.json (même logique que les autres scripts)
_CONFIG_DIRS = (
    SCRIPT_DIR,
    SCRIPT_DIR.parent,
    SCRIPT_DIR.parent.parent,
)


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


def infer_year_from_filename(path: str) -> int:
    """
    Déduit l'année à partir du nom du fichier.

    Exemples supportés :
      - dvf2024_stats_immo.csv
      - dvf2024.csv
      - dvf2024-quelquechose.csv
    """
    base = os.path.basename(path)
    m = re.search(r"dvf(\d{4})", base)
    if not m:
        raise ValueError(
            f"Impossible de déduire l'année depuis le nom de fichier: {base} "
            "(attendu format type 'dvf2024*.csv')"
        )
    return int(m.group(1))


def parse_int(value: str) -> Optional[int]:
    value = value.strip()
    if not value:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def parse_float(value: str) -> Optional[float]:
    value = value.strip()
    if not value:
        return None
    # Remplacer la virgule éventuelle par un point
    value = value.replace(",", ".")
    try:
        return float(value)
    except ValueError:
        return None


def load_csv_into_db(conn, csv_path: str, table: str = "foncier.agreg_communes_dvf", batch_size: int = 5000) -> None:
    year_from_name = infer_year_from_filename(csv_path)
    print(f"Import de {csv_path} (annee={year_from_name})")

    insert_sql = f"""
        INSERT INTO {table} (
            insee_com,
            annee,
            nb_mutations,
            nb_maisons,
            nb_apparts,
            prop_maison,
            prop_appart,
            prix_moyen,
            prixm2_moyen,
            surface_moy
        )
        VALUES (
            %(insee_com)s,
            %(annee)s,
            %(nb_mutations)s,
            %(nb_maisons)s,
            %(nb_apparts)s,
            %(prop_maison)s,
            %(prop_appart)s,
            %(prix_moyen)s,
            %(prixm2_moyen)s,
            %(surface_moy)s
        )
        ON CONFLICT (insee_com, annee) DO UPDATE SET
            nb_mutations = EXCLUDED.nb_mutations,
            nb_maisons   = EXCLUDED.nb_maisons,
            nb_apparts   = EXCLUDED.nb_apparts,
            prop_maison  = EXCLUDED.prop_maison,
            prop_appart  = EXCLUDED.prop_appart,
            prix_moyen   = EXCLUDED.prix_moyen,
            prixm2_moyen = EXCLUDED.prixm2_moyen,
            surface_moy  = EXCLUDED.surface_moy;
    """

    with conn.cursor() as cur, open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter=",")
        batch = []
        total = 0

        for row in reader:
            # On peut vérifier la cohérence avec la colonne 'annee' du fichier si elle existe
            annee_csv = parse_int(row.get("annee", "").strip()) if "annee" in row else None
            if annee_csv is not None and annee_csv != year_from_name:
                # On logge un avertissement mais on continue (en forçant l'année du nom de fichier)
                print(
                    f"  Avertissement: annee CSV={annee_csv} différente de l'année dans le nom de fichier "
                    f"({year_from_name}) pour INSEE_COM={row.get('INSEE_COM')}",
                    file=sys.stderr,
                )

            data = {
                "insee_com":      row["INSEE_COM"].strip(),
                "annee":          year_from_name,
                "nb_mutations":   parse_int(row.get("nb_mutations", "")),
                "nb_maisons":     parse_int(row.get("NbMaisons", "")),
                "nb_apparts":     parse_int(row.get("NbApparts", "")),
                "prop_maison":    parse_float(row.get("PropMaison", "")),
                "prop_appart":    parse_float(row.get("PropAppart", "")),
                "prix_moyen":     parse_float(row.get("PrixMoyen", "")),
                "prixm2_moyen":   parse_float(row.get("Prixm2Moyen", "")),
                "surface_moy":    parse_float(row.get("SurfaceMoy", "")),
            }
            batch.append(data)
            total += 1

            if len(batch) >= batch_size:
                execute_batch(cur, insert_sql, batch, page_size=batch_size)
                conn.commit()
                print(f"  -> {total} lignes traitées...")
                batch.clear()

        if batch:
            execute_batch(cur, insert_sql, batch, page_size=batch_size)
            conn.commit()
            print(f"  -> {total} lignes traitées (fin).")


def main(argv: list[str]) -> None:
    """
    Modes d'appel possibles :

      1) Fichiers explicites :
         python import_agreg_communes_dvf.py dvf2024_stats_immo.csv dvf2023_stats_immo.csv

      2) Tous les fichiers dvf*.csv du répertoire courant :
         python import_agreg_communes_dvf.py all

      3) Intervalle d'années [annee_debut, annee_fin] (incluses) :
         python import_agreg_communes_dvf.py 2014 2024
         -> pour chaque année Y, recherche des fichiers dvfY*.csv dans le répertoire courant.
    """
    if len(argv) < 2:
        print(
            "Usage :\n"
            "  python import_agreg_communes_dvf.py <dvfYYYY*.csv> [autres.csv...]\n"
            "  python import_agreg_communes_dvf.py all\n"
            "  python import_agreg_communes_dvf.py <annee_debut> <annee_fin>\n\n"
            "Exemples :\n"
            "  python import_agreg_communes_dvf.py dvf2024_stats_immo.csv\n"
            "  python import_agreg_communes_dvf.py all\n"
            "  python import_agreg_communes_dvf.py 2014 2024",
            file=sys.stderr,
        )
        sys.exit(1)

    # Détermination de la liste de fichiers à traiter
    args = argv[1:]
    base_dir = Path.cwd()
    files: list[Path] = []

    # Mode 2 : "all" -> tous les dvf*.csv du répertoire courant
    if len(args) == 1 and args[0].lower() == "all":
        files = sorted(base_dir.glob("dvf*.csv"))
        if not files:
            print("Aucun fichier dvf*.csv trouvé dans le répertoire courant.", file=sys.stderr)
            sys.exit(1)

    # Mode 3 : intervalle d'années
    elif len(args) == 2 and args[0].isdigit() and args[1].isdigit():
        start_year = int(args[0])
        end_year = int(args[1])
        if start_year > end_year:
            start_year, end_year = end_year, start_year

        for year in range(start_year, end_year + 1):
            pattern = f"dvf{year}*.csv"
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

    # Connexion DB et import
    conn = psycopg2.connect(**get_db_config())

    try:
        for path in files:
            load_csv_into_db(conn, str(path))
    finally:
        conn.close()


if __name__ == "__main__":
    main(sys.argv)