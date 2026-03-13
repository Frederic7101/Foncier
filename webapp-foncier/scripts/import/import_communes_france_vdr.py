import csv
import json
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_batch


SCRIPT_DIR = Path(__file__).resolve().parent

_CONFIG_DIRS = (
    SCRIPT_DIR,
    SCRIPT_DIR.parent,
    SCRIPT_DIR.parent.parent,
)


def get_db_config():
    """Charge la config DB depuis config.postgres.json (même logique que geocode_ban_postgres.py)."""
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


# Chemin vers le fichier CSV Villedereve
CSV_PATH = r"c:\Users\frede\OneDrive\Documents\Cursor\webapp-foncier\scripts\import\communes_france_2025.csv"

# Nom complet de la table cible
TARGET_TABLE = "foncier.ref_communes_new"

# Mapping CSV → colonnes de la table.
# Adapte les clés (noms de colonnes du CSV) si nécessaire.
CSV_TO_DB_COLS = [
    ("code_insee",                        "code_insee"),
    ("nom_standard",                      "nom_standard"),
    ("nom_sans_pronom",                   "nom_sans_pronom"),
    ("nom_a",                             "nom_a"),
    ("nom_de",                            "nom_de"),
    ("nom_sans_accent",                   "nom_sans_accent"),
    ("nom_standard_majuscule",            "nom_standard_majuscule"),
    ("typecom",                           "typecom"),
    ("typecom_texte",                     "typecom_texte"),
    ("reg_code",                          "reg_code"),
    ("reg_nom",                           "reg_nom"),
    ("dep_code",                          "dep_code"),
    ("dep_nom",                           "dep_nom"),
    ("canton_code",                       "canton_code"),
    ("canton_nom",                        "canton_nom"),
    ("epci_code",                         "epci_code"),
    ("epci_nom",                          "epci_nom"),
    ("academie_code",                     "academie_code"),
    ("academie_nom",                      "academie_nom"),
    ("code_postal",                       "code_postal"),
    ("codes_postaux",                     "codes_postaux"),
    ("zone_emploi",                       "zone_emploi"),
    ("code_insee_centre_zone_emploi",     "code_insee_centre_zone_emploi"),
    ("code_unite_urbaine",                "code_unite_urbaine"),
    ("nom_unite_urbaine",                 "nom_unite_urbaine"),
    ("type_commune_unite_urbaine",        "type_commune_unite_urbaine"),
    ("statut_commune_unite_urbaine",      "statut_commune_unite_urbaine"),
    ("population",                        "population"),
    ("superficie_hectare",                "superficie_hectare"),
    ("superficie_km2",                    "superficie_km2"),
    ("densite",                           "densite"),
    ("altitude_moyenne",                  "altitude_moyenne"),
    ("altitude_minimale",                 "altitude_minimale"),
    ("altitude_maximale",                 "altitude_maximale"),
    ("latitude_mairie",                   "latitude_mairie"),
    ("longitude_mairie",                  "longitude_mairie"),
    ("latitude_centre",                   "latitude_centre"),
    ("longitude_centre",                  "longitude_centre"),
    ("grille_densite",                    "grille_densite"),
    ("grille_densite_texte",              "grille_densite_texte"),
    ("niveau_equipements_services",       "niveau_equipements_services"),
    ("niveau_equipements_services_texte", "niveau_equipements_services_texte"),
    ("gentile",                           "gentile"),
    ("url_wikipedia",                     "url_wikipedia"),
    ("url_villedereve",                   "url_villedereve"),
]

def parse_int(value):
    if value is None:
        return None
    v = str(value).strip()
    if not v:
        return None
    try:
        return int(v)
    except ValueError:
        return None

def parse_float(value):
    if value is None:
        return None
    v = str(value).strip().replace(",", ".")
    if not v:
        return None
    try:
        return float(v)
    except ValueError:
        return None

# Colonnes numériques à convertir spécifiquement
INT_COLS = {
    "population",
    "superficie_hectare",
    "altitude_moyenne",
    "altitude_minimale",
    "altitude_maximale",
}
FLOAT_COLS = {
    "superficie_km2",
    "densite",
    "latitude_mairie",
    "longitude_mairie",
    "latitude_centre",
    "longitude_centre",
}

def main():
    # Prépare la liste des colonnes pour l’INSERT
    db_cols = [db for _, db in CSV_TO_DB_COLS]
    placeholders = ", ".join(["%s"] * len(db_cols))
    cols_sql = ", ".join(db_cols)

    insert_sql = f"""
        INSERT INTO {TARGET_TABLE} ({cols_sql})
        VALUES ({placeholders})
        ON CONFLICT (code_insee) DO UPDATE SET
          ({cols_sql}) = (EXCLUDED.{cols_sql.replace(", ", " , EXCLUDED.")});
    """

    conn = psycopg2.connect(**get_db_config())
    conn.autocommit = False

    try:
        with conn.cursor() as cur, open(CSV_PATH, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            batch = []
            batch_size = 2000

            for row in reader:
                values = []
                for csv_col, db_col in CSV_TO_DB_COLS:
                    raw = row.get(csv_col)

                    if db_col in INT_COLS:
                        values.append(parse_int(raw))
                    elif db_col in FLOAT_COLS:
                        values.append(parse_float(raw))
                    else:
                        values.append(raw if raw != "" else None)

                batch.append(values)

                if len(batch) >= batch_size:
                    execute_batch(cur, insert_sql, batch)
                    batch.clear()

            if batch:
                execute_batch(cur, insert_sql, batch)

        conn.commit()
        print("Import ref_communes terminé avec succès.")

    except Exception as e:
        conn.rollback()
        print("Erreur pendant l’import ref_communes :", e)
        raise

    finally:
        conn.close()

if __name__ == "__main__":
    main()