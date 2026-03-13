#!/usr/bin/env python3
"""
Import des données brutes Licitor (CSV) dans foncier.licitor_brut.

Réentrant : on peut relancer pour une même source_region ou ajouter d'autres
régions en appelant le script avec un autre fichier et un autre source_region.
Les doublons sont évités via une contrainte d'unicité sur url_annonce et un
INSERT ... ON CONFLICT DO NOTHING (une annonce déjà présente est simplement ignorée).

Le CSV n'a pas de ligne d'en-tête. Ordre des colonnes (déduit du fichier Île-de-France) :
  0: url_annonce, 1: code_dept, 2: commune, 3: desc_courte, 4: montant_adjudication,
  5: date_vente_texte, 6: reserve (actuellement vide), 7: date_scraping

Usage :
  python import_licitor_brut.py [chemin_csv] [source_region]
  Ex. python import_licitor_brut.py encheres_historique_ile_de_france.csv ile_de_france

Prérequis : créer la table avec webapp-foncier/sql/postgresql/create_licitor_brut.sql
  psql -U postgres -d foncier -f webapp-foncier/sql/postgresql/create_licitor_brut.sql
"""

import csv
import json
import os
import sys
import unicodedata
from datetime import datetime
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_batch

# Répertoire du script (pour chemins relatifs)
SCRIPT_DIR = Path(__file__).resolve().parent

# Dossiers où chercher config.postgres.json (scripts/, webapp-foncier/, racine projet)
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
    # Pas de config.postgres.json : on échoue explicitement plutôt que de tomber sur des valeurs par défaut implicites.
    raise RuntimeError("Aucun fichier config.postgres.json trouvé pour la configuration PostgreSQL.")

# Colonnes de la table (sans id, sans date_import) dans l'ordre du CSV + source_region en 1er
INSERT_COLS = [
    "source_region",
    "url_annonce",
    "code_dept",
    "commune",
    "desc_courte",
    "montant_adjudication",
    "date_vente_texte",
    "date_scraping",
]

def _normalize_region_key(s: str) -> str:
    """Normalise un libellé de région pour comparaison (minuscule, sans accents, _ et espaces → -)."""
    if not s:
        return ""
    s = s.strip().lower().replace("_", "-").replace(" ", "-")
    # enlever les accents
    s = "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")
    # réduire les doubles tirets
    while "--" in s:
        s = s.replace("--", "-")
    return s


def _parse_montant(value, currency: str = "€"):
    """Convertit '374 000 €' en entier (374000). Si décimal, arrondit à l'unité supérieure."""
    if not value:
        return None
    s = str(value).strip()
    if currency:
        s = s.replace(currency, "")
    s = s.replace("\u00a0", " ")  # espace insécable éventuel
    s = s.replace(" ", "")
    s = s.replace(",", ".")
    # Garder chiffres et éventuel point
    cleaned = "".join(ch for ch in s if (ch.isdigit() or ch == "."))
    if not cleaned:
        return None
    try:
        num = float(cleaned)
    except ValueError:
        return None
    # Arrondi à l'unité supérieure
    return int(num) if num.is_integer() else int(num) + 1


# Formats reconnus pour date_scraping (colonne 8)
def parse_date_scraping(value):
    if not value or not str(value).strip():
        return None
    s = str(value).strip()[:26]  # ex. 2026-02-22 18:46:06.208068
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def load_csv_rows(csv_path: Path, source_region_label: str, existing_keys, existing_keys_db):
    """
    Lit le CSV (sans en-tête) et produit :
      - rows_to_insert : tuples (source_region, url, code_dept, ...)
      - rejected_rows : lignes rejetées (erreur de parsing)
      - stats : dict avec inserted (prévu), already_existing, rejected
    """
    rows_to_insert = []
    rejected_rows = []
    existing_rows = []
    already_existing = 0
    rejected = 0

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        for raw_row in reader:
            row = list(raw_row)
            if len(row) < 8:
                # Ligne incomplète : on pad à 8
                row = (row + [""] * 8)[:8]
            else:
                row = row[:8]

            url_annonce_raw = (row[0] or "").strip()
            url_annonce = url_annonce_raw or None

            code_dept = (row[1] or "").strip() or None
            commune = (row[2] or "").strip() or None
            desc_courte = (row[3] or "").strip() or None

            raw_montant = (row[4] or "").strip()
            montant_adjudication = _parse_montant(raw_montant or None)
            # Si un montant non vide ne peut pas être parsé, on rejette la ligne
            if raw_montant and montant_adjudication is None:
                rejected += 1
                rejected_rows.append(list(row))
                continue

            key = (url_annonce, desc_courte, montant_adjudication)

            # Détection doublon : même triplet (url, desc_courte, montant) déjà en base ou déjà vu dans ce fichier
            if key in existing_keys:
                already_existing += 1
                # On écrit toutes les lignes doublons (en base OU intra-fichier) dans le .exist.csv
                existing_rows.append(list(row))
                continue

            date_vente_texte = (row[5] or "").strip() or None
            # row[6] = reserve (non utilisée car vide dans les fichiers actuels)
            date_scraping = parse_date_scraping(row[7])

            rows_to_insert.append((
                source_region_label,
                url_annonce,
                code_dept,
                commune,
                desc_courte,
                montant_adjudication,
                date_vente_texte,
                date_scraping,
            ))

            # On ajoute la clé complète à l'ensemble pour éviter les doublons intra-fichier
            existing_keys.add(key)

    stats = {
        "to_insert": len(rows_to_insert),
        "already_existing": already_existing,
        "rejected": rejected,
    }
    return rows_to_insert, rejected_rows, existing_rows, stats


def main():
    if len(sys.argv) >= 3:
        csv_path = Path(sys.argv[1])
        raw_region = sys.argv[2].strip()
    else:
        csv_path = SCRIPT_DIR / "encheres_historique_ile_de_france.csv"
        raw_region = "ile_de_france"

    if not csv_path.is_absolute():
        csv_path = SCRIPT_DIR / csv_path
    if not csv_path.exists():
        print(f"Fichier introuvable : {csv_path}")
        sys.exit(1)

    # Déterminer un identifiant de région à partir du nom de fichier si possible
    stem = csv_path.stem
    if "encheres_historique_" in stem:
        slug_region = stem.split("encheres_historique_", 1)[1]
    else:
        slug_region = stem
    slug_region = slug_region or raw_region

    cfg = get_db_config()
    conn = psycopg2.connect(**cfg)
    conn.autocommit = False

    # Tenter de mapper ce slug sur un nom de région existant dans foncier.ref_regions
    source_region_label = slug_region
    try:
        with conn.cursor() as cur:
            cur.execute("SET search_path TO foncier, public")
            cur.execute("SELECT nom_region FROM ref_regions")
            rows_regions = cur.fetchall()
        slug_norm = _normalize_region_key(slug_region)
        for (nom_region,) in rows_regions:
            if _normalize_region_key(nom_region) == slug_norm:
                source_region_label = nom_region
                break
    except Exception:
        # En cas de problème (table manquante, etc.), on garde le slug brut
        pass

    try:
        with conn.cursor() as cur:
            # Charger les triplets (url, desc_courte, montant) déjà présents pour éviter les doublons
            cur.execute("SET search_path TO foncier, public")
            cur.execute("SELECT url_annonce, desc_courte, montant_adjudication FROM licitor_brut")
            existing_keys_db = {(r[0], r[1], r[2]) for r in cur.fetchall()}
            existing_keys = set(existing_keys_db)

            print(f"Lecture de {csv_path} (source_region={source_region_label})...")
            rows_to_insert, rejected_rows, existing_rows, stats = load_csv_rows(
                csv_path, source_region_label, existing_keys, existing_keys_db
            )
            total_read = stats["to_insert"] + stats["already_existing"] + stats["rejected"]
            print(f"  {total_read} lignes lues : {stats['to_insert']} à insérer, "
                  f"{stats['already_existing']} déjà existantes, {stats['rejected']} rejetées.")

            # Écrire les rejets éventuels dans un fichier .<timestamp>.rejets.csv
            if rejected_rows:
                ts = datetime.now().strftime("%Y%m%d%H%M%S")
                reject_path = csv_path.with_name(f"{csv_path.stem}.{ts}.rejets.csv")
                with open(reject_path, "w", newline="", encoding="utf-8") as rej_f:
                    writer = csv.writer(rej_f)
                    for r in rejected_rows:
                        writer.writerow(r)
                print(f"  {len(rejected_rows)} lignes rejetées écrites dans {reject_path}")

            # Écrire les lignes déjà existantes dans un fichier .<timestamp>.exist.csv
            if existing_rows:
                ts = datetime.now().strftime("%Y%m%d%H%M%S")
                exist_path = csv_path.with_name(f"{csv_path.stem}.{ts}.exist.csv")
                with open(exist_path, "w", newline="", encoding="utf-8") as exist_f:
                    writer = csv.writer(exist_f)
                    for r in existing_rows:
                        writer.writerow(r)
                print(f"  {len(existing_rows)} lignes déjà existantes écrites dans {exist_path}")

            if not rows_to_insert:
                print("Aucune nouvelle ligne à insérer.")
                conn.commit()
                print("Import terminé (0 insertion).")
                return

            placeholders = ", ".join(["%s"] * len(INSERT_COLS))
            cols_sql = ", ".join(INSERT_COLS)
            insert_sql = f"""
                INSERT INTO foncier.licitor_brut ({cols_sql})
                VALUES ({placeholders})
                ON CONFLICT (url_annonce, desc_courte, montant_adjudication) DO NOTHING
            """
            execute_batch(cur, insert_sql, rows_to_insert, page_size=1000)

        conn.commit()
        print(f"Import terminé : {len(rows_to_insert)} ligne(s) réellement insérée(s) dans foncier.licitor_brut.")
    except Exception as e:
        conn.rollback()
        print("Erreur pendant l'import :", e)
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
