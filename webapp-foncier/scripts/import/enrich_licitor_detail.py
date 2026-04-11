#!/usr/bin/env python3
"""
Enrichissement des annonces Licitor depuis description_longue (sans re-scraper le site).

Parse le champ description_longue stocké en base pour (re-)extraire les champs
structurés : date_vente, tribunal, mise_a_prix, adresse, statut_occupation,
avocat_nom/tel/adresse, et les booléens de dépendances/aménités.

Ce script est complémentaire de scrap_licitor.py --backfill-details :
- backfill-details : re-scrape le site pour remplir description_longue
- enrich_licitor_detail.py : parse description_longue pour remplir les champs structurés

Usage :
    python enrich_licitor_detail.py                  # toutes les lignes avec description_longue
    python enrich_licitor_detail.py --max-rows 500   # limiter à 500 lignes
    python enrich_licitor_detail.py --force           # re-parser même si déjà renseigné
    python enrich_licitor_detail.py --dry-run         # afficher sans modifier
"""

import argparse
import json
import re
import sys
from datetime import date as date_type
from pathlib import Path
from typing import Optional, Tuple

SCRIPT_DIR = Path(__file__).resolve().parent

# ─── Mois français → numéro ────────────────────────────────────────────────
_MOIS_FR = {
    "janvier": 1, "février": 2, "fevrier": 2, "mars": 3, "avril": 4,
    "mai": 5, "juin": 6, "juillet": 7, "août": 8, "aout": 8,
    "septembre": 9, "octobre": 10, "novembre": 11, "décembre": 12, "decembre": 12,
}

_RE_DATE_FR = re.compile(
    r"(?:lundi|mardi|mercredi|jeudi|vendredi|samedi|dimanche)\s+"
    r"(\d{1,2})\s+(\w+)\s+(\d{4})",
    re.I,
)

_RE_DATE_NUM = re.compile(r"(\d{2})[-/](\d{2})[-/](\d{4})")


# ─── Fonctions de parsing du texte description_longue ──────────────────────
def parse_date_vente(text: str) -> Tuple[Optional[date_type], Optional[str]]:
    """Retourne (date, texte_date) depuis le texte."""
    # 1) Format littéral : « mercredi 8 avril 2026 à 9h30 »
    m = _RE_DATE_FR.search(text)
    if m:
        jour = int(m.group(1))
        mois = _MOIS_FR.get(m.group(2).lower())
        annee = int(m.group(3))
        if mois:
            try:
                return date_type(annee, mois, jour), m.group(0)
            except ValueError:
                pass
    # 2) Format numérique : « 08-04-2026 »
    m2 = _RE_DATE_NUM.search(text)
    if m2:
        dd, mm, yyyy = int(m2.group(1)), int(m2.group(2)), int(m2.group(3))
        try:
            return date_type(yyyy, mm, dd), m2.group(0)
        except ValueError:
            pass
    return None, None


def _clean_ws(text: Optional[str]) -> Optional[str]:
    """Normalise les espaces multiples (tabs/newlines internes) en un seul espace."""
    if not text:
        return text
    return re.sub(r"\s+", " ", text).strip()


def parse_tribunal(text: str) -> Optional[str]:
    for line in text.split("\n"):
        line = line.strip()
        if line.lower().startswith("tribunal"):
            return _clean_ws(line)
    return None


def _parse_montant(value) -> Optional[int]:
    if not value:
        return None
    s = str(value).strip()
    for ch in ("€", "\u00a0", " "):
        s = s.replace(ch, "")
    s = s.replace(",", ".")
    cleaned = "".join(c for c in s if c.isdigit() or c == ".")
    if not cleaned:
        return None
    try:
        n = float(cleaned)
    except ValueError:
        return None
    result = int(n) if n.is_integer() else int(n) + 1
    # Vérifier que la valeur est dans les limites BIGINT PostgreSQL
    BIGINT_MAX = 9223372036854775807
    if result > BIGINT_MAX:
        return None
    return result


def parse_mise_a_prix(text: str) -> Optional[int]:
    m = re.search(r"[Mm]ise\s+[aà]\s+prix\s*:\s*([\d\s.,]+)\s*€", text)
    return _parse_montant(m.group(1)) if m else None


_RE_VOIE = re.compile(
    r"^\d+\s*[,.]?\s*(?:rue|avenue|av\.|boulevard|bd\.?|place|chemin|route|"
    r"impasse|allée|passage|cours|quai|square|résidence|cité|hameau|lieu-dit|"
    r"lotissement|voie|sente|sentier)",
    re.I,
)


def parse_adresse(text: str) -> Optional[str]:
    """Cherche une ligne commençant par un numéro + type de voie."""
    for line in text.split("\n"):
        line = line.strip()
        if _RE_VOIE.match(line):
            return line
    return None


def parse_statut_occupation(text: str) -> Optional[str]:
    if re.search(r"\boccup[eé]", text, re.I):
        return "occupé"
    if re.search(r"\b(?:libre|inoccu|vacant)", text, re.I):
        return "libre"
    return None


def parse_avocat(text: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Retourne (nom, tel, adresse) du premier avocat trouvé."""
    nom = None
    m_nom = re.search(r"(Ma[iî]tre\s+[^,\n]+(?:,\s*(?:membre\s+de\s+)?[^,\n]*)?),?\s*[Aa]vocat", text)
    if m_nom:
        nom = m_nom.group(1).strip()

    tel = None
    m_tel = re.search(r"T[eé]l\.?\s*:?\s*([\d\s.]+)", text)
    if m_tel:
        tel = m_tel.group(1).strip()

    adresse = None
    if m_tel:
        # Adresse = lignes entre le nom de l'avocat et le téléphone
        text_before_tel = text[: m_tel.start()]
        lines = [ln.strip() for ln in text_before_tel.split("\n") if ln.strip()]
        addr_candidates = []
        found_avocat = False
        for ln in lines:
            if re.search(r"[Aa]vocat", ln):
                found_avocat = True
                continue
            if found_avocat and not re.match(r"^(?:Pour plus|www\.)", ln):
                addr_candidates.append(ln)
        if addr_candidates:
            adresse = ", ".join(addr_candidates)

    return nom, tel, adresse


def parse_amenities(text: str) -> dict:
    """Détecte les dépendances / aménités mentionnées dans le texte."""
    return {
        "has_cave": bool(re.search(r"\bcave\b", text, re.I)),
        "has_parking_dep": bool(re.search(r"\b(?:parking|stationnement)\b", text, re.I)),
        "has_garage": bool(re.search(r"\bgarage\b", text, re.I)),
        "has_jardin": bool(re.search(r"\bjardin\b", text, re.I)),
        "has_balcon": bool(re.search(r"\bbalcon\b", text, re.I)),
        "has_terrasse": bool(re.search(r"\bterrasse\b", text, re.I)),
    }


def parse_all(text: str) -> dict:
    """Parse description_longue et retourne tous les champs structurés."""
    result = {}
    dv, dv_txt = parse_date_vente(text)
    result["date_vente"] = dv
    result["date_vente_texte"] = dv_txt
    result["tribunal"] = parse_tribunal(text)
    result["mise_a_prix"] = parse_mise_a_prix(text)
    result["adresse"] = parse_adresse(text)
    result["statut_occupation"] = parse_statut_occupation(text)
    nom, tel, addr = parse_avocat(text)
    result["avocat_nom"] = nom
    result["avocat_tel"] = tel
    result["avocat_adresse"] = addr
    result.update(parse_amenities(text))
    return result


# ─── Configuration DB ──────────────────────────────────────────────────────
_CONFIG_DIRS = (SCRIPT_DIR, SCRIPT_DIR.parent, SCRIPT_DIR.parent.parent)


def _get_db_config() -> dict:
    for base in _CONFIG_DIRS:
        p = base / "config.postgres.json"
        if p.is_file():
            with open(p, encoding="utf-8") as f:
                data = json.load(f)
            db = data.get("database") or data
            return {
                "host": db.get("host"),
                "port": int(db.get("port") or 5432),
                "dbname": db.get("database", "foncier"),
                "user": db.get("user"),
                "password": db.get("password") or "",
            }
    raise RuntimeError("config.postgres.json introuvable")


_UPDATE_SQL = """
    UPDATE foncier.licitor_brut SET
        date_vente          = %(date_vente)s,
        date_vente_texte    = COALESCE(%(date_vente_texte)s, date_vente_texte),
        tribunal            = %(tribunal)s,
        mise_a_prix         = %(mise_a_prix)s,
        adresse             = %(adresse)s,
        statut_occupation   = %(statut_occupation)s,
        avocat_nom          = %(avocat_nom)s,
        avocat_tel          = %(avocat_tel)s,
        avocat_adresse      = %(avocat_adresse)s,
        has_cave            = %(has_cave)s,
        has_parking_dep     = %(has_parking_dep)s,
        has_garage          = %(has_garage)s,
        has_jardin          = %(has_jardin)s,
        has_balcon          = %(has_balcon)s,
        has_terrasse        = %(has_terrasse)s
    WHERE id = %(id)s
"""


def main():
    parser = argparse.ArgumentParser(
        description="Parse description_longue des annonces Licitor -> champs structures"
    )
    parser.add_argument("--max-rows", type=int, default=None,
                        help="Nombre max de lignes a traiter")
    parser.add_argument("--force", action="store_true",
                        help="Re-parser meme si les champs sont deja renseignes")
    parser.add_argument("--dry-run", action="store_true",
                        help="Afficher sans modifier la base")
    args = parser.parse_args()

    import psycopg2

    cfg = _get_db_config()
    conn = psycopg2.connect(**cfg)
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            where = "WHERE description_longue IS NOT NULL"
            if not args.force:
                # Ne traiter que les lignes sans tribunal (= pas encore parsées)
                where += " AND tribunal IS NULL"
            limit = f" LIMIT {args.max_rows}" if args.max_rows else ""
            cur.execute(
                f"SELECT id, description_longue FROM foncier.licitor_brut {where} ORDER BY id{limit}"
            )
            rows = cur.fetchall()

        if not rows:
            print("Aucune ligne a traiter.")
            return

        print(f"{len(rows)} lignes a parser...")

        updated = 0
        for i, (row_id, desc_longue) in enumerate(rows, 1):
            parsed = parse_all(desc_longue)
            parsed["id"] = row_id

            if not args.dry_run:
                with conn.cursor() as cur:
                    cur.execute(_UPDATE_SQL, parsed)
                if i % 100 == 0:
                    conn.commit()

            updated += 1
            if i % 200 == 0 or i == len(rows):
                marker = " (dry-run)" if args.dry_run else ""
                print(f"  [{i}/{len(rows)}] {updated} mis a jour{marker}")

        if not args.dry_run:
            conn.commit()

        print(f"\nTermine : {updated} lignes mises a jour.")

    except KeyboardInterrupt:
        conn.commit()
        print("\nInterrompu. Dernieres modifications commitees.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
