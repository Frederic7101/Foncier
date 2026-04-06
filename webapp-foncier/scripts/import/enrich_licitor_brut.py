#!/usr/bin/env python3
"""
Enrichissement de foncier.licitor_brut : parse desc_courte et renseigne
type_local, surf_bati, surf_non_bati pour chaque ligne.

Exécuter après alter_licitor_brut_add_parsed_fields.sql :
  python enrich_licitor_brut.py [--force]

Options :
  --force  Réanalyse même les lignes déjà traitées (type_local IS NOT NULL)
"""

import json
import re
import sys
import unicodedata
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_batch, RealDictCursor

SCRIPT_DIR = Path(__file__).resolve().parent
_CONFIG_DIRS = (SCRIPT_DIR, SCRIPT_DIR.parent, SCRIPT_DIR.parent.parent)

BATCH_SIZE = 500

# ─────────────────────────────────────────────
# Config DB (identique aux autres scripts import)
# ─────────────────────────────────────────────

def get_db_config():
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
    raise RuntimeError("config.postgres.json introuvable.")


# ─────────────────────────────────────────────
# Normalisation texte
# ─────────────────────────────────────────────

def _norm(s: str) -> str:
    """Minuscules + suppression accents + collapsage espaces."""
    if not s:
        return ""
    s = s.lower()
    s = "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")
    return re.sub(r"\s+", " ", s).strip()


# ─────────────────────────────────────────────
# Détection du type de local
# ─────────────────────────────────────────────

# Ordre des vérifications : du plus spécifique au plus générique
_TYPE_RULES = [
    # (type_local, pattern à tester sur la tête normalisée sans article)
    # 1. Local industriel/commercial — AVANT "local" générique
    ("local_indus_comm", re.compile(
        r"^(local\s+(commercial|industriel|a\s+usage|d.activit)"
        r"|surface\s+commerciale"
        r"|batiment\s+(artisanal|a\s+usage\s+(industriel|commercial|professionnel))"
        r"|atelier"
        r"|hangar"
        r"|entrepot\b"  # entrepôt en tête = local_indus_comm (ex: "Un entrepôt de 874 m²")
        r"|boutique"
        r"|commerce\b)"
    )),
    # 2. Immeuble
    ("immeuble", re.compile(r"^(immeuble|ensemble\s+immobilier)")),
    # 3. Terrain
    ("terrain", re.compile(r"^(terrain|parcelle(\s+de\s+terrain)?|terres?\b)")),
    # 4. Parking / garage
    ("parking", re.compile(
        r"^(parking|garage|box\b"
        r"|emplacement\s+de\s+(stationnement|parking)"
        r"|place\s+de\s+(stationnement|parking))"
    )),
    # 5. Dépendance (entrepôt après, cave, débarras, local générique, grange)
    ("dependance", re.compile(r"^(cave|dependance|debarras|entrepot|grange|local\b)")),
    # 6. Maison
    ("maison", re.compile(r"^(maison|pavillon|villa\b)")),
    # 7. Appartement
    ("appartement", re.compile(
        r"^(appartement|chambre|studio|logement|studette|piece\b)"
    )),
]

# Article initial à supprimer
_ARTICLE_RE = re.compile(
    r"^(un|une|des|deux|trois|quatre|cinq|six|sept|huit|neuf|dix|\d+)\s+"
)

# Cas particuliers : pluriel "Parkings 1 à 6 ..."
_PLURAL_PREFIX_RE = re.compile(r"^parkings?\b")


def detect_type_local(desc: str) -> str:
    """Détermine le type de local à partir des premiers mots de desc_courte."""
    if not desc or not desc.strip():
        return "autre"

    n = _norm(desc)

    # Cas pluriel explicite (ex: "Parkings 1 à 6")
    if _PLURAL_PREFIX_RE.match(n):
        return "parking"

    # Retirer l'article de tête
    m = _ARTICLE_RE.match(n)
    head = n[m.end():] if m else n

    # 1er passage : le type est le premier mot du head
    for type_local, pat in _TYPE_RULES:
        if pat.match(head):
            return type_local

    # 2e passage : un adjectif qualificatif précède le type (ex: "Un important terrain")
    parts = head.split(None, 1)
    if len(parts) == 2:
        head2 = parts[1]
        for type_local, pat in _TYPE_RULES:
            if pat.match(head2):
                return type_local

    return "autre"


# ─────────────────────────────────────────────
# Parsing des surfaces
# ─────────────────────────────────────────────

# m² sous différentes formes (m², m2, m°, etc.)
_M2 = r"m[²2\xb2\xba°]"


def _parse_float_fr(s: str):
    """Convertit une chaîne numérique française en float.

    '74,31' → 74.31   (virgule décimale)
    '5.000' → 5000.0  (point séparateur milliers)
    '5 000' → 5000.0  (espace séparateur milliers)
    """
    s = s.strip().replace("\u00a0", "").replace(" ", "")
    if "," in s:
        # Virgule = décimale ; supprimer les points (milliers)
        s = s.replace(".", "").replace(",", ".")
    elif re.match(r"^\d{1,3}(\.\d{3})+$", s):
        # Points séparateurs de milliers
        s = s.replace(".", "")
    try:
        v = float(s)
        return v if v > 0 else None
    except ValueError:
        return None


# ── Surface bâtie ──────────────────────────────

# Patterns explicites (priorité décroissante)
_SURF_BATI_PATTERNS = [
    # d'une superficie/surface [privative] [Loi Carrez] [habitable] [mesurée] de X m²
    re.compile(
        r"d[u']une\s+(?:superficie|surface)\s+"
        r"(?:privative\s+)?(?:loi\s+carrez\s+)?(?:habitable\s+)?(?:mesuree\s+)?de\s+"
        r"([\d.,]+)\s*" + _M2, re.I
    ),
    # superficie [privative] [Loi Carrez] de X m²
    re.compile(
        r"superficie\s+(?:privative\s+)?(?:loi\s+carrez\s+)?de\s+([\d.,]+)\s*" + _M2, re.I
    ),
    # surface habitable [de] X m²
    re.compile(r"surface\s+habitable\s+(?:de\s+)?([\d.,]+)\s*" + _M2, re.I),
    # surface au sol [totale :] X m²
    re.compile(r"surface\s+au\s+sol\s+(?:totale?\s*:?\s*)?([\d.,]+)\s*" + _M2, re.I),
    # Loi Carrez : X m²  (nombre après le label)
    re.compile(r"loi\s+carrez\s*:?\s*([\d.,]+)\s*" + _M2, re.I),
    # X m² Loi Carrez  OU  X Loi Carrez (nombre avant le label)
    re.compile(r"([\d.,]+)\s*(?:" + _M2 + r"\s+)?loi\s+carrez", re.I),
]

# Contextes terrain à exclure du fallback "de X m²"
# Détecte la présence de "terrain" (ou "cadastr", "contenance") dans le contexte.
# Le code teste aussi si un mot de bâtiment s'intercale (ex: "terrain - bâtiment de 33,51 m²")
# pour NE PAS exclure la surface du bâtiment édifié sur la parcelle.
_TERRAIN_CONTEXT_RE = re.compile(r"\bterrain\b|cadastr|contenance")


def detect_surf_bati(desc: str):
    """Retourne la surface bâtie en m² (float) ou None."""
    if not desc:
        return None
    n = _norm(desc)

    # 1. Patterns explicites
    for pat in _SURF_BATI_PATTERNS:
        m = pat.search(n)
        if m:
            v = _parse_float_fr(m.group(1))
            if v:
                return v

    # 2. Fallback : premier « de X m² » hors contexte terrain
    # Élargir le contexte à 60 chars pour distinguer :
    #   "terrain - de X m²"         → terrain pur, skip sauf si un bâtiment s'intercale
    #   "terrain - bâtiment de X m²"→ bâtiment sur terrain, garder
    _BUILDING_KW = re.compile(r"\b(batiment|edifice|construc|hangar|maison|logement|atelier|entrepot)\b")
    for m in re.finditer(r"(?<!\w)de\s+([\d.,]+)\s*" + _M2, n, re.I):
        start = max(0, m.start() - 60)
        context = n[start:m.start()]
        if _TERRAIN_CONTEXT_RE.search(context):
            # Accepter quand même si un mot de bâtiment s'intercale entre "terrain" et "de X m²"
            if not _BUILDING_KW.search(context):
                continue
        v = _parse_float_fr(m.group(1))
        if v:
            return v

    return None


# ── Surface non bâtie (terrain) ───────────────

def _extract_ha_a_ca(segment: str):
    """Extrait ha/a/ca d'un segment textuel et retourne la valeur en m² (float) ou None."""
    n = _norm(segment)
    ha = a = ca = 0

    m = re.search(r"(?<!\w)(\d+)\s*ha\b", n)
    if m:
        ha = int(m.group(1))

    # 'a' : doit être précédé d'un chiffre sans 'h' immédiatement devant
    for am in re.finditer(r"(?<!\w)(\d+)\s*a\b", n):
        # Vérifier que ce n'est pas 'ha' : regarder le char juste avant le digit
        before_pos = am.start()
        if before_pos > 0 and n[before_pos - 1] == "h":
            continue
        a = int(am.group(1))
        break

    m = re.search(r"(?<!\w)(\d+)\s*ca\b", n)
    if m:
        ca = int(m.group(1))

    if ha or a or ca:
        return float(ha * 10000 + a * 100 + ca)
    return None


# Marqueurs de surface cadastrale / terrain
_TERRAIN_SURF_MARKERS = re.compile(
    r"(pour\s+(?:une\s+)?(?:contenance\s+(?:de|totale)?\s+)?"
    r"|cadastr[ea]\w*\b[^.]{0,80}pour\s+"
    r"|terrain\s+(?:approximatif\w*\s+)?de\s+"
    r"|sur\s+(?:un\s+)?terrain\s+de\s+"
    r"|superficie\s+(?:du\s+)?terrain\s+(?:de\s+)?)",
    re.I,
)

# m² terrain (avec séparateur milliers éventuel)
# "terrain de X m²", "terrain superficie X m²", "terrain approximatif de X m²", etc.
_TERRAIN_M2_RE = re.compile(
    r"terrain\s+(?:[a-z ]{0,40}(?:de\s+|d.une\s+superficie\s+(?:de\s+)?)?)?([\d., ]+)\s*" + _M2, re.I
)
_SUR_TERRAIN_M2_RE = re.compile(
    r"sur\s+(?:un\s+)?terrain\s+(?:[a-z ]{0,20}(?:de\s+)?)?([\d., ]+)\s*" + _M2, re.I
)


def detect_surf_non_bati(desc: str):
    """Retourne la surface non bâtie (terrain) en m² (float) ou None."""
    if not desc:
        return None
    n = _norm(desc)

    # Motif ha/a/ca réutilisé
    _HAC = (
        r"((?:\d+\s*ha\s*)?(?:\d+\s*a\s+(?:et\s+)?)?\d+\s*ca\b"
        r"|(?:\d+\s*ha\s*)?\d+\s*a\b"
        r"|\d+\s*ha\b)"
    )

    # 1a. « pour [une contenance de] [Xha] [Xa] [Xca] »
    for m in re.finditer(r"pour\s+(?:une\s+)?(?:contenance\s+de\s+)?" + _HAC, n, re.I):
        v = _extract_ha_a_ca(m.group(1))
        if v:
            return v

    # 1b. « contenance [de] [Xha] [Xa] [Xca] » (sans "pour" devant)
    for m in re.finditer(r"contenance\s+(?:de\s+|totale\s+de\s+)?" + _HAC, n, re.I):
        v = _extract_ha_a_ca(m.group(1))
        if v:
            return v

    # 2. « terrain de [Xha] [Xa] [Xca] » (sans m²)
    for m in re.finditer(r"terrain\s+(?:[a-z]{0,15}\s+)?de\s+" + _HAC, n, re.I):
        v = _extract_ha_a_ca(m.group(1))
        if v:
            return v

    # 3. « terrain de X m² » ou « sur un terrain de X m² »
    for pat in (_SUR_TERRAIN_M2_RE, _TERRAIN_M2_RE):
        m = pat.search(n)
        if m:
            v = _parse_float_fr(m.group(1))
            if v:
                return v

    # 4. « de Xha Xa Xca » (généralement après "cadastré" ou dans desc terrain)
    if "cadastr" in n or ("terrain" in n and "m" not in n[n.find("terrain"):n.find("terrain") + 40]):
        m = re.search(r"(?:pour|de)\s+" + _HAC, n, re.I)
        if m:
            v = _extract_ha_a_ca(m.group(1))
            if v:
                return v

    # 5. Filet de sécurité : "terrain" présent + surface en m² sans marqueur explicite
    #    ex. "Un important terrain - de 5.067 m²" ou "Un terrain - de 270 m²"
    #    Ne pas utiliser si un bâtiment s'intercale entre terrain et la valeur.
    if "terrain" in n:
        _BLD = re.compile(r"\b(batiment|edifice|constru|hangar|entrepot|maison|logement)\b")
        for m in re.finditer(r"(?<!\w)de\s+([\d.,]+)\s*" + _M2, n, re.I):
            start = max(0, m.start() - 70)
            ctx = n[start:m.start()]
            if _BLD.search(ctx):
                continue  # bâtiment entre terrain et la valeur → c'est surf_bati, pas non_bati
            v = _parse_float_fr(m.group(1))
            if v:
                return v

    return None


# ─────────────────────────────────────────────
# Main : mise à jour de la base
# ─────────────────────────────────────────────

def main():
    force = "--force" in sys.argv

    cfg = get_db_config()
    conn = psycopg2.connect(**cfg)
    conn.autocommit = False

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SET search_path TO foncier, public")

            # Vérifier que les colonnes existent
            cur.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = 'foncier' AND table_name = 'licitor_brut' "
                "AND column_name IN ('type_local', 'surf_bati', 'surf_non_bati')"
            )
            existing = {r["column_name"] for r in cur.fetchall()}
            missing = {"type_local", "surf_bati", "surf_non_bati"} - existing
            if missing:
                print(f"Colonnes manquantes : {missing}")
                print("Exécuter d'abord : alter_licitor_brut_add_parsed_fields.sql")
                return

            # Sélectionner les lignes à traiter
            where = "" if force else " WHERE type_local IS NULL"
            cur.execute(f"SELECT id, desc_courte FROM foncier.licitor_brut{where} ORDER BY id")
            rows = cur.fetchall()

        if not rows:
            print("Aucune ligne à traiter.")
            return

        print(f"{len(rows)} ligne(s) à analyser{'  (--force : toutes)' if force else ''}...")

        # Préparer les mises à jour
        updates = []
        stats: dict = {"appartement": 0, "maison": 0, "parking": 0, "dependance": 0,
                       "local_indus_comm": 0, "terrain": 0, "immeuble": 0, "autre": 0}

        for row in rows:
            rid = row["id"]
            desc = row["desc_courte"] or ""

            tl = detect_type_local(desc)
            sb = detect_surf_bati(desc)
            snb = detect_surf_non_bati(desc)

            updates.append((tl, sb, snb, rid))
            stats[tl] = stats.get(tl, 0) + 1

        # Mise à jour en base par lots
        update_sql = """
            UPDATE foncier.licitor_brut
               SET type_local    = %s,
                   surf_bati     = %s,
                   surf_non_bati = %s
             WHERE id = %s
        """
        with conn.cursor() as cur:
            cur.execute("SET search_path TO foncier, public")
            execute_batch(cur, update_sql, updates, page_size=BATCH_SIZE)

        conn.commit()

        print(f"Mise à jour terminée : {len(updates)} ligne(s).")
        print("Répartition par type_local :")
        for k, v in sorted(stats.items(), key=lambda x: -x[1]):
            if v:
                print(f"  {k:<20} {v:>6}")

    except Exception as e:
        conn.rollback()
        print("Erreur :", e)
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
