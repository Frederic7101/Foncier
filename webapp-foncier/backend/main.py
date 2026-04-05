import csv
import hashlib
import json
import math
import os
import re
import threading
import time as _time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from datetime import date, datetime
from decimal import Decimal
from typing import Optional, List, Any, Literal, Tuple, Dict, Set

import psycopg2
from psycopg2.extras import RealDictCursor, Json
from fastapi import FastAPI, Query, HTTPException, Response, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv


load_dotenv()

# Mode debug : logs [fiche], [indicators], [refresh-indicateurs], [stats] uniquement si DEBUG=1 (ou true/yes)
DEBUG = os.environ.get("DEBUG", "").strip().lower() in ("1", "true", "yes")
IGN_WMTS_URL = (
    "https://data.geopf.fr/wmts?SERVICE=WMTS&REQUEST=GetTile&VERSION=1.0.0"
    "&LAYER=GEOGRAPHICALGRIDSYSTEMS.PLANIGNV2&STYLE=normal&TILEMATRIXSET=PM"
    "&TILEMATRIX={z}&TILEROW={y}&TILECOL={x}&FORMAT=image/png"
)
IGN_TILE_CACHE_DIR = Path(__file__).resolve().parent.parent / "frontend" / "data" / "carto" / "ign_tiles"


# Fichier de log horodaté (créé au démarrage si DEBUG et LOG_TO_FILE=1)
_debug_log_file = None

def _init_debug_log_file() -> None:
    global _debug_log_file
    if _debug_log_file is not None:
        return
    if not DEBUG or not os.environ.get("LOG_TO_FILE", "").strip().lower() in ("1", "true", "yes"):
        return
    log_dir = Path(__file__).resolve().parent / "logs"
    log_dir.mkdir(exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = log_dir / f"debug_{ts}.log"
    try:
        _debug_log_file = open(log_path, "a", encoding="utf-8")
        _debug_log_file.write(f"# Log started {datetime.now().isoformat()}\n")
        _debug_log_file.flush()
    except OSError:
        _debug_log_file = None

def _debug_log(msg: str, *args: Any, **kwargs: Any) -> None:
    """Affiche un log uniquement si DEBUG est activé (env DEBUG=1). Optionnellement écrit dans un fichier horodaté si LOG_TO_FILE=1."""
    if not DEBUG:
        return
    text = (msg % args) if args else msg
    print(text, **kwargs)
    _init_debug_log_file()
    if _debug_log_file is not None:
        try:
            _debug_log_file.write(text + "\n")
            _debug_log_file.flush()
        except OSError:
            pass


# =============================================================================
# Variables globales / Config par défaut — modifier ici ou via config / env
# =============================================================================

# Fichiers de config recherchés (dans l’ordre) dans backend/, webapp-foncier/, racine.
# Règle commune à tous les scripts : paramètres DB dans config.postgres.json.
CONFIG_FILENAMES = ("config.postgres.json",)

# Valeurs par défaut base de données (surchargées par config.json ou variables d’environnement)
DEFAULT_DB_HOST = "localhost"
DEFAULT_DB_PORT = 5432
DEFAULT_DB_USER = "postgres"
DEFAULT_DB_PASSWORD = ""
DEFAULT_DB_NAME = "foncier"
DEFAULT_DB_SCHEMA = "foncier"

# Variables d’environnement pour la DB (priorité après les fichiers de config)
ENV_DB_HOST = "DB_HOST"
ENV_DB_PORT = "DB_PORT"
ENV_DB_USER = "DB_USER"
ENV_DB_PASSWORD = "DB_PASSWORD"
ENV_DB_NAME = "DB_NAME"
ENV_DB_SCHEMA = "DB_SCHEMA"

# Dossiers où chercher config (backend/, webapp-foncier/, racine projet)
_CONFIG_DIRS = (
    Path(__file__).resolve().parent,
    Path(__file__).resolve().parent.parent,
    Path(__file__).resolve().parent.parent.parent,
)


def _load_db_config() -> dict:
    """Charge la config DB depuis config.postgres.json (règle commune à tous les scripts)."""
    for base in _CONFIG_DIRS:
        for name in CONFIG_FILENAMES:
            config_path = base / name
            if config_path.is_file():
                with open(config_path, encoding="utf-8") as f:
                    data = json.load(f)
                db = data.get("database") or data
                return {
                    "host": db.get("host", DEFAULT_DB_HOST),
                    "port": int(db.get("port") or DEFAULT_DB_PORT),
                    "user": db.get("user", DEFAULT_DB_USER),
                    "password": db.get("password", DEFAULT_DB_PASSWORD),
                    "database": db.get("database", DEFAULT_DB_NAME),
                    "schema": db.get("schema", DEFAULT_DB_SCHEMA),
                }
    # Pas de fichier de config trouvé : on échoue explicitement plutôt que de basculer
    # silencieusement sur des variables d'environnement.
    raise RuntimeError("Aucun fichier config.postgres.json trouvé pour la configuration PostgreSQL.")


# Caractères apostrophe / guillemet à normaliser ou supprimer pour forme canonique
_APOSTROPHE_VARIANTS = "\u2019\u02bc\u02b9\u2032"  # RIGHT SINGLE QUOTATION MARK, MODIFIER LETTER APOSTROPHE, etc.


def _normalize_name_canonical(s: Optional[str]) -> str:
    """Forme canonique pour comparaison de noms (communes, etc.) : uniquement lettres A-Z majuscules.
    Aligné avec le frontend stats.js normalizeNameCanonical (comparaisons table / variables).
    Même logique que la SQL _sql_norm_name_canonical : désaccentuer, puis ne garder que A-Z.
    Supprime espaces, apostrophes, traits d'union, parenthèses finales, tout caractère hors A-Z."""
    if s is None:
        return ""
    s = str(s).strip()
    if not s:
        return ""
    # Supprimer les parenthèses finales et leur contenu : (le), (la), (les), (l'), (lès), etc.
    s = re.sub(r"\s*\([^)]*\)\s*$", "", s, flags=re.IGNORECASE).strip()
    # Supprimer les variantes d'apostrophe
    for c in _APOSTROPHE_VARIANTS:
        s = s.replace(c, "")
    s = s.replace("'", "")
    # Désaccentuer avant de filtrer : NFD + retirer les marques combinantes (é→e, ç→c, etc.)
    nfd = unicodedata.normalize("NFD", s)
    sans_accent = "".join(c for c in nfd if unicodedata.category(c) != "Mn")
    # Ne garder que les lettres (A-Z après désaccentuation)
    s = re.sub(r"[^A-Za-z]", "", sans_accent)
    return s.upper()


# Aligné sur comparaison_scores.html normalizeCommuneNameForMapMatch (cartes / rapprochement IGN vs ref).
_COMMUNE_MAP_STOPWORDS = frozenset(
    {
        "le",
        "la",
        "les",
        "de",
        "du",
        "des",
        "en",
        "sur",
        "sous",
        "lès",
        "lè",
        "au",
        "aux",
        "à",
        "un",
        "une",
        "d",
        "l",
        "chez",
        "devant",
        "entre",
        "et",
        "ou",
        "dans",
        "par",
        "pour",
    }
)


def _ascii_fold_lower(w: str) -> str:
    if not w:
        return ""
    nfd = unicodedata.normalize("NFD", w)
    sans = "".join(c for c in nfd if unicodedata.category(c) != "Mn")
    return sans.lower()


def _expand_commune_abbrev_token(word: str) -> str:
    w = (word or "").strip()
    if not w:
        return w
    base = _ascii_fold_lower(w).rstrip(".")
    if base == "st":
        return "saint"
    if base == "ste":
        return "sainte"
    if base == "stes":
        return "saintes"
    if base == "ss":
        return "saints"
    return w


def _normalize_commune_name_for_map_match(name: Optional[str]) -> str:
    """Clé de correspondance commune (API ↔ ref ↔ vf ↔ GeoJSON) : stopwords, abréviations St/Ste, puis _normalize_name_canonical."""
    if name is None:
        return ""
    s = str(name).strip()
    if not s:
        return ""
    while re.search(r"\s*\([^)]*\)\s*$", s):
        s = re.sub(r"\s*\([^)]*\)\s*$", "", s, flags=re.IGNORECASE).strip()
    s = re.sub(r"[-–—]", " ", s)
    for c in _APOSTROPHE_VARIANTS:
        s = s.replace(c, " ")
    s = s.replace("'", " ")
    raw_tokens = [t for t in s.split() if t]
    expanded = [_expand_commune_abbrev_token(t) for t in raw_tokens]
    filtered: List[str] = []
    k = 0
    while k < len(expanded):
        tk = _ascii_fold_lower(expanded[k]).rstrip(".")
        if k + 1 < len(expanded) and tk == "de":
            nxt = _ascii_fold_lower(expanded[k + 1]).rstrip(".")
            if nxt in ("la", "l", "les"):
                k += 2
                continue
        if tk in _COMMUNE_MAP_STOPWORDS:
            k += 1
            continue
        filtered.append(expanded[k])
        k += 1
    joined = " ".join(filtered).strip()
    key = _normalize_name_canonical(joined)
    if len(key) >= 2:
        return key
    return _normalize_name_canonical(name)


def _debug_sql_params(sql: str, params: List[Any]) -> str:
    """Construit une SQL exécutable pour les logs (chaînes en guillemets simples, apostrophe échappée)."""
    parts = []
    i = 0
    for token in sql.split("%s"):
        parts.append(token)
        if i < len(params):
            p = params[i]
            if isinstance(p, str):
                escaped = p.replace("'", "''")
                parts.append("'" + escaped + "'")
            elif isinstance(p, (int, float)) and not isinstance(p, bool):
                parts.append(str(p))
            elif isinstance(p, (list, tuple)):
                parts.append(", ".join("'" + str(x).replace("'", "''") + "'" if isinstance(x, str) else str(x) for x in p))
            else:
                parts.append(repr(p))
            i += 1
    return "".join(parts)


def _sql_norm_name(column_sql: str) -> str:
    """Expression SQL pour normaliser un nom (majuscules, sans accent, apostrophes unifiées).
    Utilise U&'\\XXXX' pour les caractères Unicode (apostrophe typographique, etc.)."""
    return (
        "REPLACE(REPLACE(REPLACE(REPLACE(UPPER(unaccent(TRIM(" + column_sql + "))), "
        "U&'\\2019', ''''), U&'\\02bc', ''''), U&'\\02b9', ''''), U&'\\2032', '''')"
    )


def _normalize_code_dept_for_vf(code_dept: Optional[str]) -> str:
    """Pour les requêtes vers vf_communes : code_dept est utilisé tel quel (ex. 01, 02, …).
    La table peut avoir 2 chiffres pour 01-09."""
    if not code_dept:
        return ""
    return str(code_dept).strip()


def _normalize_code_postal_for_vf(code_postal: Optional[str]) -> str:
    """Pour les requêtes vers vf_communes : code_postal y est stocké sans zéros en tête
    (ex. 01500 → "1500"). On enlève les zéros en tête pour matcher."""
    if not code_postal:
        return ""
    s = str(code_postal).strip()
    if not s:
        return s
    if s.isdigit():
        return s.lstrip("0") or "0"  # "01500" → "1500", "00000" → "0"
    return s

def _normalize_code_postal_for_ref_communes(code_postal: Optional[str]) -> str:
    """Pour les requêtes vers ref_communes : code_postal y est stocké avec zéro en tête s'il ne contient que 4 chiffres
    (ex. 1500 → "01500"). On ajoute un zéro en tête pour matcher."""
    if not code_postal:
        return ""
    s = str(code_postal).strip()
    if not s:
        return s
    if s.isdigit():
        return s.zfill(5)
    return s

def _sql_norm_name_canonical(column_sql: str) -> str:
    """Expression SQL : forme canonique pour comparaison de noms (communes).
    Même logique que Python : désaccentuer d'abord (unaccent), puis ne garder que A-Z."""
    # 1) TRIM 2) Parenthèses finales 3) Apostrophes 4) unaccent 5) ne garder que a-zA-Z 6) UPPER
    cleaned = (
        "REPLACE(REPLACE(REPLACE(REPLACE(REPLACE("
        "REGEXP_REPLACE(TRIM(" + column_sql + "), '\\s*\\([^)]*\\)\\s*$', ''), "
        "U&'\\2019', ''), U&'\\02bc', ''), U&'\\02b9', ''), U&'\\2032', ''), '''', '')"
    )
    return "UPPER(REGEXP_REPLACE(unaccent(" + cleaned + "), '[^a-zA-Z]', '', 'g'))"


def _sql_norm_name_canonical_commune_vf(column_sql: str) -> str:
    """Forme canonique spécifique aux communes dans vf_communes.

    Objectif : ignorer les suffixes d'arrondissements des très grandes villes, ex.:
    - PARIS 01, PARIS 1ER, PARIS 19 → PARIS
    - MARSEILLE 1ER, MARSEILLE 15EME → MARSEILLE
    - LYON 2, LYON 2EME → LYON

    Étapes :
    1) TRIM
    2) Retirer un éventuel suffixe " espace + nombre + (ER|EME|E) optionnels" en fin de chaîne
    3) Parenthèses finales, apostrophes comme _sql_norm_name_canonical
    4) unaccent
    5) ne garder que A-Z
    6) UPPER
    """
    base = (
        "TRIM(" + column_sql + ")"
    )
    # Suffixe arrondissement : ex. " PARIS 01", " PARIS 1ER", " PARIS 2EME"
    without_arr = (
        "REGEXP_REPLACE("
        + base
        + ", '\\s+[0-9]{1,2}\\s*(ER|EME|E)?\\s*$', '', 'i')"
    )
    cleaned = (
        "REPLACE(REPLACE(REPLACE(REPLACE(REPLACE("
        "REGEXP_REPLACE(" + without_arr + ", '\\s*\\([^)]*\\)\\s*$', ''), "
        "U&'\\2019', ''), U&'\\02bc', ''), U&'\\02b9', ''), U&'\\2032', ''), '''', '')"
    )
    return "UPPER(REGEXP_REPLACE(unaccent(" + cleaned + "), '[^a-zA-Z]', '', 'g'))"


def _sql_libgeo_ville_canonical(column_sql: str) -> str:
    """Expression SQL : forme canonique du nom de ville extrait de libgeo (loyers_communes).
    libgeo peut être 'Paris 1er Arrondissement', 'Marseille 15eme Arrondissement', 'Lyon 2e Arrondissement'.
    On extrait uniquement le nom avant le premier chiffre (n° d'arrondissement), puis même normalisation (unaccent, A-Z, UPPER)."""
    # De "Paris 1er Arrondissement" ou "Paris 01" → "Paris" : retirer " espace + premier chiffre + tout le reste"
    without_arr = (
        "TRIM(REGEXP_REPLACE(TRIM(" + column_sql + "), E'\\\\s+[0-9].*', '', 'i'))"
    )
    cleaned = (
        "REPLACE(REPLACE(REPLACE(REPLACE(REPLACE("
        "REGEXP_REPLACE(" + without_arr + ", '\\s*\\([^)]*\\)\\s*$', ''), "
        "U&'\\2019', ''), U&'\\02bc', ''), U&'\\02b9', ''), U&'\\2032', ''), '''', '')"
    )
    return "UPPER(REGEXP_REPLACE(unaccent(" + cleaned + "), '[^a-zA-Z]', '', 'g'))"


def _normalize_name(s: Optional[str]) -> str:
    """Met un nom (commune, département, type de bien, etc.) en capitales sans accent pour comparaison SQL.
    Normalise aussi les variantes d'apostrophe en apostrophe ASCII pour que L'Union, L'Union, etc. matchent."""
    if s is None:
        return ""
    s = str(s).strip()
    if not s:
        return ""
    for c in _APOSTROPHE_VARIANTS:
        s = s.replace(c, "'")
    nfd = unicodedata.normalize("NFD", s)
    sans_accent = "".join(c for c in nfd if unicodedata.category(c) != "Mn")
    return sans_accent.upper()


def get_db_connection():
    try:
        cfg = _load_db_config()
        conn = psycopg2.connect(
            host=cfg["host"],
            port=cfg["port"],
            user=cfg["user"],
            password=cfg["password"],
            dbname=cfg["database"],
        )
        schema = cfg.get("schema", DEFAULT_DB_SCHEMA)
        with conn.cursor() as cur:
            cur.execute("SET search_path TO %s, public", (schema,))
        conn.commit()
        return conn
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Erreur de connexion PostgreSQL : {e}")


class Vente(BaseModel):
    id: int
    date_mutation: date
    nature_mutation: str
    valeur_fonciere: float
    type_local: str
    surface_reelle_bati: Optional[float]
    surface_terrain: Optional[float]
    code_postal: str
    commune: str
    voie: Optional[str]
    type_de_voie: Optional[str]
    no_voie: Optional[str]
    latitude: float
    longitude: float
    distance_km: float


app = FastAPI(title="API Foncier", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/api/period")
def get_period():
    """Retourne les bornes d'années (annee_min, annee_max) disponibles dans vf_communes."""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        # Tables agrégées par commune dans le schéma foncier
        cur.execute(
            "SELECT COALESCE(MIN(annee), 2020) AS annee_min, "
            "COALESCE(MAX(annee), 2025) AS annee_max "
            "FROM foncier.vf_communes"
        )
        row = cur.fetchone()
        cur.close()
        return {"annee_min": row[0], "annee_max": row[1]}
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Erreur PostgreSQL : {e}")
    finally:
        if conn is not None:
            conn.close()


@app.get("/api/ign-tiles/{z}/{x}/{y}.png")
def get_ign_tile_cached(
    z: int,
    x: int,
    y: int,
    refresh: bool = Query(False, description="Forcer le rechargement IGN en ignorant le cache local"),
):
    """Retourne une tuile IGN via cache local disque.

    - Lecture locale prioritaire: frontend/data/carto/ign_tiles/{z}/{x}/{y}.png
    - Si absente: téléchargement depuis IGN, stockage local, puis renvoi.
    """
    if z < 0 or z > 19:
        raise HTTPException(status_code=400, detail="z doit être entre 0 et 19.")
    max_coord = (1 << z) - 1
    if x < 0 or x > max_coord or y < 0 or y > max_coord:
        raise HTTPException(status_code=400, detail="x/y hors bornes pour le niveau z.")

    tile_path = IGN_TILE_CACHE_DIR / str(z) / str(x) / f"{y}.png"
    try:
        if tile_path.exists() and not refresh:
            data = tile_path.read_bytes()
            return Response(content=data, media_type="image/png", headers={"Cache-Control": "public, max-age=31536000"})
    except OSError:
        pass

    url = IGN_WMTS_URL.format(z=z, x=x, y=y)
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "webapp-foncier/ign-cache"})
        with urllib.request.urlopen(req, timeout=20) as resp:
            if getattr(resp, "status", 200) != 200:
                raise HTTPException(status_code=502, detail=f"IGN a répondu {getattr(resp, 'status', 'inconnu')}")
            data = resp.read()
    except urllib.error.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"Erreur HTTP IGN: {e.code}")
    except urllib.error.URLError as e:
        raise HTTPException(status_code=502, detail=f"IGN indisponible: {e.reason}")

    try:
        tile_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = tile_path.with_suffix(".tmp")
        tmp_path.write_bytes(data)
        tmp_path.replace(tile_path)
    except OSError:
        # Même si l'écriture cache échoue, on renvoie la tuile téléchargée.
        pass

    return Response(content=data, media_type="image/png", headers={"Cache-Control": "public, max-age=86400"})


def _get_regions_and_depts(cur) -> tuple[list[dict], list[dict]]:
    """Régions (id, nom, departements) et liste des départements { code, nom }.
    Source : foncier.ref_regions et foncier.ref_departements (référentiel complet).
    Ne pas filtrer par vf_communes : sinon des départements sans ligne agrégée (ex. 57, 67, 68)
    disparaissent du regroupement régional alors qu’ils sont bien rattachés dans ref_departements."""
    # Départements effectivement présents dans vf_communes (info utile pour d’autres usages ; plus pour l’UI régions)
    cur.execute("SELECT DISTINCT code_dept FROM foncier.vf_communes ORDER BY code_dept")
    depts_in_data = [row[0] for row in cur.fetchall()]
    try:
        cur.execute(
            "SELECT r.code_region, r.nom_region "
            "FROM foncier.ref_regions r "
            "ORDER BY r.nom_region"
        )
        ref_regions_rows = cur.fetchall()
        if not ref_regions_rows:
            return [], [{"code": c, "nom": c} for c in depts_in_data]
        # code_dept, code_region, nom_dept depuis ref_departements (nom pour l’affichage, pas de doublon avec liste en dur)
        cur.execute(
            "SELECT code_dept, code_region, nom_dept FROM foncier.ref_departements ORDER BY code_dept"
        )
        ref_depts_rows = cur.fetchall()
        ref_depts = {row[0]: row[1] for row in ref_depts_rows}
        dept_noms = {row[0]: row[2] for row in ref_depts_rows}
        regions = []
        for code_region, nom_region in ref_regions_rows:
            # Tous les départements de la région selon le référentiel (pas seulement ceux présents dans vf_communes)
            region_depts = [d for d, r in ref_depts.items() if r == code_region]
            if region_depts:
                region_depts.sort()
                regions.append({"id": code_region, "nom": nom_region, "departements": region_depts})
        # Liste plate : tous les départements du référentiel (ordre code), pour cohérence avec les cases par région
        all_dept_codes = sorted(ref_depts.keys())
        departements = [{"code": c, "nom": dept_noms.get(c, c)} for c in all_dept_codes]
        return regions, departements
    except (psycopg2.Error, ValueError):
        pass
    departements = [{"code": c, "nom": c} for c in depts_in_data]
    return [], departements


@app.get("/api/geo")
def get_geo():
    """Régions et départements (code + nom) depuis ref_regions / ref_departements.
    Les listes par région incluent tous les départements du référentiel, pas seulement ceux ayant des lignes dans vf_communes."""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        regions, departements = _get_regions_and_depts(cur)
        cur.close()
        return {"regions": regions, "departements": departements}
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Erreur PostgreSQL : {e}")
    finally:
        if conn is not None:
            conn.close()


@app.get("/api/refs-comparaison-logement")
def get_refs_comparaison_logement():
    """Listes pour les sélecteurs type de logement, surface, nb de pièces (ref_type_logts, ref_type_surf, ref_nb_pieces)."""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        out = {"type_logts": [], "type_surf": [], "nb_pieces": []}
        try:
            cur.execute(
                "SELECT code, libelle, sort_order, type_local_pattern AS type_local_pattern "
                "FROM foncier.ref_type_logts ORDER BY sort_order, code"
            )
            out["type_logts"] = [dict(r) for r in cur.fetchall()]
        except psycopg2.Error:
            pass
        try:
            cur.execute(
                "SELECT code, libelle, sort_order, vf_suffix FROM foncier.ref_type_surf ORDER BY sort_order, code"
            )
            out["type_surf"] = [dict(r) for r in cur.fetchall()]
        except psycopg2.Error:
            pass
        try:
            cur.execute(
                "SELECT code, libelle, sort_order, vf_suffix FROM foncier.ref_nb_pieces ORDER BY sort_order, code"
            )
            out["nb_pieces"] = [dict(r) for r in cur.fetchall()]
        except psycopg2.Error:
            pass
        cur.close()
        return out
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Erreur PostgreSQL : {e}")
    finally:
        if conn is not None:
            conn.close()


@app.get("/api/communes")
def get_communes(
    code_dept: Optional[str] = Query(None, description="Code département (optionnel)"),
    code_region: Optional[str] = Query(None, description="Code région (optionnel)"),
    all_France: bool = Query(False, description="Retourner toutes les communes de la France (optionnel)"),
    q: Optional[str] = Query(None, description="Recherche par nom de commune ou code postal (optionnel, filtre ILIKE/LIKE)"),
):
    """Liste (code_dept, code_postal, commune).

    Si q fourni, filtre par nom de commune ou code postal (max 25 résultats).
    Sinon, si code_dept fourni, filtre par département ; sinon toutes les communes.
    """
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        search = (q or "").strip()
        if search:
            # Saisie uniquement numérique → filtre par code postal commençant par cette chaîne
            if search.isdigit():
                cp_prefix = search + "%"
                try:
                    cur.execute(
                        """
                        SELECT dep_code AS code_dept, code_postal, nom_standard_majuscule AS commune, code_insee
                        FROM foncier.ref_communes
                        WHERE code_postal::text LIKE %s
                        ORDER BY code_postal, nom_standard_majuscule
                        LIMIT 50
                        """,
                        (cp_prefix,),
                    )
                except psycopg2.Error:
                    cur.execute(
                        """
                        SELECT DISTINCT code_dept, code_postal, commune, NULL AS code_insee
                        FROM foncier.vf_communes
                        WHERE code_postal::text LIKE %s
                        ORDER BY code_postal, commune
                        LIMIT 50
                        """,
                        (cp_prefix,),
                    )
            else:
                # Saisie avec lettres :
                # - par défaut : filtre par NOM commençant par la chaîne (préfixe)
                # - si l'utilisateur commence par '%' : recherche "contient" (LIKE %...%)
                # Pour le code postal, on conserve un LIKE %...% pour permettre de taper un fragment.
                raw = search
                starts_with_percent = raw.startswith("%")
                term = raw[1:].strip() if starts_with_percent else raw
                if not term:
                    starts_with_percent = False
                    term = raw.strip()
                norm = _normalize_name_canonical(term)
                if starts_with_percent:
                    search_norm_like = "%" + norm + "%"
                    search_cp_like = "%" + term + "%"
                else:
                    search_norm_like = norm + "%"
                    search_cp_like = "%" + term + "%"
                try:
                    cur.execute(
                        """
                        SELECT dep_code AS code_dept, code_postal, nom_standard_majuscule AS commune, code_insee
                        FROM foncier.ref_communes
                        WHERE """ + _sql_norm_name_canonical("nom_standard_majuscule") + """ LIKE %s
                           OR code_postal::text LIKE %s
                        ORDER BY nom_standard_majuscule, code_postal
                        LIMIT 25
                        """,
                        (search_norm_like, search_cp_like),
                    )
                except psycopg2.Error:
                    cur.execute(
                        """
                        SELECT DISTINCT code_dept, code_postal, commune, NULL AS code_insee
                        FROM foncier.vf_communes
                        WHERE """ + _sql_norm_name_canonical_commune_vf("commune") + """ LIKE %s
                           OR code_postal::text LIKE %s
                        ORDER BY commune, code_postal
                        LIMIT 25
                        """,
                        (search_norm_like, search_cp_like),
                    )
        elif code_dept:
            code_dept_vf = _normalize_code_dept_for_vf(code_dept.strip())
            cur.execute(
                """
                SELECT DISTINCT v.code_dept, v.code_postal, v.commune, rc.code_insee
                FROM foncier.vf_communes v
                LEFT JOIN foncier.ref_communes rc
                  ON rc.dep_code = v.code_dept AND rc.code_postal = v.code_postal
                  AND """ + _sql_norm_name_canonical("rc.nom_standard_majuscule") + """ = """ + _sql_norm_name_canonical("v.commune") + """
                WHERE v.code_dept = %s
                ORDER BY v.commune, v.code_postal
                """,
                (code_dept_vf,),
            )
        elif code_region:
            code_region_vf = code_region.strip()
            cur.execute(
                """
                SELECT DISTINCT v.code_dept, v.code_postal, v.commune, rc.code_insee
                FROM foncier.vf_communes v
                LEFT JOIN foncier.ref_communes rc
                  ON rc.dep_code = v.code_dept AND rc.code_postal = v.code_postal
                  AND """ + _sql_norm_name_canonical("rc.nom_standard_majuscule") + """ = """ + _sql_norm_name_canonical("v.commune") + """
                WHERE v.code_dept IN (SELECT code_dept FROM foncier.ref_departements WHERE code_region = %s)
                ORDER BY v.commune, v.code_postal
                """,
                (code_region_vf,),
            )
        elif all_France:
            cur.execute(
                """
                SELECT DISTINCT v.code_dept, v.code_postal, v.commune, rc.code_insee
                FROM foncier.vf_communes v
                LEFT JOIN foncier.ref_communes rc
                  ON rc.dep_code = v.code_dept AND rc.code_postal = v.code_postal
                  AND """ + _sql_norm_name_canonical("rc.nom_standard_majuscule") + """ = """ + _sql_norm_name_canonical("v.commune") + """
                ORDER BY v.commune, v.code_postal
                """
            )
        else:
            raise HTTPException(status_code=400, detail="Aucun paramètre de filtre valide fourni")
        rows = [{"code_dept": r[0], "code_postal": r[1], "commune": r[2], "code_insee": r[3]} for r in cur.fetchall()]
        cur.close()
        return rows
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Erreur PostgreSQL : {e}")
    finally:
        if conn is not None:
            conn.close()


def _float(v: Any) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, Decimal):
        return float(v)
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _int(v: Any) -> int:
    """Convertit en int (MySQL peut renvoyer str ou Decimal)."""
    if v is None:
        return 0
    if isinstance(v, (int, float)):
        return int(v)
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return 0


# Libellés « type » ventes / rentabilité (hors maison/appart) — alignés ref_type_logts + UI
VENTE_TYPE_PARKING = "Parkings / garages"
VENTE_TYPE_LOCAL_INDUS = "Locaux indus. / comm."
VENTE_TYPE_TERRAIN = "Terrains"
VENTE_TYPE_IMMEUBLE = "Immeubles"

EXTRA_VENTE_TYPE_ORDER: Tuple[str, ...] = (
    VENTE_TYPE_PARKING,
    VENTE_TYPE_LOCAL_INDUS,
    VENTE_TYPE_TERRAIN,
    VENTE_TYPE_IMMEUBLE,
)


def _vf_extra_type_label(type_local: Optional[str]) -> Optional[str]:
    """Associe vf_communes.type_local (libellé DVF) à un libellé d'affichage pour les ventes agrégées."""
    if not type_local:
        return None
    t = _normalize_name_canonical(type_local)
    if "DEPENDANCE" in t:
        return VENTE_TYPE_PARKING
    if "INDUSTRIEL" in t or "COMMERCIAL" in t:
        return VENTE_TYPE_LOCAL_INDUS
    if "TERRAIN" in t:
        return VENTE_TYPE_TERRAIN
    if "IMMEUBLE" in t:
        return VENTE_TYPE_IMMEUBLE
    return None


# Fenêtres DVF (années glissantes jusqu’à MAX(annee)) pour rentabilité / comparaison.
PERIODES_DVF_ANNEES: Tuple[int, ...] = (1, 2, 3, 5)

_VF_COMMUNES_SELECT_COLS = (
    "type_local, nb_ventes, prix_median, prix_m2_mediane, surface_mediane, prix_moyen, prix_m2_moyenne, surface_moyenne, "
    "prix_med_s1, surf_med_s1, prix_m2_w_s1, prix_med_s2, surf_med_s2, prix_m2_w_s2, "
    "prix_med_s3, surf_med_s3, prix_m2_w_s3, prix_med_s4, surf_med_s4, prix_m2_w_s4, "
    "prix_med_s5, surf_med_s5, prix_m2_w_s5, "
    "prix_med_t1, surf_med_t1, prix_m2_w_t1, prix_med_t2, surf_med_t2, prix_m2_w_t2, "
    "prix_med_t3, surf_med_t3, prix_m2_w_t3, prix_med_t4, surf_med_t4, prix_m2_w_t4, "
    "prix_med_t5, surf_med_t5, prix_m2_w_t5, "
    "nb_ventes_s1, nb_ventes_s2, nb_ventes_s3, nb_ventes_s4, nb_ventes_s5, "
    "nb_ventes_t1, nb_ventes_t2, nb_ventes_t3, nb_ventes_t4, nb_ventes_t5"
)


def _fetch_vf_communes_range(
    cur,
    code_dept_vf: str,
    ventes_where_commune: str,
    ventes_params_commune: tuple,
    annee_min: int,
    annee_max: int,
) -> List[dict]:
    """Lit vf_communes sur [annee_min, annee_max] ; réessaie code_dept sans zéro initial si vide."""
    sql = (
        "SELECT " + _VF_COMMUNES_SELECT_COLS + " FROM foncier.vf_communes "
        "WHERE code_dept = %s AND " + ventes_where_commune + " AND annee BETWEEN %s AND %s"
    )
    params = (code_dept_vf,) + ventes_params_commune + (annee_min, annee_max)
    cur.execute(sql, params)
    rows = cur.fetchall()
    if not rows and code_dept_vf and len(str(code_dept_vf)) == 2 and str(code_dept_vf).isdigit() and str(code_dept_vf).startswith("0"):
        alt = str(code_dept_vf)[1:]
        cur.execute(sql, (alt,) + ventes_params_commune + (annee_min, annee_max))
        rows = cur.fetchall()
    return list(rows) if rows else []


def _build_ventes_lignes_from_vf_rows(vf_rows: List[dict]) -> Tuple[List[dict], dict]:
    """Agrège des lignes vf_communes (une ou plusieurs années) en ventes_lignes + by_type (Maisons / Appart.)."""
    ventes_lignes: List[dict] = []
    by_type: dict = {}
    if not vf_rows:
        return ventes_lignes, by_type
    for r in vf_rows:
        t = (r.get("type_local") or "").strip()
        if not t:
            continue
        norm = _normalize_name_canonical(t)
        if "APPARTEMENT" in norm or norm == "APPART":
            key = "Appartements"
        elif "MAISON" in norm:
            key = "Maisons"
        else:
            key = t
        if key not in by_type:
            by_type[key] = []
        by_type[key].append(r)
    sw = sum(_int(r.get("nb_ventes")) for r in vf_rows) or 1

    def wavg_total(col: str) -> Optional[float]:
        total = sum((_float(r.get(col)) or 0) * _int(r.get("nb_ventes")) for r in vf_rows)
        return round(total / sw, 2) if total is not None and sw else None

    ventes_lignes.append({
        "type": "Maisons/Appart.",
        "nb_ventes": sum(_int(r.get("nb_ventes")) for r in vf_rows),
        "prix_median": wavg_total("prix_median"),
        "prix_m2_mediane": wavg_total("prix_m2_mediane"),
        "surface_mediane": wavg_total("surface_mediane"),
        "prix_moyen": wavg_total("prix_moyen"),
        "prix_m2_moyenne": wavg_total("prix_m2_moyenne"),
        "surface_moyenne": wavg_total("surface_moyenne"),
    })
    for label, key in [("Maisons", "Maisons"), ("Appartements", "Appartements")]:
        rows_bt = by_type.get(key, [])
        if not rows_bt:
            ventes_lignes.append(_empty_vente_line(label))
            continue
        sw2 = sum(_int(r.get("nb_ventes")) for r in rows_bt) or 1

        def wavg2b(rows: List[dict], col: str) -> Optional[float]:
            total = sum((_float(r.get(col)) or 0) * _int(r.get("nb_ventes")) for r in rows_bt)
            return round(total / sw2, 2) if total is not None and sw2 else None

        ventes_lignes.append({
            "type": label,
            "nb_ventes": sum(_int(r.get("nb_ventes")) for r in rows_bt),
            "prix_median": wavg2b(rows_bt, "prix_median"),
            "prix_m2_mediane": wavg2b(rows_bt, "prix_m2_mediane"),
            "surface_mediane": wavg2b(rows_bt, "surface_mediane"),
            "prix_moyen": wavg2b(rows_bt, "prix_moyen"),
            "prix_m2_moyenne": wavg2b(rows_bt, "prix_m2_moyenne"),
            "surface_moyenne": wavg2b(rows_bt, "surface_moyenne"),
        })
    extra_buckets = {lbl: [] for lbl in EXTRA_VENTE_TYPE_ORDER}
    for r in vf_rows:
        t = (r.get("type_local") or "").strip()
        if not t:
            continue
        norm = _normalize_name_canonical(t)
        if "APPARTEMENT" in norm or norm == "APPART" or "MAISON" in norm:
            continue
        xlabel = _vf_extra_type_label(t)
        if xlabel and xlabel in extra_buckets:
            extra_buckets[xlabel].append(r)
    for lbl in EXTRA_VENTE_TYPE_ORDER:
        _append_weighted_vente_line(ventes_lignes, extra_buckets[lbl], lbl)
    return ventes_lignes, by_type


def _append_weighted_vente_line(dest: List[dict], rows: List[dict], label: str) -> None:
    """Ajoute une ligne ventes pondérée par nb_ventes (médianes / moyennes DVF)."""
    if not rows:
        dest.append({
            "type": label,
            "nb_ventes": 0,
            "prix_median": None,
            "prix_m2_mediane": None,
            "surface_mediane": None,
            "prix_moyen": None,
            "prix_m2_moyenne": None,
            "surface_moyenne": None,
        })
        return
    sw = sum(_int(r.get("nb_ventes")) for r in rows) or 1

    def wavg(col: str) -> Optional[float]:
        total = sum((_float(r.get(col)) or 0) * _int(r.get("nb_ventes")) for r in rows)
        return round(total / sw, 2) if total is not None else None

    dest.append({
        "type": label,
        "nb_ventes": sum(_int(r.get("nb_ventes")) for r in rows),
        "prix_median": wavg("prix_median"),
        "prix_m2_mediane": wavg("prix_m2_mediane"),
        "surface_mediane": wavg("surface_mediane"),
        "prix_moyen": wavg("prix_moyen"),
        "prix_m2_moyenne": wavg("prix_m2_moyenne"),
        "surface_moyenne": wavg("surface_moyenne"),
    })


def _empty_vente_line(label: str) -> dict:
    return {
        "type": label,
        "nb_ventes": 0,
        "prix_median": None,
        "prix_m2_mediane": None,
        "surface_mediane": None,
        "prix_moyen": None,
        "prix_m2_moyenne": None,
        "surface_moyenne": None,
    }


def _vente_line_from_agg(label: str, agg: dict) -> dict:
    if not agg:
        return _empty_vente_line(label)
    return {
        "type": label,
        "nb_ventes": agg.get("nb_ventes") or 0,
        "prix_median": agg.get("prix_median"),
        "prix_m2_mediane": agg.get("prix_m2_mediane"),
        "surface_mediane": agg.get("surface_mediane"),
        "prix_moyen": agg.get("prix_moyen"),
        "prix_m2_moyenne": agg.get("prix_m2_moyenne"),
        "surface_moyenne": agg.get("surface_moyenne"),
    }


def _build_ventes_lignes_for_tranche(
    vf_rows: List[dict],
    by_type: dict,
    surface_cat: Optional[str],
    pieces_cat: Optional[str],
) -> List[dict]:
    """Même structure que les lignes ventes fiche (total, Maisons, Appart., types DVF extra) avec agrégats vf pour la tranche S ou T."""
    ventes_lignes: List[dict] = []
    if not vf_rows:
        return ventes_lignes
    agg_all = _agg_rows(vf_rows, surface_cat, pieces_cat)
    ventes_lignes.append(_vente_line_from_agg("Maisons/Appart.", agg_all))
    for label, key in [("Maisons", "Maisons"), ("Appartements", "Appartements")]:
        rows_bt = by_type.get(key, [])
        if not rows_bt:
            ventes_lignes.append(_empty_vente_line(label))
        else:
            agg = _agg_rows(rows_bt, surface_cat, pieces_cat)
            ventes_lignes.append(_vente_line_from_agg(label, agg))
    extra_buckets = {lbl: [] for lbl in EXTRA_VENTE_TYPE_ORDER}
    for r in vf_rows:
        t = (r.get("type_local") or "").strip()
        if not t:
            continue
        norm = _normalize_name_canonical(t)
        if "APPARTEMENT" in norm or norm == "APPART" or "MAISON" in norm:
            continue
        xlabel = _vf_extra_type_label(t)
        if xlabel and xlabel in extra_buckets:
            extra_buckets[xlabel].append(r)
    for lbl in EXTRA_VENTE_TYPE_ORDER:
        _append_weighted_vente_line(ventes_lignes, extra_buckets[lbl], lbl)
    return ventes_lignes


def _pick_tranche_renta_line(lignes: Optional[List[dict]]) -> dict:
    """Extrait renta brute/nette pour Maisons, Appartements et ligne agrégée (tranche S ou T)."""
    by_tb = {}
    for ln in lignes or []:
        tb = (ln.get("type_bien") or "").strip()
        if tb:
            by_tb[tb] = ln

    def p(tb: str):
        r = by_tb.get(tb)
        if not r:
            return None, None
        return r.get("renta_brute"), r.get("renta_nette")

    rb_m, rn_m = p("Maisons")
    rb_a, rn_a = p("Appartements")
    rb_g, rn_g = p("Maisons/Appart.")

    def nb(tb: str):
        r = by_tb.get(tb)
        return r.get("nb_locaux") if r else None

    return {
        "renta_brute_maisons": rb_m,
        "renta_nette_maisons": rn_m,
        "renta_brute_appts": rb_a,
        "renta_nette_appts": rn_a,
        "renta_brute_agg": rb_g,
        "renta_nette_agg": rn_g,
        "nb_locaux_maisons": nb("Maisons"),
        "nb_locaux_appts": nb("Appartements"),
        "nb_locaux_agg": nb("Maisons/Appart."),
    }


def _iter_tranche_renta_column_names() -> List[str]:
    out: List[str] = []
    for i in range(1, 6):
        s = f"s{i}"
        out.extend(
            [
                f"renta_brute_maisons_{s}",
                f"renta_nette_maisons_{s}",
                f"renta_brute_appts_{s}",
                f"renta_nette_appts_{s}",
                f"renta_brute_agg_{s}",
                f"renta_nette_agg_{s}",
            ]
        )
    for i in range(1, 6):
        t = f"t{i}"
        out.extend(
            [
                f"renta_brute_maisons_{t}",
                f"renta_nette_maisons_{t}",
                f"renta_brute_appts_{t}",
                f"renta_nette_appts_{t}",
                f"renta_brute_agg_{t}",
                f"renta_nette_agg_{t}",
            ]
        )
    return out


TRANCHE_RENTA_COLS: Tuple[str, ...] = tuple(_iter_tranche_renta_column_names())


def _iter_nb_locaux_tranche_column_names() -> List[str]:
    out: List[str] = []
    for i in range(1, 6):
        s = f"s{i}"
        out.extend([f"nb_locaux_maisons_{s}", f"nb_locaux_appts_{s}", f"nb_locaux_agg_{s}"])
    for i in range(1, 6):
        t = f"t{i}"
        out.extend([f"nb_locaux_maisons_{t}", f"nb_locaux_appts_{t}", f"nb_locaux_agg_{t}"])
    return out


NB_LOCAUX_TRANCHE_COLS: Tuple[str, ...] = tuple(_iter_nb_locaux_tranche_column_names())

_INDICATEURS_COMMUNES_DATA_COLS: Tuple[str, ...] = (
    "code_insee",
    "code_dept",
    "code_postal",
    "commune",
    "reg_nom",
    "dep_nom",
    "population",
    "nb_locaux",
    "nb_ventes_dvf",
    "renta_brute",
    "renta_nette",
    "renta_brute_maisons",
    "renta_nette_maisons",
    "renta_brute_appts",
    "renta_nette_appts",
    "renta_brute_parking",
    "renta_nette_parking",
    "renta_brute_local_indus",
    "renta_nette_local_indus",
    "renta_brute_terrain",
    "renta_nette_terrain",
    "renta_brute_immeuble",
    "renta_nette_immeuble",
) + TRANCHE_RENTA_COLS + NB_LOCAUX_TRANCHE_COLS + ("taux_tfb", "taux_teom")


def _build_upsert_indicateurs_communes_sql() -> str:
    renta_cols = [
        "renta_brute",
        "renta_nette",
        "renta_brute_maisons",
        "renta_nette_maisons",
        "renta_brute_appts",
        "renta_nette_appts",
        "renta_brute_parking",
        "renta_nette_parking",
        "renta_brute_local_indus",
        "renta_nette_local_indus",
        "renta_brute_terrain",
        "renta_nette_terrain",
        "renta_brute_immeuble",
        "renta_nette_immeuble",
    ]
    all_cols = (
        ["code_insee", "code_dept", "code_postal", "commune", "reg_nom", "dep_nom", "population", "nb_locaux", "nb_locaux_maisons", "nb_locaux_appts", "nb_ventes_dvf", "indicateurs_par_periode"]
        + renta_cols
        + list(TRANCHE_RENTA_COLS)
        + list(NB_LOCAUX_TRANCHE_COLS)
        + ["taux_tfb", "taux_teom", "updated_at"]
    )
    insert_cols = ", ".join(all_cols)
    n_ph = len(all_cols) - 1
    ph = ", ".join(["%s"] * n_ph) + ", clock_timestamp()"
    upd_parts = []
    for c in all_cols:
        if c == "code_insee":
            continue
        if c == "updated_at":
            upd_parts.append("updated_at = clock_timestamp()")
        else:
            upd_parts.append(f"{c} = EXCLUDED.{c}")
    upd = ", ".join(upd_parts)
    return (
        "INSERT INTO foncier.indicateurs_communes ("
        + insert_cols
        + ") VALUES ("
        + ph
        + ") ON CONFLICT (code_insee) DO UPDATE SET "
        + upd
    )


_UPSERT_SQL_INDICATEURS_COMMUNES = _build_upsert_indicateurs_communes_sql()
# SAVEPOINT pour refresh batch (commit=False) : une erreur SQL n’annule pas toute la transaction.
_UPSERT_INDICATEURS_COMMUNES_SAVEPOINT = "sp_upsert_indicateurs_communes"


def _clamp_numeric_6_2(v: Any) -> Optional[float]:
    """Colonnes NUMERIC(6,2) en base : |valeur| doit être < 10^4 (bornes ±9999.99).

    Important : `round(x, 2)` en Python peut produire 10000.0 pour des entrées proches
    de la borne (ex. round(9999.995, 2) -> 10000.0), ce qui dépasse encore NUMERIC(6,2).
    On borne donc avant **et** après l'arrondi.
    """
    if v is None:
        return None
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    lo, hi = -9999.99, 9999.99
    x = max(lo, min(hi, x))
    xr = round(x, 2)
    xr = max(lo, min(hi, xr))
    return xr


def _tuple_params_indicateurs_communes(row: dict) -> tuple:
    return (
        row.get("code_insee"),
        row.get("code_dept"),
        row.get("code_postal"),
        row.get("commune"),
        row.get("region"),
        row.get("dep_nom"),
        row.get("population"),
        row.get("nb_locaux"),
        row.get("nb_locaux_maisons"),
        row.get("nb_locaux_appts"),
        row.get("nb_ventes_dvf"),
        Json(row["indicateurs_par_periode"]) if row.get("indicateurs_par_periode") is not None else None,
        _clamp_numeric_6_2(row.get("renta_brute")),
        _clamp_numeric_6_2(row.get("renta_nette")),
        _clamp_numeric_6_2(row.get("renta_brute_maisons")),
        _clamp_numeric_6_2(row.get("renta_nette_maisons")),
        _clamp_numeric_6_2(row.get("renta_brute_appts")),
        _clamp_numeric_6_2(row.get("renta_nette_appts")),
        _clamp_numeric_6_2(row.get("renta_brute_parking")),
        _clamp_numeric_6_2(row.get("renta_nette_parking")),
        _clamp_numeric_6_2(row.get("renta_brute_local_indus")),
        _clamp_numeric_6_2(row.get("renta_nette_local_indus")),
        _clamp_numeric_6_2(row.get("renta_brute_terrain")),
        _clamp_numeric_6_2(row.get("renta_nette_terrain")),
        _clamp_numeric_6_2(row.get("renta_brute_immeuble")),
        _clamp_numeric_6_2(row.get("renta_nette_immeuble")),
        *tuple(_clamp_numeric_6_2(row.get(k)) for k in TRANCHE_RENTA_COLS),
        *tuple(row.get(k) for k in NB_LOCAUX_TRANCHE_COLS),
        row.get("taux_tfb"),
        row.get("taux_teom"),
    )


def _append_tranche_floats_from_db_row(r: dict, dest: dict) -> None:
    for k in TRANCHE_RENTA_COLS:
        dest[k] = float(r[k]) if r.get(k) is not None else None
    for k in NB_LOCAUX_TRANCHE_COLS:
        dest[k] = int(r[k]) if r.get(k) is not None else None


def _build_upsert_indicateurs_depts_sql() -> str:
    renta_cols = [
        "renta_brute",
        "renta_nette",
        "renta_brute_maisons",
        "renta_nette_maisons",
        "renta_brute_appts",
        "renta_nette_appts",
        "renta_brute_parking",
        "renta_nette_parking",
        "renta_brute_local_indus",
        "renta_nette_local_indus",
        "renta_brute_terrain",
        "renta_nette_terrain",
        "renta_brute_immeuble",
        "renta_nette_immeuble",
    ]
    all_cols = (
        ["code_dept", "dep_nom", "reg_nom", "code_region", "population", "nb_locaux", "nb_locaux_maisons", "nb_locaux_appts", "nb_ventes_dvf", "indicateurs_par_periode"]
        + renta_cols
        + list(TRANCHE_RENTA_COLS)
        + list(NB_LOCAUX_TRANCHE_COLS)
        + ["taux_tfb", "taux_teom", "updated_at"]
    )
    insert_cols = ", ".join(all_cols)
    n_ph = len(all_cols) - 1
    ph = ", ".join(["%s"] * n_ph) + ", clock_timestamp()"
    upd_parts = []
    for c in all_cols:
        if c == "code_dept":
            continue
        if c == "updated_at":
            upd_parts.append("updated_at = clock_timestamp()")
        else:
            upd_parts.append(f"{c} = EXCLUDED.{c}")
    upd = ", ".join(upd_parts)
    return (
        "INSERT INTO foncier.indicateurs_depts ("
        + insert_cols
        + ") VALUES ("
        + ph
        + ") ON CONFLICT (code_dept) DO UPDATE SET "
        + upd
    )


_UPSERT_SQL_INDICATEURS_DEPTS = _build_upsert_indicateurs_depts_sql()


def _tuple_params_indicateurs_depts(row: dict) -> tuple:
    return (
        row.get("code_dept"),
        row.get("dep_nom"),
        row.get("region"),
        row.get("code_region"),
        row.get("population"),
        row.get("nb_locaux"),
        row.get("nb_locaux_maisons"),
        row.get("nb_locaux_appts"),
        row.get("nb_ventes_dvf"),
        Json(row["indicateurs_par_periode"]) if row.get("indicateurs_par_periode") is not None else None,
        _clamp_numeric_6_2(row.get("renta_brute")),
        _clamp_numeric_6_2(row.get("renta_nette")),
        _clamp_numeric_6_2(row.get("renta_brute_maisons")),
        _clamp_numeric_6_2(row.get("renta_nette_maisons")),
        _clamp_numeric_6_2(row.get("renta_brute_appts")),
        _clamp_numeric_6_2(row.get("renta_nette_appts")),
        _clamp_numeric_6_2(row.get("renta_brute_parking")),
        _clamp_numeric_6_2(row.get("renta_nette_parking")),
        _clamp_numeric_6_2(row.get("renta_brute_local_indus")),
        _clamp_numeric_6_2(row.get("renta_nette_local_indus")),
        _clamp_numeric_6_2(row.get("renta_brute_terrain")),
        _clamp_numeric_6_2(row.get("renta_nette_terrain")),
        _clamp_numeric_6_2(row.get("renta_brute_immeuble")),
        _clamp_numeric_6_2(row.get("renta_nette_immeuble")),
        *tuple(_clamp_numeric_6_2(row.get(k)) for k in TRANCHE_RENTA_COLS),
        *tuple(row.get(k) for k in NB_LOCAUX_TRANCHE_COLS),
        row.get("taux_tfb"),
        row.get("taux_teom"),
    )


def _build_upsert_indicateurs_regions_sql() -> str:
    renta_cols = [
        "renta_brute",
        "renta_nette",
        "renta_brute_maisons",
        "renta_nette_maisons",
        "renta_brute_appts",
        "renta_nette_appts",
        "renta_brute_parking",
        "renta_nette_parking",
        "renta_brute_local_indus",
        "renta_nette_local_indus",
        "renta_brute_terrain",
        "renta_nette_terrain",
        "renta_brute_immeuble",
        "renta_nette_immeuble",
    ]
    all_cols = (
        ["code_region", "reg_nom", "population", "nb_locaux", "nb_locaux_maisons", "nb_locaux_appts", "nb_ventes_dvf", "indicateurs_par_periode"]
        + renta_cols
        + list(TRANCHE_RENTA_COLS)
        + list(NB_LOCAUX_TRANCHE_COLS)
        + ["taux_tfb", "taux_teom", "updated_at"]
    )
    insert_cols = ", ".join(all_cols)
    n_ph = len(all_cols) - 1
    ph = ", ".join(["%s"] * n_ph) + ", clock_timestamp()"
    upd_parts = []
    for c in all_cols:
        if c == "code_region":
            continue
        if c == "updated_at":
            upd_parts.append("updated_at = clock_timestamp()")
        else:
            upd_parts.append(f"{c} = EXCLUDED.{c}")
    upd = ", ".join(upd_parts)
    return (
        "INSERT INTO foncier.indicateurs_regions ("
        + insert_cols
        + ") VALUES ("
        + ph
        + ") ON CONFLICT (code_region) DO UPDATE SET "
        + upd
    )


_UPSERT_SQL_INDICATEURS_REGIONS = _build_upsert_indicateurs_regions_sql()


def _tuple_params_indicateurs_regions(row: dict) -> tuple:
    return (
        row.get("code_region"),
        row.get("region"),
        row.get("population"),
        row.get("nb_locaux"),
        row.get("nb_locaux_maisons"),
        row.get("nb_locaux_appts"),
        row.get("nb_ventes_dvf"),
        Json(row["indicateurs_par_periode"]) if row.get("indicateurs_par_periode") is not None else None,
        _clamp_numeric_6_2(row.get("renta_brute")),
        _clamp_numeric_6_2(row.get("renta_nette")),
        _clamp_numeric_6_2(row.get("renta_brute_maisons")),
        _clamp_numeric_6_2(row.get("renta_nette_maisons")),
        _clamp_numeric_6_2(row.get("renta_brute_appts")),
        _clamp_numeric_6_2(row.get("renta_nette_appts")),
        _clamp_numeric_6_2(row.get("renta_brute_parking")),
        _clamp_numeric_6_2(row.get("renta_nette_parking")),
        _clamp_numeric_6_2(row.get("renta_brute_local_indus")),
        _clamp_numeric_6_2(row.get("renta_nette_local_indus")),
        _clamp_numeric_6_2(row.get("renta_brute_terrain")),
        _clamp_numeric_6_2(row.get("renta_nette_terrain")),
        _clamp_numeric_6_2(row.get("renta_brute_immeuble")),
        _clamp_numeric_6_2(row.get("renta_nette_immeuble")),
        *tuple(_clamp_numeric_6_2(row.get(k)) for k in TRANCHE_RENTA_COLS),
        *tuple(row.get(k) for k in NB_LOCAUX_TRANCHE_COLS),
        row.get("taux_tfb"),
        row.get("taux_teom"),
    )


def _round_indicator_optional(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        return round(float(v), 2)
    except (TypeError, ValueError):
        return None


def _compute_rentabilite_tranches_nested(
    vf_rows: List[dict],
    by_type: dict,
    loc_lignes: List[dict],
    parc_data: Optional[dict],
    loyer_ref: Optional[float],
    taux_tfb: Optional[float],
    taux_teom: Optional[float],
    code_insee: Optional[str],
) -> dict:
    """Rentabilités par tranche surface S1–S5 et pièces T1–T5 (vf_communes), sans repli sur l’agrégat sans tranche."""
    out: dict = {"surface": {}, "pieces": {}}
    if not vf_rows or not loc_lignes:
        return out
    for i in range(1, 6):
        s = f"S{i}"
        ventes = _build_ventes_lignes_for_tranche(vf_rows, by_type, s, None)
        lignes = _build_renta_lignes(
            ventes, loc_lignes, parc_data, loyer_ref, taux_tfb, taux_teom, use_median=True, code_insee=code_insee
        )
        out["surface"][s] = _pick_tranche_renta_line(lignes)
    for i in range(1, 6):
        t = f"T{i}"
        ventes = _build_ventes_lignes_for_tranche(vf_rows, by_type, None, t)
        lignes = _build_renta_lignes(
            ventes, loc_lignes, parc_data, loyer_ref, taux_tfb, taux_teom, use_median=True, code_insee=code_insee
        )
        out["pieces"][t] = _pick_tranche_renta_line(lignes)
    return out


def _flatten_tranche_nested_to_indicator_row(nested: Optional[dict]) -> dict:
    """Aplatit rentabilite_tranches (surface/pieces) vers les colonnes indicateurs_communes."""
    row = {k: None for k in TRANCHE_RENTA_COLS}
    if not nested:
        return row
    surf = nested.get("surface") or {}
    for i in range(1, 6):
        s = f"S{i}"
        ss = f"s{i}"
        d = surf.get(s) or {}
        row[f"renta_brute_maisons_{ss}"] = d.get("renta_brute_maisons")
        row[f"renta_nette_maisons_{ss}"] = d.get("renta_nette_maisons")
        row[f"renta_brute_appts_{ss}"] = d.get("renta_brute_appts")
        row[f"renta_nette_appts_{ss}"] = d.get("renta_nette_appts")
        row[f"renta_brute_agg_{ss}"] = d.get("renta_brute_agg")
        row[f"renta_nette_agg_{ss}"] = d.get("renta_nette_agg")
        row[f"nb_locaux_maisons_{ss}"] = d.get("nb_locaux_maisons")
        row[f"nb_locaux_appts_{ss}"] = d.get("nb_locaux_appts")
        row[f"nb_locaux_agg_{ss}"] = d.get("nb_locaux_agg")
    pie = nested.get("pieces") or {}
    for i in range(1, 6):
        t = f"T{i}"
        tt = f"t{i}"
        d = pie.get(t) or {}
        row[f"renta_brute_maisons_{tt}"] = d.get("renta_brute_maisons")
        row[f"renta_nette_maisons_{tt}"] = d.get("renta_nette_maisons")
        row[f"renta_brute_appts_{tt}"] = d.get("renta_brute_appts")
        row[f"renta_nette_appts_{tt}"] = d.get("renta_nette_appts")
        row[f"renta_brute_agg_{tt}"] = d.get("renta_brute_agg")
        row[f"renta_nette_agg_{tt}"] = d.get("renta_nette_agg")
        row[f"nb_locaux_maisons_{tt}"] = d.get("nb_locaux_maisons")
        row[f"nb_locaux_appts_{tt}"] = d.get("nb_locaux_appts")
        row[f"nb_locaux_agg_{tt}"] = d.get("nb_locaux_agg")
    return row


def _nb_locaux_for_type(
    type_label: str,
    ventes_by_type: dict,
    nb_maisons_agreg: Any,
    nb_apparts_agreg: Any,
) -> Optional[int]:
    """Effectif (parc logements RP ou nb ventes DVF) pour la ligne de rentabilité du type donné."""
    v = ventes_by_type.get(type_label) or {}
    n_ventes = _int(v.get("nb_ventes"))
    if type_label == "Maisons/Appart.":
        parc = int(nb_maisons_agreg or 0) + int(nb_apparts_agreg or 0)
        if parc > 0:
            return parc
        if n_ventes > 0:
            return n_ventes
        n1 = _int((ventes_by_type.get("Maisons") or {}).get("nb_ventes"))
        n2 = _int((ventes_by_type.get("Appartements") or {}).get("nb_ventes"))
        s = n1 + n2
        return s if s > 0 else None
    if type_label == "Maisons":
        pm = int(nb_maisons_agreg or 0)
        if pm > 0:
            return pm
        return n_ventes if n_ventes > 0 else None
    if type_label == "Appartements":
        pa = int(nb_apparts_agreg or 0)
        if pa > 0:
            return pa
        return n_ventes if n_ventes > 0 else None
    return n_ventes if n_ventes > 0 else None


def _build_renta_lignes(
    ventes_lignes: List[dict],
    loc_lignes: List[dict],
    parc_data: Optional[dict],
    loyer_ref: Optional[float],
    taux_tfb: Optional[float],
    taux_teom: Optional[float],
    use_median: bool = True,
    code_insee: Optional[str] = None,
) -> List[dict]:
    """Calcule les lignes de rentabilité (brute, HC, nette) par type de bien. Utilisé par fiche-logement et comparaison_scores."""
    if not ventes_lignes or not loc_lignes:
        return []
    nb_maisons_agreg = (parc_data.get("nb_maisons") or 0) if parc_data else 0
    nb_apparts_agreg = (parc_data.get("nb_apparts") or 0) if parc_data else 0
    total_log = nb_maisons_agreg + nb_apparts_agreg or 1
    surface_moy_agreg = parc_data.get("surface_moy") if parc_data else None
    tf_mediane = None
    if surface_moy_agreg is not None and loyer_ref is not None and taux_tfb is not None:
        tf_mediane = surface_moy_agreg * 3 * loyer_ref * (taux_tfb / 100.0)

    prix_m2_key = "prix_m2_mediane" if use_median else "prix_m2_moyenne"
    prix_total_key = "prix_median" if use_median else "prix_moyen"
    surface_key = "surface_mediane" if use_median else "surface_moyenne"
    loyer_key = "loyer_med_m2"
    ventes_by_type = {r["type"]: r for r in ventes_lignes}
    loc_by_type = {r["type"]: r for r in loc_lignes}
    charges_pre: dict = {}
    for type_label in ["Maisons", "Appartements"]:
        v = ventes_by_type.get(type_label, {})
        l = loc_by_type.get(type_label, {})
        surf = _float(v.get(surface_key))
        loyer_m2 = _float(l.get(loyer_key))
        if type_label == "Maisons":
            if taux_teom is not None and taux_teom <= 0.20 and surf is not None and surf > 0 and loyer_ref is not None:
                charges_pre[type_label] = (taux_teom / 100.0) * surf * 3 * loyer_ref
            else:
                if taux_teom is None or taux_teom > 0.20:
                    if surf is not None and surf > 0 and loyer_ref is not None:
                        charges_pre[type_label] = (10 / 100.0) * surf * 3 * loyer_ref
                    else:
                        charges_pre[type_label] = None
                else:
                    charges_pre[type_label] = None
        else:
            charges_pre[type_label] = (0.10 * loyer_m2 * surf * 12) if (loyer_m2 is not None and surf is not None and surf > 0) else None
    if charges_pre.get("Maisons") is not None and charges_pre.get("Appartements") is not None and total_log > 0:
        charges_pre["Maisons/Appart."] = (nb_maisons_agreg * charges_pre["Maisons"] + nb_apparts_agreg * charges_pre["Appartements"]) / total_log
    else:
        charges_pre["Maisons/Appart."] = charges_pre.get("Maisons") if nb_apparts_agreg == 0 else (charges_pre.get("Appartements") if nb_maisons_agreg == 0 else None)

    # Types DVF additionnels : charges au même titre que les appartements (10 % loyer annuel)
    for type_label in EXTRA_VENTE_TYPE_ORDER:
        if type_label not in ventes_by_type:
            continue
        v = ventes_by_type[type_label]
        l = loc_by_type.get(type_label, {})
        loyer_m2 = _float(l.get(loyer_key))
        surf = _float(v.get(surface_key))
        if surf is None and not use_median:
            surf = _float(v.get("surface_mediane"))
        charges_pre[type_label] = (0.10 * loyer_m2 * surf * 12) if (loyer_m2 is not None and surf is not None and surf > 0) else None

    row_plan: List[Tuple[str, float]] = [
        ("Maisons/Appart.", float(total_log)),
        ("Maisons", float(nb_maisons_agreg)),
        ("Appartements", float(nb_apparts_agreg)),
    ]
    for et in EXTRA_VENTE_TYPE_ORDER:
        if et in ventes_by_type:
            row_plan.append((et, 0.0))

    out: List[dict] = []
    for type_label, _poids in row_plan:
        v = ventes_by_type.get(type_label, {})
        l = loc_by_type.get(type_label, {})
        nl = _nb_locaux_for_type(type_label, ventes_by_type, nb_maisons_agreg, nb_apparts_agreg)
        prix_m2 = _float(v.get(prix_m2_key))
        if (prix_m2 is None or prix_m2 <= 0) and not use_median:
            prix_m2 = _float(v.get("prix_m2_mediane"))
        prix_total = _float(v.get(prix_total_key))
        if prix_total is None and not use_median:
            prix_total = _float(v.get("prix_median"))
        loyer_m2 = _float(l.get(loyer_key))
        surface_val = _float(v.get(surface_key))
        if surface_val is None and not use_median:
            surface_val = _float(v.get("surface_mediane"))
        charges = charges_pre.get(type_label)
        if prix_m2 is None or prix_m2 <= 0:
            out.append({
                "type_bien": type_label,
                "renta_brute": None,
                "renta_hc": None,
                "charges_mediane": None,
                "renta_nette": None,
                "nb_locaux": nl,
            })
            continue
        renta_brute = (loyer_m2 * 12 / prix_m2 * 100) if loyer_m2 is not None else None
        denom = (prix_total * 1.10) if (prix_total is not None and prix_total > 0) else None
        renta_hc = None
        if loyer_m2 is not None and surface_val is not None and surface_val > 0 and denom:
            renta_hc = (loyer_m2 * surface_val * 12 - (tf_mediane or 0)) / denom * 100
        renta_nette = None
        if loyer_m2 is not None and surface_val is not None and surface_val > 0 and denom and charges is not None:
            renta_nette = (loyer_m2 * surface_val * 12 * 0.75 - 0.25 * charges - (tf_mediane or 0)) / denom * 100
        if DEBUG:
            _debug_log(
                "[renta-debug] code_insee=%s type=%s prix_m2=%s prix_total=%s loyer_m2=%s "
                "surface_mediane=%s charges=%s tf_mediane=%s -> renta_brute=%s renta_hc=%s renta_nette=%s",
                code_insee,
                type_label,
                prix_m2,
                prix_total,
                loyer_m2,
                surface_val,
                charges,
                tf_mediane,
                renta_brute,
                renta_hc,
                renta_nette,
            )
        out.append({
            "type_bien": type_label,
            "renta_brute": round(renta_brute, 2) if renta_brute is not None else None,
            "renta_hc": round(renta_hc, 2) if renta_hc is not None else None,
            "charges_mediane": round(charges, 2) if charges is not None else None,
            "renta_nette": round(renta_nette, 2) if renta_nette is not None else None,
            "nb_locaux": nl,
        })
    return out


def _extract_rentas_from_lignes(lignes: Optional[List[dict]]) -> dict:
    """Lit renta_brute / renta_nette par type_bien (dont types DVF extra)."""
    keys_out = {
        "nb_locaux": None,
        "nb_locaux_maisons": None,
        "nb_locaux_appts": None,
        "renta_nette": None,
        "renta_brute": None,
        "renta_brute_maisons": None,
        "renta_nette_maisons": None,
        "renta_brute_appts": None,
        "renta_nette_appts": None,
        "renta_brute_parking": None,
        "renta_nette_parking": None,
        "renta_brute_local_indus": None,
        "renta_nette_local_indus": None,
        "renta_brute_terrain": None,
        "renta_nette_terrain": None,
        "renta_brute_immeuble": None,
        "renta_nette_immeuble": None,
    }
    if not lignes:
        return keys_out
    by_tb = {}
    for ln in lignes:
        tb = (ln.get("type_bien") or "").strip()
        if tb:
            by_tb[tb] = ln
    if not by_tb:
        if len(lignes) >= 1:
            keys_out["renta_nette"] = lignes[0].get("renta_nette")
            keys_out["renta_brute"] = lignes[0].get("renta_brute")
            keys_out["nb_locaux"] = lignes[0].get("nb_locaux")
        if len(lignes) > 1:
            keys_out["renta_brute_maisons"] = lignes[1].get("renta_brute")
            keys_out["renta_nette_maisons"] = lignes[1].get("renta_nette")
        if len(lignes) > 2:
            keys_out["renta_brute_appts"] = lignes[2].get("renta_brute")
            keys_out["renta_nette_appts"] = lignes[2].get("renta_nette")
        return keys_out

    def pick(tb: str, f: str):
        r = by_tb.get(tb)
        return r.get(f) if r else None

    keys_out["renta_nette"] = pick("Maisons/Appart.", "renta_nette")
    keys_out["renta_brute"] = pick("Maisons/Appart.", "renta_brute")
    keys_out["nb_locaux"] = pick("Maisons/Appart.", "nb_locaux")
    keys_out["nb_locaux_maisons"] = pick("Maisons", "nb_locaux")
    keys_out["nb_locaux_appts"] = pick("Appartements", "nb_locaux")
    keys_out["renta_brute_maisons"] = pick("Maisons", "renta_brute")
    keys_out["renta_nette_maisons"] = pick("Maisons", "renta_nette")
    keys_out["renta_brute_appts"] = pick("Appartements", "renta_brute")
    keys_out["renta_nette_appts"] = pick("Appartements", "renta_nette")
    keys_out["renta_brute_parking"] = pick(VENTE_TYPE_PARKING, "renta_brute")
    keys_out["renta_nette_parking"] = pick(VENTE_TYPE_PARKING, "renta_nette")
    keys_out["renta_brute_local_indus"] = pick(VENTE_TYPE_LOCAL_INDUS, "renta_brute")
    keys_out["renta_nette_local_indus"] = pick(VENTE_TYPE_LOCAL_INDUS, "renta_nette")
    keys_out["renta_brute_terrain"] = pick(VENTE_TYPE_TERRAIN, "renta_brute")
    keys_out["renta_nette_terrain"] = pick(VENTE_TYPE_TERRAIN, "renta_nette")
    keys_out["renta_brute_immeuble"] = pick(VENTE_TYPE_IMMEUBLE, "renta_brute")
    keys_out["renta_nette_immeuble"] = pick(VENTE_TYPE_IMMEUBLE, "renta_nette")
    return keys_out


def _indicator_snapshot_from_median_block(block: Optional[dict]) -> Optional[dict]:
    """Snapshot JSON pour une fenêtre DVF : rentas + nb_locaux + nb_ventes_dvf."""
    if not block or not block.get("lignes"):
        return None
    rx = _extract_rentas_from_lignes(block["lignes"])
    out = {k: v for k, v in rx.items() if v is not None}
    nv = block.get("nb_ventes")
    if nv is not None:
        try:
            out["nb_ventes_dvf"] = int(nv)
        except (TypeError, ValueError):
            pass
    return out if out else None


def _build_indicateurs_par_periode_json(fiche: dict) -> Optional[dict]:
    """Construit { '1'|'2'|'3'|'5': snapshot } depuis rentabilite_mediane_par_periode."""
    mpp = fiche.get("rentabilite_mediane_par_periode")
    if not isinstance(mpp, dict) or not mpp:
        return None
    out: Dict[str, dict] = {}
    for k in ("1", "2", "3", "5"):
        snap = _indicator_snapshot_from_median_block(mpp.get(k))
        if snap:
            out[k] = snap
    return out if out else None


def _aggregate_indicator_snapshots_weighted(buckets: List[Tuple[dict, float]]) -> dict:
    """Moyenne pondérée population pour les rentas / tranches ; somme nb_locaux et nb_ventes_dvf."""
    numeric_keys = [
        "renta_brute", "renta_nette", "renta_brute_maisons", "renta_nette_maisons",
        "renta_brute_appts", "renta_nette_appts",
        "renta_brute_parking", "renta_nette_parking", "renta_brute_local_indus", "renta_nette_local_indus",
        "renta_brute_terrain", "renta_nette_terrain", "renta_brute_immeuble", "renta_nette_immeuble",
    ] + list(TRANCHE_RENTA_COLS)
    total_w = sum(w for _, w in buckets) or 1.0
    out: dict = {}
    for k in numeric_keys:
        s = 0.0
        for snap, w in buckets:
            v = snap.get(k)
            if v is not None:
                try:
                    s += float(v) * w
                except (TypeError, ValueError):
                    pass
        out[k] = round(s / total_w, 2) if s else None
    nl_sum = 0
    nv_sum = 0
    for snap, _ in buckets:
        if snap.get("nb_locaux") is not None:
            try:
                nl_sum += int(snap["nb_locaux"])
            except (TypeError, ValueError):
                pass
        if snap.get("nb_ventes_dvf") is not None:
            try:
                nv_sum += int(snap["nb_ventes_dvf"])
            except (TypeError, ValueError):
                pass
    out["nb_locaux"] = nl_sum if nl_sum else None
    out["nb_ventes_dvf"] = nv_sum if nv_sum else None
    nl_m_sum = 0
    nl_a_sum = 0
    for snap, _ in buckets:
        if snap.get("nb_locaux_maisons") is not None:
            try:
                nl_m_sum += int(snap["nb_locaux_maisons"])
            except (TypeError, ValueError):
                pass
        if snap.get("nb_locaux_appts") is not None:
            try:
                nl_a_sum += int(snap["nb_locaux_appts"])
            except (TypeError, ValueError):
                pass
    out["nb_locaux_maisons"] = nl_m_sum if nl_m_sum else None
    out["nb_locaux_appts"] = nl_a_sum if nl_a_sum else None
    for col in NB_LOCAUX_TRANCHE_COLS:
        col_sum = 0
        for snap, _ in buckets:
            if snap.get(col) is not None:
                try:
                    col_sum += int(snap[col])
                except (TypeError, ValueError):
                    pass
        out[col] = col_sum if col_sum else None
    return out


def _aggregate_par_periode_from_commune_rows(
    commune_rows: List[dict],
    weight_key: str = "population",
) -> Optional[dict]:
    """Agrège indicateurs_par_periode des communes (même clés '1'…'5')."""
    keys = ("1", "2", "3", "5")
    out: Dict[str, dict] = {}
    for pk in keys:
        buckets: List[Tuple[dict, float]] = []
        for row in commune_rows:
            jp = row.get("indicateurs_par_periode")
            if isinstance(jp, str):
                try:
                    jp = json.loads(jp)
                except (TypeError, ValueError):
                    jp = None
            if not isinstance(jp, dict):
                continue
            snap = jp.get(pk)
            if not snap:
                continue
            w = float(row.get(weight_key) or 1)
            if w <= 0:
                w = 1.0
            buckets.append((snap, w))
        if not buckets:
            continue
        out[pk] = _aggregate_indicator_snapshots_weighted(buckets)
    return out if out else None


def _merge_periode_into_row(row: dict, periode_annees: int) -> dict:
    """Applique le snapshot indicateurs_par_periode[str(periode)] sur les champs plats pour tri / affichage."""
    if periode_annees == 1:
        return row
    jp = row.get("indicateurs_par_periode")
    if isinstance(jp, str):
        try:
            jp = json.loads(jp)
        except (TypeError, ValueError):
            jp = None
    if not isinstance(jp, dict):
        return row
    snap = jp.get(str(periode_annees))
    if not snap:
        return row
    out = dict(row)
    for k, v in snap.items():
        if v is not None:
            out[k] = v
    return out


def _agg_rows(rows: List[dict], surface_cat: Optional[str], pieces_cat: Optional[str]) -> dict:
    """Agrège des lignes vf_communes : sommes et moyennes pondérées par nb_ventes."""
    if not rows:
        return {}
    sum_w = sum(_int(r.get("nb_ventes")) for r in rows)
    if sum_w == 0:
        sum_w = 1
    total_ventes = sum(_int(r.get("nb_ventes")) for r in rows)

    use_s = surface_cat and surface_cat.upper() in ("S1", "S2", "S3", "S4", "S5")
    use_t = pieces_cat and pieces_cat.upper() in ("T1", "T2", "T3", "T4", "T5")
    if use_s:
        s = surface_cat.upper().lower()
        col_prix, col_surf, col_p2m = f"prix_med_{s}", f"surf_med_{s}", f"prix_m2_w_{s}"
    elif use_t:
        # PostgreSQL : identifiants non quotés → minuscules (prix_med_t1, pas prix_med_T1)
        t = pieces_cat.upper().lower()
        col_prix, col_surf, col_p2m = f"prix_med_{t}", f"surf_med_{t}", f"prix_m2_w_{t}"
    else:
        col_prix, col_surf, col_p2m = "prix_median", "surface_mediane", "prix_m2_mediane"

    def wavg(key: str) -> Optional[float]:
        total = 0.0
        for r in rows:
            v = _float(r.get(key))
            w = _int(r.get("nb_ventes"))
            if v is not None:
                total += v * w
        return round(total / sum_w, 2) if total else None

    if use_s or use_t:
        # Utiliser le comptage spécifique à la tranche si disponible (Phase 2)
        if use_s:
            tranche_key = f"nb_ventes_{s}"
        else:
            tranche_key = f"nb_ventes_{t}"
        tranche_cnt = sum(_int(r.get(tranche_key)) for r in rows)
        return {
            "nb_ventes": tranche_cnt if tranche_cnt > 0 else total_ventes,
            "prix_moyen": wavg(col_prix),
            "prix_median": wavg(col_prix),
            "prix_q1": None,
            "prix_q3": None,
            "surface_moyenne": wavg(col_surf),
            "surface_mediane": wavg(col_surf),
            "surface_q1": None,
            "surface_q3": None,
            "prix_m2_moyenne": wavg(col_p2m),
            "prix_m2_mediane": wavg(col_p2m),
            "prix_m2_q1": None,
            "prix_m2_q3": None,
        }
    return {
        "nb_ventes": total_ventes,
        "prix_moyen": wavg("prix_moyen"),
        "prix_median": wavg("prix_median"),
        "prix_q1": wavg("prix_q1"),
        "prix_q3": wavg("prix_q3"),
        "surface_moyenne": wavg("surface_moyenne"),
        "surface_mediane": wavg("surface_mediane"),
        "surface_q1": None,
        "surface_q3": None,
        "prix_m2_moyenne": wavg("prix_m2_moyenne"),
        "prix_m2_mediane": wavg("prix_m2_mediane"),
        "prix_m2_q1": wavg("prix_m2_q1"),
        "prix_m2_q3": wavg("prix_m2_q3"),
    }


@app.get("/api/stats")
def get_stats(
    niveau: Literal["region", "department", "commune"] = Query(..., description="Niveau géographique"),
    region_id: Optional[str] = Query(None, description="Id région (si niveau=region)"),
    code_dept: Optional[str] = Query(None, description="Code département"),
    code_postal: Optional[str] = Query(None, description="Code postal (si niveau=commune)"),
    commune: Optional[str] = Query(None, description="Nom commune (si niveau=commune)"),
    type_local: Optional[str] = Query(None, description="Appartement, Maison ou vide = tous"),
    surface_cat: Optional[str] = Query(None, description="S1..S5 (optionnel, si type Appartement/Maison)"),
    pieces_cat: Optional[str] = Query(None, description="T1..T5 (optionnel)"),
    annee_min: Optional[int] = Query(None),
    annee_max: Optional[int] = Query(None),
):
    """
    Agrégats vf_communes selon niveau (région/département/commune), type, catégories S/T, période.
    Retourne un résumé global et une série par année pour les courbes d'évolution.
    """
    # Traiter chaînes vides comme None (query string peut envoyer "")
    if annee_min is not None and (isinstance(annee_min, str) and annee_min.strip() == ""):
        annee_min = None
    if annee_max is not None and (isinstance(annee_max, str) and annee_max.strip() == ""):
        annee_max = None
    if code_dept and isinstance(code_dept, str):
        code_dept = code_dept.strip()
    if code_postal and isinstance(code_postal, str):
        code_postal = code_postal.strip()
    if commune and isinstance(commune, str):
        commune = commune.strip()
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        # Période par défaut (s'assurer que annee_min/max sont des int, la query peut renvoyer str)
        if annee_min is None or annee_max is None:
            cur.execute(
                "SELECT COALESCE(MIN(annee),2020) AS mn, "
                "COALESCE(MAX(annee),2025) AS mx "
                "FROM foncier.vf_communes"
            )
            r = cur.fetchone()
            annee_min = int(annee_min or r["mn"])
            annee_max = int(annee_max or r["mx"])
        else:
            annee_min = int(annee_min)
            annee_max = int(annee_max)
        # Filtre géo
        dept_list = None
        if niveau == "region":
            if not region_id:
                raise HTTPException(status_code=400, detail="region_id requis pour niveau=region")
            try:
                cur.execute(
                    "SELECT code_dept FROM foncier.ref_departements "
                    "WHERE code_region = %s ORDER BY code_dept",
                    (region_id.strip(),),
                )
                dept_list = [row["code_dept"] for row in cur.fetchall()]
            except (psycopg2.Error, KeyError, TypeError):
                dept_list = None
            if not dept_list:
                raise HTTPException(status_code=400, detail="Région inconnue")
        elif niveau == "department":
            if not code_dept:
                raise HTTPException(status_code=400, detail="code_dept requis pour niveau=department")
            dept_list = [code_dept]
        else:
            if not code_dept or not commune:
                raise HTTPException(status_code=400, detail="code_dept et commune requis pour niveau=commune")
            dept_list = [code_dept]
        # Type (noms normalisés : capitales sans accent)
        if type_local:
            types = [type_local]
        else:
            types = ["Appartement", "Maison"]
        types_norm = [_normalize_name_canonical(t) for t in types]
        # vf_communes peut stocker code_dept sur 5 chiffres → normaliser pour la requête
        dept_list_vf = [_normalize_code_dept_for_vf(d) for d in dept_list]
        placeholders_dept = ",".join(["%s"] * len(dept_list_vf))
        placeholders_type = ",".join(["%s"] * len(types_norm))
        sql = f"""
        SELECT annee, code_dept, code_postal, commune, type_local, nb_ventes,
               prix_moyen, prix_q1, prix_median, prix_q3, surface_moyenne, surface_mediane,
               prix_m2_moyenne, prix_m2_q1, prix_m2_mediane, prix_m2_q3,
               prix_med_s1, surf_med_s1, prix_m2_w_s1, prix_med_s2, surf_med_s2, prix_m2_w_s2,
               prix_med_s3, surf_med_s3, prix_m2_w_s3, prix_med_s4, surf_med_s4, prix_m2_w_s4,
               prix_med_s5, surf_med_s5, prix_m2_w_s5,
               prix_med_T1, surf_med_T1, prix_m2_w_T1, prix_med_T2, surf_med_T2, prix_m2_w_T2,
               prix_med_T3, surf_med_T3, prix_m2_w_T3, prix_med_T4, surf_med_T4, prix_m2_w_T4,
               prix_med_T5, surf_med_T5, prix_m2_w_T5
        FROM foncier.vf_communes
        WHERE code_dept IN ({placeholders_dept})
          AND {_sql_norm_name_canonical("type_local")} IN ({placeholders_type})
          AND annee BETWEEN %s AND %s
        """
        params = list(dept_list_vf) + list(types_norm) + [annee_min, annee_max]
        if niveau == "commune":
            sql += " AND " + _sql_norm_name_canonical_commune_vf("commune") + " = %s"
            params.extend([_normalize_name_canonical(commune)])
        _debug_log("[stats] SQL (exécutable): %s", _debug_sql_params(sql, params))
        cur.execute(sql, params)
        # Avec dictionary=True, fetchall() renvoie déjà des dicts (clés = noms de colonnes)
        rows = cur.fetchall()
        # Pour niveau commune : code postal principal + liste formatée depuis ref_communes
        codes_postaux_display: str = code_postal or "—"
        nom_standard_commune: Optional[str] = None
        dep_nom_ref = None
        reg_nom_ref = None
        epci_nom_ref = None
        population_ref = None
        loypredm2_ref = None
        code_insee_ref: Optional[str] = None
        if niveau == "commune" and code_dept and commune:
            try:
                ref_sql = (
                    "SELECT code_postal, codes_postaux, nom_standard, dep_nom, dep_code, reg_nom, epci_nom, population, code_insee "
                    "FROM foncier.ref_communes "
                    "WHERE dep_code = %s AND " + _sql_norm_name_canonical("nom_standard_majuscule") + " = %s ORDER BY code_postal"
                )
                ref_params = (code_dept, _normalize_name_canonical(commune))
                cur.execute(ref_sql, ref_params)
                ref_rows = cur.fetchall()
                if ref_rows:
                    row = ref_rows[0]
                    nom_standard_commune = row.get("nom_standard") or commune
                    dep_nom_ref = row.get("dep_nom")
                    reg_nom_ref = row.get("reg_nom")
                    epci_nom_ref = row.get("epci_nom")
                    population_ref = row.get("population")
                    if population_ref is not None and isinstance(population_ref, Decimal):
                        population_ref = int(population_ref)
                    principal = str(row.get("code_postal") or code_postal or "").strip()
                    code_insee_ref = row.get("code_insee")
                    if code_insee_ref:
                        try:
                            cur.execute(
                                "SELECT AVG(loypredm2) AS loypredm2 FROM foncier.loyers_communes "
                                "WHERE insee_c = %s AND annee = (SELECT MAX(annee) FROM foncier.loyers_communes WHERE insee_c = %s)",
                                (code_insee_ref, code_insee_ref),
                            )
                            lr = cur.fetchone()
                            if lr and lr.get("loypredm2") is not None:
                                loypredm2_ref = float(lr["loypredm2"]) if isinstance(lr["loypredm2"], Decimal) else lr["loypredm2"]
                        except (psycopg2.Error, KeyError, TypeError):
                            pass
                        if loypredm2_ref is None and ref_params:
                            try:
                                commune_norm_stats = ref_params[1]
                                # Ville à arrondissements : moyenne pondérée par nbobs_com
                                cur.execute(
                                    "SELECT SUM(l.loypredm2 * COALESCE(l.nbobs_com, 0)) / NULLIF(SUM(COALESCE(l.nbobs_com, 0)), 0) AS loypredm2 "
                                    "FROM foncier.loyers_communes l "
                                    "WHERE " + _sql_libgeo_ville_canonical("l.libgeo") + " = %s "
                                    "AND l.annee = (SELECT MAX(annee) FROM foncier.loyers_communes l2 WHERE " + _sql_libgeo_ville_canonical("l2.libgeo") + " = %s)",
                                    (commune_norm_stats, commune_norm_stats),
                                )
                                lr = cur.fetchone()
                                if lr and lr.get("loypredm2") is not None:
                                    loypredm2_ref = float(lr["loypredm2"]) if isinstance(lr["loypredm2"], Decimal) else lr["loypredm2"]
                            except (psycopg2.Error, KeyError, TypeError):
                                pass
                    raw = row.get("codes_postaux")
                    if isinstance(raw, list):
                        liste_cp = [str(x).strip() for x in raw if x]
                    elif isinstance(raw, str):
                        liste_cp = [x.strip() for x in raw.replace(";", ",").split(",") if x.strip()]
                    else:
                        liste_cp = [principal] if principal else []
                    # Supprimer le principal, tri numérique, dédoublonnage ; afficher au plus 2 supplémentaires + "..."
                    def sort_key(s: str):
                        try:
                            return (0, int(s))
                        except (ValueError, TypeError):
                            return (1, s)
                    extras_all = sorted(
                        {c for c in liste_cp if c and c != principal},
                        key=sort_key,
                    )
                    autres = extras_all[:2]
                    if principal:
                        codes_postaux_display = principal
                        if autres:
                            codes_postaux_display += " (" + ", ".join(autres)
                            if len(extras_all) > 2:
                                codes_postaux_display += ", ..."
                            codes_postaux_display += ")"
                    else:
                        codes_postaux_display = " (" + ", ".join(autres) + ("..." if len(extras_all) > 2 else "") + ")" if autres else "—"
                else:
                    nom_standard_commune = commune
                    codes_postaux_display = code_postal or "—"
                #print("[ref_communes] nb lignes:", len(ref_rows), "| codes_postaux_display:", codes_postaux_display)
            except (psycopg2.Error, KeyError, TypeError):
                nom_standard_commune = commune
                codes_postaux_display = code_postal or "—"
        cur.close()
        # Normaliser Decimal
        for r in rows:
            for k, v in r.items():
                if isinstance(v, Decimal):
                    r[k] = float(v)
        # Agrégat global
        global_agg = _agg_rows(rows, surface_cat, pieces_cat) if rows else {}
        # Série par année
        by_year = {}
        for r in rows:
            y = r["annee"]
            if y not in by_year:
                by_year[y] = []
            by_year[y].append(r)
        series = []
        for y in sorted(by_year.keys()):
            agg_y = _agg_rows(by_year[y], surface_cat, pieces_cat)
            agg_y["annee"] = y
            series.append(agg_y)
        result: dict = {"global": global_agg, "series": series}
        if niveau == "commune":
            result["codes_postaux_display"] = codes_postaux_display
            result["nom_standard"] = nom_standard_commune
            result["dep_nom"] = dep_nom_ref
            result["dep_code"] = code_dept
            result["reg_nom"] = reg_nom_ref
            result["epci_nom"] = epci_nom_ref
            result["population"] = population_ref
            result["loypredm2"] = loypredm2_ref
            result["code_insee"] = code_insee_ref
        return result
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Erreur PostgreSQL : {e}")
    finally:
        if conn is not None:
            conn.close()


# Clés toujours présentes dans la réponse fiche-logement (même ordre, null si absent)
_FICHE_PAYLOAD_KEYS = (
    "code_insee", "parc", "ventes", "locations", "fiscalite",
    "rentabilite_mediane", "rentabilite_moyenne",
    "rentabilite_mediane_par_periode", "rentabilite_moyenne_par_periode",
    "loypredm2", "prix_m2_moyenne",
    "rentabilite_tranches",
)


def _normalize_fiche_payload(pl: dict) -> dict:
    """Retourne un dict avec exactement les clés attendues, dans l'ordre fixe (null si manquant)."""
    if not isinstance(pl, dict):
        return {k: None for k in _FICHE_PAYLOAD_KEYS}
    return {k: pl.get(k) for k in _FICHE_PAYLOAD_KEYS}


@app.get("/api/fiche-logement")
def get_fiche_logement(
    code_dept: str = Query(..., description="Code département"),
    code_postal: str = Query(..., description="Code postal"),
    commune: str = Query(..., description="Nom de la commune"),
):
    """
    Données pour le Panorama Logement : parc (agreg_communes_dvf), ventes (vf_communes),
    locations (loyers_communes), rentabilités (médianes et moyennes).
    Réponse : toujours les mêmes clés (code_insee, parc, ventes, locations, fiscalite,
    rentabilite_mediane, rentabilite_moyenne, loypredm2, prix_m2_moyenne). ventes / prix_m2_moyenne /
    rentabilite_* sont null si la commune n’a pas de lignes dans vf_communes (ex. libellé différent ou pas de ventes).
    """
    code_dept = (code_dept or "").strip()
    code_postal = (code_postal or "").strip()
    commune = (commune or "").strip()
    if not code_dept or not code_postal or not commune:
        raise HTTPException(status_code=400, detail="code_dept, code_postal et commune requis")
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        # 1) Résoudre code_insee depuis ref_communes (reg_nom, nom_standard pour comparaison_scores)
        cur.execute(
            "SELECT code_insee, reg_nom, nom_standard FROM foncier.ref_communes "
            "WHERE dep_code = %s AND " + _sql_norm_name_canonical("nom_standard_majuscule") + " = %s ORDER BY code_postal LIMIT 1",
            (code_dept, _normalize_name_canonical(commune)),
        )
        ref_row = cur.fetchone()
        code_insee = str(ref_row["code_insee"]).strip() if ref_row and ref_row.get("code_insee") else None
        if not code_insee:
            return _normalize_fiche_payload({"code_insee": None, "parc": None, "ventes": None, "locations": None, "fiscalite": None, "rentabilite_mediane": None, "rentabilite_moyenne": None, "loypredm2": None, "prix_m2_moyenne": None})

        # Utiliser le libellé commune tel qu’il figure dans vf_communes pour les requêtes ventes
        # (même normalisation canonique que stats.js / ref_communes, mais évite écarts de libellé ref vs vf_communes)
        # code_dept / code_postal : vf_communes stocke 1 chiffre pour 01-09 et code postal sans zéros en tête (01500 → 1500)
        code_dept_vf = _normalize_code_dept_for_vf(code_dept)
        code_postal_vf = _normalize_code_postal_for_vf(code_postal)
        commune_norm = _normalize_name_canonical(commune)
        _debug_log("[fiche] code_insee=%s vf_communes lookup: code_dept_vf=%r code_postal_vf=%r commune_norm=%s",
            code_insee, code_dept_vf, code_postal_vf, commune_norm)
        # Important : pour gérer les communes à multiples codes postaux (ex. Paris),
        # on ne filtre plus par code_postal, uniquement par (code_dept, commune canonique).
        cur.execute(
            "SELECT commune FROM foncier.vf_communes "
            "WHERE code_dept = %s AND "
            + _sql_norm_name_canonical_commune_vf("commune")
            + " = %s LIMIT 1",
            (code_dept_vf, commune_norm),
        )
        vf_commune_row = cur.fetchone()
        # Si rien trouvé et code_dept sur 2 chiffres (ex. "01"), réessayer sans le 0 (ex. "1") car vf_communes peut stocker "1"
        if not vf_commune_row and code_dept_vf and len(code_dept_vf) == 2 and code_dept_vf.isdigit() and code_dept_vf.startswith("0"):
            code_dept_alt = code_dept_vf[1:]
            _debug_log("[fiche] code_insee=%s tentative code_dept_alt=%r (vf_communes sans 0)", code_insee, code_dept_alt)
            cur.execute(
                "SELECT commune FROM foncier.vf_communes "
                "WHERE code_dept = %s AND "
                + _sql_norm_name_canonical_commune_vf("commune")
                + " = %s LIMIT 1",
                (code_dept_alt, commune_norm),
            )
            vf_commune_row = cur.fetchone()
            if vf_commune_row:
                code_dept_vf = code_dept_alt
                _debug_log("[fiche] code_insee=%s vf_communes trouvé avec code_dept_alt=%r", code_insee, code_dept_alt)
        commune_ventes = (vf_commune_row.get("commune") if vf_commune_row and vf_commune_row.get("commune") else commune).strip()
        use_canonical_commune_ventes = bool(vf_commune_row and vf_commune_row.get("commune"))
        if vf_commune_row and vf_commune_row.get("commune"):
            _debug_log("[fiche] code_insee=%s commune_ventes trouvé vf_communes: %r", code_insee, commune_ventes)
        else:
            _debug_log("[fiche] code_insee=%s commune_ventes absent vf_communes, fallback param: %r (norm=%s)", code_insee, commune, commune_norm)
            # Diagnostic : quelles lignes existent dans vf_communes pour ce code_postal (tous code_dept proches) ?
            try:
                cur.execute(
                    "SELECT DISTINCT code_dept, commune FROM foncier.vf_communes WHERE code_postal = %s ORDER BY code_dept, commune",
                    (code_postal_vf,),
                )
                diag = cur.fetchall()
                if diag:
                    _debug_log("[fiche] code_insee=%s DIAG vf_communes pour code_postal_vf=%s: %s",
                        code_insee, code_postal_vf, [(r.get("code_dept"), r.get("commune")) for r in diag[:20]])
                else:
                    _debug_log("[fiche] code_insee=%s DIAG vf_communes pour code_postal_vf=%s: aucune ligne", code_insee, code_postal_vf)
            except Exception as e:
                _debug_log("[fiche] code_insee=%s DIAG vf_communes erreur: %s", code_insee, e)

        # Lecture cache fiche (évite recalcul si déjà calculé)
        try:
            cur.execute(
                "SELECT payload FROM foncier.fiche_logement_cache WHERE code_insee = %s",
                (code_insee,),
            )
            cache_row = cur.fetchone()
            if cache_row and cache_row.get("payload"):
                pl = cache_row["payload"]
                if isinstance(pl, dict):
                    pass
                else:
                    pl = json.loads(pl) if isinstance(pl, str) else {}
                _debug_log("[fiche] code_insee=%s retour cache (pas de recalcul)", code_insee)
                return _normalize_fiche_payload(pl)
        except (psycopg2.Error, TypeError, ValueError):
            pass

        def _float(v: Any) -> Optional[float]:
            if v is None: return None
            try: return float(v)
            except (TypeError, ValueError): return None

        # 2) Parc logements : on n'utilise plus agreg_communes_dvf (données jugées non fiables pour ce projet).
        #    parc_data reste vide pour l'instant, les calculs de rentabilité s'appuyant sur vf_communes + loyers_communes.
        parc_annee = None
        parc_data = None

        # 3) Ventes (vf_communes) — année max puis lignes
        # Important : pour Paris/Lyon/Marseille et autres villes à arrondissements,
        # agréger toutes les lignes vf_communes de la commune en utilisant la forme canonique
        # (même logique que pour loyers_communes via libgeo).
        ventes_where_commune = _sql_norm_name_canonical_commune_vf("commune") + " = %s"
        ventes_params_commune = (commune_norm,)
        _debug_log("[fiche] code_insee=%s ventes: comparaison canonique commune (norm=%s, commune_ventes=%r)", code_insee, commune_norm, commune_ventes)
        # Même logique : filtrer uniquement par code_dept + commune, pas par code_postal.
        cur.execute(
            "SELECT MAX(annee) AS annee_max FROM foncier.vf_communes "
            "WHERE code_dept = %s AND " + ventes_where_commune,
            (code_dept_vf,) + ventes_params_commune,
        )
        ventes_annee_row = cur.fetchone()
        ventes_annee = int(ventes_annee_row["annee_max"]) if ventes_annee_row and ventes_annee_row.get("annee_max") else None
        ventes_lignes: List[dict] = []
        vf_rows: List[dict] = []
        by_type: dict = {}
        ventes_lignes_by_period: Dict[int, List[dict]] = {}
        if not ventes_annee:
            _debug_log("[fiche] code_insee=%s ventes: aucune année (code_dept=%s code_postal=%s commune_ventes=%r canonical=%s)",
                code_insee, code_dept_vf, code_postal_vf, commune_ventes, use_canonical_commune_ventes)
        if ventes_annee:
            vf_rows_latest = _fetch_vf_communes_range(
                cur, code_dept_vf, ventes_where_commune, ventes_params_commune, ventes_annee, ventes_annee
            )
            vf_rows = vf_rows_latest
            ventes_lignes, by_type = _build_ventes_lignes_from_vf_rows(vf_rows_latest)
            ventes_lignes_by_period[1] = ventes_lignes
            prix_m2_l1 = vf_rows[0].get("prix_m2_moyenne") if vf_rows else None
            _debug_log("[fiche] code_insee=%s ventes: annee_max=%s nb_lignes_1a=%s prix_m2_moyenne(ligne0)=%s", code_insee, ventes_annee, len(vf_rows), prix_m2_l1)
            for P in (2, 3, 5):
                annee_min = ventes_annee - P + 1
                vf_p = _fetch_vf_communes_range(
                    cur, code_dept_vf, ventes_where_commune, ventes_params_commune, annee_min, ventes_annee
                )
                vl_p, _ = _build_ventes_lignes_from_vf_rows(vf_p)
                ventes_lignes_by_period[P] = vl_p

        ventes_payload = {"annee": ventes_annee, "lignes": ventes_lignes} if ventes_annee else None

        # 4) Locations (loyers_communes) — année max pour insee_c ; fallback par libgeo pour Paris/Lyon/Marseille (arrondissements)
        cur.execute(
            "SELECT MAX(annee) AS annee_max FROM foncier.loyers_communes WHERE insee_c = %s",
            (code_insee,),
        )
        loc_annee_row = cur.fetchone()
        loc_annee = int(loc_annee_row["annee_max"]) if loc_annee_row and loc_annee_row.get("annee_max") else None
        loc_via_libgeo = False
        if not loc_annee and commune_norm:
            cur.execute(
                "SELECT MAX(annee) AS annee_max FROM foncier.loyers_communes l "
                "WHERE " + _sql_libgeo_ville_canonical("l.libgeo") + " = %s",
                (commune_norm,),
            )
            loc_annee_row = cur.fetchone()
            loc_annee = int(loc_annee_row["annee_max"]) if loc_annee_row and loc_annee_row.get("annee_max") else None
            if loc_annee:
                loc_via_libgeo = True
                _debug_log("[fiche] code_insee=%s locations: fallback libgeo (arrondissements) annee=%s", code_insee, loc_annee)
        loc_lignes = []
        if not loc_annee:
            _debug_log("[fiche] code_insee=%s locations: aucune année loyers_communes", code_insee)
        if loc_annee:
            if loc_via_libgeo:
                cur.execute(
                    "SELECT type_bien, segment_surface, nbobs_com, loypredm2, lwr_ipm2, upr_ipm2 "
                    "FROM foncier.loyers_communes l WHERE " + _sql_libgeo_ville_canonical("l.libgeo") + " = %s AND l.annee = %s",
                    (commune_norm, loc_annee),
                )
            else:
                cur.execute(
                    "SELECT type_bien, segment_surface, nbobs_com, loypredm2, lwr_ipm2, upr_ipm2 "
                    "FROM foncier.loyers_communes WHERE insee_c = %s AND annee = %s",
                    (code_insee, loc_annee),
                )
            loc_rows = cur.fetchall()
            _debug_log("[fiche] code_insee=%s locations: annee=%s nb_lignes=%s", code_insee, loc_annee, len(loc_rows))
            # segment_surface: 'all', '1-2_pieces', '3_plus_pieces'
            # type_bien: 'maison', 'appartement'
            def loc_key(r):
                tb = (r.get("type_bien") or "").strip().lower()
                seg = (r.get("segment_surface") or "").strip()
                if seg == "1-2_pieces":
                    return "Apparts 1/2 pièces"
                if seg == "3_plus_pieces":
                    return "Apparts. 3p+"
                if tb == "maison":
                    return "Maisons"
                if tb == "appartement":
                    return "Appartements"
                return "Maisons/Appart." if seg == "all" else tb
            # Agrégat "Maisons/Appart." = lignes type_bien all + segment all (ou somme maison+appart segment all)
            by_loc_key = {}
            for r in loc_rows:
                k = loc_key(r)
                if k not in by_loc_key:
                    by_loc_key[k] = []
                by_loc_key[k].append(r)
            total_nb = 0
            total_loy = 0
            total_q1 = 0
            total_q3 = 0
            for r in loc_rows:
                seg = (r.get("segment_surface") or "").strip()
                if seg != "all":
                    continue
                n = _int(r.get("nbobs_com"))
                total_nb += n
                loy = _float(r.get("loypredm2"))
                q1 = _float(r.get("lwr_ipm2"))
                q3 = _float(r.get("upr_ipm2"))
                if loy is not None:
                    total_loy += loy * n
                if q1 is not None:
                    total_q1 += q1 * n
                if q3 is not None:
                    total_q3 += q3 * n
            if total_nb > 0:
                loc_lignes.append({
                    "type": "Maisons/Appart.",
                    "nb_loyers": total_nb,
                    "loyer_med_m2": round(total_loy / total_nb, 2),
                    "loyer_q1_m2": round(total_q1 / total_nb, 2) if total_q1 else None,
                    "loyer_q3_m2": round(total_q3 / total_nb, 2) if total_q3 else None,
                })
            for label, key in [("Maisons", "Maisons"), ("Appartements", "Appartements"), ("Apparts 1/2 pièces", "Apparts 1/2 pièces"), ("Apparts. 3p+", "Apparts. 3p+")]:
                rows = by_loc_key.get(key, [])
                if not rows:
                    if key in ("Apparts 1/2 pièces", "Apparts. 3p+"):
                        loc_lignes.append({"type": label, "nb_loyers": 0, "loyer_med_m2": None, "loyer_q1_m2": None, "loyer_q3_m2": None})
                    continue
                sn = sum(_int(r.get("nbobs_com")) for r in rows) or 1
                loc_lignes.append({
                    "type": label,
                    "nb_loyers": sum(_int(r.get("nbobs_com")) for r in rows),
                    "loyer_med_m2": round(sum((_float(r.get("loypredm2")) or 0) * _int(r.get("nbobs_com")) for r in rows) / sn, 2) if sn else None,
                    "loyer_q1_m2": round(sum((_float(r.get("lwr_ipm2")) or 0) * _int(r.get("nbobs_com")) for r in rows) / sn, 2) if sn else None,
                    "loyer_q3_m2": round(sum((_float(r.get("upr_ipm2")) or 0) * _int(r.get("nbobs_com")) for r in rows) / sn, 2) if sn else None,
                })
            # Proxy : loyer ANIL « appartements » pour parkings / locaux pro (pas de série ANIL dédiée ; prix DVF reste spécifique)
            _loc_appart = next((x for x in loc_lignes if x.get("type") == "Appartements"), None)
            if _loc_appart:
                for _plbl in (VENTE_TYPE_PARKING, VENTE_TYPE_LOCAL_INDUS):
                    if not any(x.get("type") == _plbl for x in loc_lignes):
                        loc_lignes.append({
                            "type": _plbl,
                            "nb_loyers": _loc_appart.get("nb_loyers"),
                            "loyer_med_m2": _loc_appart.get("loyer_med_m2"),
                            "loyer_q1_m2": _loc_appart.get("loyer_q1_m2"),
                            "loyer_q3_m2": _loc_appart.get("loyer_q3_m2"),
                        })

        locations_payload = {"annee": loc_annee, "lignes": loc_lignes} if loc_annee else None

        # 5) Fiscalité locale (tous les taux par année) et TFB pour simulation TF
        cur.execute(
            "SELECT annee, Taux_Global_TFNB, Taux_Global_TFB, Taux_TEOM FROM foncier.fiscalite_locale "
            "WHERE code_insee = %s ORDER BY annee DESC",
            (code_insee,),
        )
        fiscalite_rows = cur.fetchall()
        fiscalite_list = []
        taux_tfb = None
        taux_teom = None
        for fl_row in fiscalite_rows or []:
            annee = fl_row.get("annee")
            tfnb = _float(fl_row.get("Taux_Global_TFNB") or fl_row.get("taux_global_tfnb"))
            tfb = _float(fl_row.get("Taux_Global_TFB") or fl_row.get("taux_global_tfb"))
            teom = _float(fl_row.get("Taux_TEOM") or fl_row.get("taux_teom"))
            if annee is not None:
                fiscalite_list.append({"annee": int(annee), "taux_tfnb": tfnb, "taux_tfb": tfb, "taux_teom": teom})
            if taux_tfb is None and tfb is not None:
                taux_tfb = tfb
            if taux_teom is None and teom is not None:
                taux_teom = teom
        surface_moy_agreg = parc_data.get("surface_moy") if parc_data else None
        loyer_ref = None
        annee_loyer = loc_annee or parc_annee
        if annee_loyer:
            cur.execute(
                "SELECT AVG(loypredm2) AS loypredm2 FROM foncier.loyers_communes WHERE insee_c = %s AND annee = %s",
                (code_insee, annee_loyer),
            )
            lr_row = cur.fetchone()
            if lr_row and lr_row.get("loypredm2") is not None:
                loyer_ref = _float(lr_row["loypredm2"])
            if loyer_ref is None and commune_norm:
                # Ville à arrondissements (Paris, Lyon, Marseille) : moyenne pondérée par nbobs_com
                cur.execute(
                    "SELECT SUM(l.loypredm2 * COALESCE(l.nbobs_com, 0)) / NULLIF(SUM(COALESCE(l.nbobs_com, 0)), 0) AS loypredm2 "
                    "FROM foncier.loyers_communes l "
                    "WHERE " + _sql_libgeo_ville_canonical("l.libgeo") + " = %s AND l.annee = %s",
                    (commune_norm, annee_loyer),
                )
                lr_row = cur.fetchone()
                if lr_row and lr_row.get("loypredm2") is not None:
                    loyer_ref = _float(lr_row["loypredm2"])
                    _debug_log("[fiche] code_insee=%s loyer_ref fallback libgeo (pondéré nbobs_com): %s", code_insee, loyer_ref)
        # Taxe foncière simulée par année pour la colonne Fiscalité (surface_moy × 3 × loypredm2 × taux_tfb/100, valeur locative cadastrale)
        if surface_moy_agreg is not None and loyer_ref is not None:
            for f in fiscalite_list:
                tfb = f.get("taux_tfb")
                f["taxe_fonciere_simulee"] = round(surface_moy_agreg * 3 * loyer_ref * (tfb / 100.0), 2) if tfb is not None else None

        # 6) Rentabilités (médianes et moyennes) par type — fenêtres DVF 1, 2, 3 et 5 ans (loyers = dernière année ANIL)
        rentabilite_mediane_par_periode: Dict[str, Any] = {}
        rentabilite_moyenne_par_periode: Dict[str, Any] = {}
        renta_med: List[dict] = []
        renta_moy: List[dict] = []
        if ventes_annee:
            for P in PERIODES_DVF_ANNEES:
                vl_p = ventes_lignes_by_period.get(P) or []
                sk = str(P)
                annee_min = ventes_annee - P + 1
                nb_v = _int(vl_p[0].get("nb_ventes")) if vl_p and len(vl_p) > 0 else None
                if vl_p and loc_lignes:
                    med = _build_renta_lignes(
                        vl_p, loc_lignes, parc_data, loyer_ref, taux_tfb, taux_teom, use_median=True, code_insee=code_insee
                    )
                    moy = _build_renta_lignes(
                        vl_p, loc_lignes, parc_data, loyer_ref, taux_tfb, taux_teom, use_median=False, code_insee=code_insee
                    )
                else:
                    med = []
                    moy = []
                rentabilite_mediane_par_periode[sk] = {
                    "lignes": med,
                    "nb_ventes": nb_v,
                    "annee_debut": annee_min,
                    "annee_fin": ventes_annee,
                }
                rentabilite_moyenne_par_periode[sk] = {
                    "lignes": moy,
                    "nb_ventes": nb_v,
                    "annee_debut": annee_min,
                    "annee_fin": ventes_annee,
                }
            renta_med = (rentabilite_mediane_par_periode.get("1") or {}).get("lignes") or []
            renta_moy = (rentabilite_moyenne_par_periode.get("1") or {}).get("lignes") or []

        rentabilite_tranches = None
        if vf_rows and loc_lignes:
            rentabilite_tranches = _compute_rentabilite_tranches_nested(
                vf_rows, by_type, loc_lignes, parc_data, loyer_ref, taux_tfb, taux_teom, code_insee
            )

        prix_m2_moyenne_fiche = None
        if ventes_lignes and len(ventes_lignes) > 0:
            prix_m2_moyenne_fiche = ventes_lignes[0].get("prix_m2_moyenne")
        _debug_log("[fiche] code_insee=%s build payload: loyer_ref=%s prix_m2_moyenne_fiche=%s rentabilite_mediane_lignes=%s",
            code_insee, loyer_ref, prix_m2_moyenne_fiche, len(renta_med) if renta_med else 0)
        payload = _normalize_fiche_payload({
            "code_insee": code_insee,
            "parc": parc_data,
            "ventes": ventes_payload,
            "locations": locations_payload,
            "fiscalite": fiscalite_list if fiscalite_list else None,
            "rentabilite_mediane": {"lignes": renta_med} if renta_med else None,
            "rentabilite_moyenne": {"lignes": renta_moy} if renta_moy else None,
            "rentabilite_mediane_par_periode": rentabilite_mediane_par_periode if rentabilite_mediane_par_periode else None,
            "rentabilite_moyenne_par_periode": rentabilite_moyenne_par_periode if rentabilite_moyenne_par_periode else None,
            "loypredm2": float(loyer_ref) if loyer_ref is not None else None,
            "prix_m2_moyenne": float(prix_m2_moyenne_fiche) if prix_m2_moyenne_fiche is not None else None,
            "rentabilite_tranches": rentabilite_tranches,
        })
        # Écriture cache pour les prochaines requêtes
        try:
            def _json_default(obj):
                if isinstance(obj, Decimal):
                    return float(obj)
                raise TypeError(type(obj))
            payload_json = json.dumps(payload, default=_json_default)
            cur.execute(
                "INSERT INTO foncier.fiche_logement_cache (code_insee, payload, updated_at) "
                "VALUES (%s, %s, clock_timestamp()) "
                "ON CONFLICT (code_insee) DO UPDATE SET payload = EXCLUDED.payload, updated_at = clock_timestamp()",
                (code_insee, payload_json),
            )
            conn.commit()
        except (psycopg2.Error, TypeError, ValueError):
            pass
        cur.close()
        return payload
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Erreur PostgreSQL : {e}")
    finally:
        if conn is not None:
            conn.close()


@app.get("/api/panorama-ventes")
def get_panorama_ventes(
    code_dept: str = Query(..., description="Code département"),
    code_postal: str = Query(..., description="Code postal"),
    commune: str = Query(..., description="Nom de la commune"),
    periode_annees: int = Query(1, ge=1, le=5, description="Période en années (1, 2, 3 ou 5)"),
    surface_cat: str = Query("", description="Catégorie surface (S1-S5) ou vide"),
    pieces_cat: str = Query("", description="Catégorie pièces (T1-T5) ou vide"),
):
    """Retourne les lignes de ventes DVF agrégées pour la commune, la période et les filtres demandés."""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        code_dept_vf = _normalize_code_dept_for_vf(code_dept)
        commune_norm = _normalize_name_canonical(commune)
        where_commune = _sql_norm_name_canonical_commune_vf("commune") + " = %s"
        params_commune = (commune_norm,)

        # Année max disponible dans vf_communes pour cette commune
        cur.execute(
            "SELECT MAX(annee) AS annee_max FROM foncier.vf_communes "
            "WHERE code_dept = %s AND " + where_commune,
            (code_dept_vf,) + params_commune,
        )
        row = cur.fetchone()
        annee_max = int(row["annee_max"]) if row and row.get("annee_max") else None

        # Réessayer sans le 0 initial si rien trouvé
        if annee_max is None and code_dept_vf and len(code_dept_vf) == 2 and code_dept_vf.isdigit() and code_dept_vf.startswith("0"):
            code_dept_alt = code_dept_vf[1:]
            cur.execute(
                "SELECT MAX(annee) AS annee_max FROM foncier.vf_communes "
                "WHERE code_dept = %s AND " + where_commune,
                (code_dept_alt,) + params_commune,
            )
            row2 = cur.fetchone()
            if row2 and row2.get("annee_max"):
                annee_max = int(row2["annee_max"])
                code_dept_vf = code_dept_alt

        if not annee_max:
            return {"annee_min": None, "annee_max": None, "lignes": []}

        annee_min = annee_max - periode_annees + 1
        vf_rows = _fetch_vf_communes_range(cur, code_dept_vf, where_commune, params_commune, annee_min, annee_max)

        sc = surface_cat.strip() or None
        pc = pieces_cat.strip() or None
        if sc or pc:
            _, by_type = _build_ventes_lignes_from_vf_rows(vf_rows)
            lignes = _build_ventes_lignes_for_tranche(vf_rows, by_type, sc, pc)
        else:
            lignes, _ = _build_ventes_lignes_from_vf_rows(vf_rows)

        return {"annee_min": annee_min, "annee_max": annee_max, "lignes": lignes}
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Erreur PostgreSQL : {e}")
    finally:
        if conn is not None:
            conn.close()


def _indicators_from_fiche_payload(
    fiche: dict,
    code_insee: str,
    code_dept: str,
    code_postal: str,
    commune: str,
    reg_nom: Optional[str] = None,
    dep_nom: Optional[str] = None,
    population: Optional[int] = None,
) -> dict:
    """Construit un dict ligne pour indicateurs_communes / comparaison_scores à partir du payload fiche (cache, avec loypredm2 et prix_m2_moyenne).

    Calcul renta_brute (pas une requête SQL unique) :
      renta_brute = (loypredm2 * 12 / prix_m2_moyenne) * 100  [en %]
    - loypredm2 : loyer de référence €/m² (depuis loyers_communes par code_insee, ou agrégé des lignes locations du payload).
    - prix_m2_moyenne : prix moyen au m² (depuis vf_communes par code_dept, code_postal, commune ; commune comparée en forme canonique, cf. stats.js normalizeNameCanonical).
    Si l’un des deux manque (ex. aucune vente pour la commune dans vf_communes), renta_brute reste NULL.
    """
    def _to_float(v):
        if v is None:
            return None
        if isinstance(v, Decimal):
            return float(v)
        try:
            return float(v)
        except (TypeError, ValueError):
            return None

    loypredm2 = _to_float(fiche.get("loypredm2"))
    prix_m2_moy = _to_float(fiche.get("prix_m2_moyenne"))
    _debug_log("[indicators] code_insee=%s entrée: loypredm2=%s prix_m2_moyenne=%s (racine payload)", code_insee, loypredm2, prix_m2_moy)
    # Fallback : ancien cache ou payload sans champs racine → dériver depuis ventes/locations
    if prix_m2_moy is None and fiche.get("ventes") and fiche["ventes"].get("lignes"):
        vl = fiche["ventes"]["lignes"]
        if vl:
            prix_m2_moy = _to_float(vl[0].get("prix_m2_moyenne"))
            _debug_log("[indicators] code_insee=%s fallback prix_m2_moy depuis ventes.lignes[0]: %s", code_insee, prix_m2_moy)
    if loypredm2 is None and fiche.get("locations") and fiche["locations"].get("lignes"):
        ll = fiche["locations"]["lignes"]
        total_n, total_loy = 0, 0.0
        for lin in ll:
            n = _int(lin.get("nb_loyers"))
            loy = _to_float(lin.get("loyer_med_m2"))
            if n and loy is not None:
                total_n += n
                total_loy += loy * n
        if total_n > 0:
            loypredm2 = round(total_loy / total_n, 2)
            _debug_log("[indicators] code_insee=%s fallback loypredm2 depuis locations.lignes (pondéré): %s", code_insee, loypredm2)

    renta_brute = None
    if loypredm2 is not None and prix_m2_moy is not None and prix_m2_moy > 0:
        try:
            renta_brute = round((float(loypredm2) * 12 / float(prix_m2_moy)) * 100, 2)
            _debug_log("[indicators] code_insee=%s renta_brute calculée: %s", code_insee, renta_brute)
        except (TypeError, ValueError):
            _debug_log("[indicators] code_insee=%s renta_brute non calculée (exception)", code_insee)
    else:
        _debug_log("[indicators] code_insee=%s renta_brute NULL (loypredm2=%s prix_m2_moy=%s)", code_insee, loypredm2, prix_m2_moy)
    renta_nette = None
    renta_brute_maisons = renta_nette_maisons = renta_brute_appts = renta_nette_appts = None
    renta_brute_parking = renta_nette_parking = None
    renta_brute_local_indus = renta_nette_local_indus = None
    renta_brute_terrain = renta_nette_terrain = None
    renta_brute_immeuble = renta_nette_immeuble = None
    nb_locaux = nb_locaux_maisons = nb_locaux_appts = None
    if fiche.get("rentabilite_mediane") and fiche["rentabilite_mediane"].get("lignes"):
        lignes = fiche["rentabilite_mediane"]["lignes"]
        rx = _extract_rentas_from_lignes(lignes)
        nb_locaux = rx.get("nb_locaux")
        nb_locaux_maisons = rx.get("nb_locaux_maisons")
        nb_locaux_appts = rx.get("nb_locaux_appts")
        renta_nette = rx.get("renta_nette")
        renta_brute_maisons = rx.get("renta_brute_maisons")
        renta_nette_maisons = rx.get("renta_nette_maisons")
        renta_brute_appts = rx.get("renta_brute_appts")
        renta_nette_appts = rx.get("renta_nette_appts")
        renta_brute_parking = rx.get("renta_brute_parking")
        renta_nette_parking = rx.get("renta_nette_parking")
        renta_brute_local_indus = rx.get("renta_brute_local_indus")
        renta_nette_local_indus = rx.get("renta_nette_local_indus")
        renta_brute_terrain = rx.get("renta_brute_terrain")
        renta_nette_terrain = rx.get("renta_nette_terrain")
        renta_brute_immeuble = rx.get("renta_brute_immeuble")
        renta_nette_immeuble = rx.get("renta_nette_immeuble")
        _debug_log("[indicators] code_insee=%s rentabilite_mediane: %s lignes → renta_nette=%s", code_insee, len(lignes), renta_nette)
    else:
        _debug_log("[indicators] code_insee=%s renta_nette NULL (pas de rentabilite_mediane.lignes)", code_insee)
    taux_tfb = taux_teom = None
    if fiche.get("fiscalite") and len(fiche["fiscalite"]) > 0:
        taux_tfb = fiche["fiscalite"][0].get("taux_tfb")
        taux_teom = fiche["fiscalite"][0].get("taux_teom")
    tr_flat = _flatten_tranche_nested_to_indicator_row(fiche.get("rentabilite_tranches"))
    _debug_log("[indicators] code_insee=%s nb_locaux_maisons_s1=%s nb_locaux_agg_s1=%s from tr_flat",
               code_insee, tr_flat.get("nb_locaux_maisons_s1"), tr_flat.get("nb_locaux_agg_s1"))
    nb_ventes_dvf = None
    mpp0 = fiche.get("rentabilite_mediane_par_periode") or {}
    if isinstance(mpp0, dict) and mpp0.get("1"):
        nb_ventes_dvf = mpp0["1"].get("nb_ventes")
        if nb_ventes_dvf is not None:
            try:
                nb_ventes_dvf = int(nb_ventes_dvf)
            except (TypeError, ValueError):
                nb_ventes_dvf = None
    indicateurs_par_periode = _build_indicateurs_par_periode_json(fiche)
    out = {
        "code_insee": code_insee,
        "code_dept": code_dept,
        "code_postal": code_postal,
        "commune": commune,
        "region": reg_nom or "",
        "nb_locaux": int(nb_locaux) if nb_locaux is not None else None,
        "nb_locaux_maisons": int(nb_locaux_maisons) if nb_locaux_maisons is not None else None,
        "nb_locaux_appts": int(nb_locaux_appts) if nb_locaux_appts is not None else None,
        "nb_ventes_dvf": nb_ventes_dvf,
        "indicateurs_par_periode": indicateurs_par_periode,
        "renta_brute": renta_brute,
        "renta_nette": renta_nette,
        "renta_brute_maisons": round(renta_brute_maisons, 2) if renta_brute_maisons is not None else None,
        "renta_nette_maisons": round(renta_nette_maisons, 2) if renta_nette_maisons is not None else None,
        "renta_brute_appts": round(renta_brute_appts, 2) if renta_brute_appts is not None else None,
        "renta_nette_appts": round(renta_nette_appts, 2) if renta_nette_appts is not None else None,
        "renta_brute_parking": round(renta_brute_parking, 2) if renta_brute_parking is not None else None,
        "renta_nette_parking": round(renta_nette_parking, 2) if renta_nette_parking is not None else None,
        "renta_brute_local_indus": round(renta_brute_local_indus, 2) if renta_brute_local_indus is not None else None,
        "renta_nette_local_indus": round(renta_nette_local_indus, 2) if renta_nette_local_indus is not None else None,
        "renta_brute_terrain": round(renta_brute_terrain, 2) if renta_brute_terrain is not None else None,
        "renta_nette_terrain": round(renta_nette_terrain, 2) if renta_nette_terrain is not None else None,
        "renta_brute_immeuble": round(renta_brute_immeuble, 2) if renta_brute_immeuble is not None else None,
        "renta_nette_immeuble": round(renta_nette_immeuble, 2) if renta_nette_immeuble is not None else None,
        "taux_tfb": float(taux_tfb) if taux_tfb is not None else None,
        "taux_teom": float(taux_teom) if taux_teom is not None else None,
        "dep_nom": dep_nom,
        "population": population,
    }
    for k in TRANCHE_RENTA_COLS:
        out[k] = _round_indicator_optional(tr_flat.get(k))
    for k in NB_LOCAUX_TRANCHE_COLS:
        v = tr_flat.get(k)
        out[k] = int(v) if v is not None else None
    return out


def _compute_one_commune_indicators(stats: dict, fiche: dict, c_dept: str, c_postal: str, c_commune: str) -> dict:
    """À partir de get_stats et get_fiche_logement, produit un dict avec renta_brute, renta_nette, *_maisons, *_appts, taux_tfb, taux_teom, region, commune."""
    g = (stats.get("global") or {})
    loypredm2 = stats.get("loypredm2")
    prix_m2_moy = g.get("prix_m2_moyenne")
    renta_brute = None
    if loypredm2 is not None and prix_m2_moy is not None and prix_m2_moy > 0:
        try:
            renta_brute = round((float(loypredm2) * 12 / float(prix_m2_moy)) * 100, 2)
        except (TypeError, ValueError):
            pass
    renta_nette = None
    renta_brute_maisons = renta_nette_maisons = renta_brute_appts = renta_nette_appts = None
    renta_brute_parking = renta_nette_parking = None
    renta_brute_local_indus = renta_nette_local_indus = None
    renta_brute_terrain = renta_nette_terrain = None
    renta_brute_immeuble = renta_nette_immeuble = None
    if fiche.get("rentabilite_mediane") and fiche["rentabilite_mediane"].get("lignes"):
        lignes = fiche["rentabilite_mediane"]["lignes"]
        rx = _extract_rentas_from_lignes(lignes)
        renta_nette = rx.get("renta_nette")
        renta_brute_maisons = rx.get("renta_brute_maisons")
        renta_nette_maisons = rx.get("renta_nette_maisons")
        renta_brute_appts = rx.get("renta_brute_appts")
        renta_nette_appts = rx.get("renta_nette_appts")
        renta_brute_parking = rx.get("renta_brute_parking")
        renta_nette_parking = rx.get("renta_nette_parking")
        renta_brute_local_indus = rx.get("renta_brute_local_indus")
        renta_nette_local_indus = rx.get("renta_nette_local_indus")
        renta_brute_terrain = rx.get("renta_brute_terrain")
        renta_nette_terrain = rx.get("renta_nette_terrain")
        renta_brute_immeuble = rx.get("renta_brute_immeuble")
        renta_nette_immeuble = rx.get("renta_nette_immeuble")
    taux_tfb = taux_teom = None
    if fiche.get("fiscalite") and len(fiche["fiscalite"]) > 0:
        taux_tfb = fiche["fiscalite"][0].get("taux_tfb")
        taux_teom = fiche["fiscalite"][0].get("taux_teom")
    region = stats.get("reg_nom") or ""
    nom_commune = stats.get("nom_standard") or c_commune
    tr_flat = _flatten_tranche_nested_to_indicator_row(fiche.get("rentabilite_tranches"))
    out_cc = {
        "code_dept": c_dept,
        "code_postal": c_postal,
        "commune": nom_commune,
        "region": region,
        "renta_brute": renta_brute,
        "renta_nette": renta_nette,
        "renta_brute_maisons": round(renta_brute_maisons, 2) if renta_brute_maisons is not None else None,
        "renta_nette_maisons": round(renta_nette_maisons, 2) if renta_nette_maisons is not None else None,
        "renta_brute_appts": round(renta_brute_appts, 2) if renta_brute_appts is not None else None,
        "renta_nette_appts": round(renta_nette_appts, 2) if renta_nette_appts is not None else None,
        "renta_brute_parking": round(renta_brute_parking, 2) if renta_brute_parking is not None else None,
        "renta_nette_parking": round(renta_nette_parking, 2) if renta_nette_parking is not None else None,
        "renta_brute_local_indus": round(renta_brute_local_indus, 2) if renta_brute_local_indus is not None else None,
        "renta_nette_local_indus": round(renta_nette_local_indus, 2) if renta_nette_local_indus is not None else None,
        "renta_brute_terrain": round(renta_brute_terrain, 2) if renta_brute_terrain is not None else None,
        "renta_nette_terrain": round(renta_nette_terrain, 2) if renta_nette_terrain is not None else None,
        "renta_brute_immeuble": round(renta_brute_immeuble, 2) if renta_brute_immeuble is not None else None,
        "renta_nette_immeuble": round(renta_nette_immeuble, 2) if renta_nette_immeuble is not None else None,
        "taux_tfb": float(taux_tfb) if taux_tfb is not None else None,
        "taux_teom": float(taux_teom) if taux_teom is not None else None,
    }
    for k in TRANCHE_RENTA_COLS:
        out_cc[k] = _round_indicator_optional(tr_flat.get(k))
    return out_cc


_RESOLVE_CHUNK_SIZE = 5000  # 5000 communes x 3 params = 15000 < limite PostgreSQL de 32767

def _resolve_communes_to_ref(cur, communes: List[tuple]) -> List[dict]:
    """Pour une liste de (code_dept, code_postal, commune), retourne les lignes ref_communes.
    Traite par chunks de _RESOLVE_CHUNK_SIZE pour rester sous la limite PostgreSQL de 32767 parametres."""
    if not communes:
        return []
    out: List[dict] = []
    #_debug_log("[_resolve_communes_to_ref] nb communes : %s dans communes", len(communes))
    #_debug_log("[_resolve_communes_to_ref] communes[0] : %s", communes[0])
    for i in range(0, len(communes), _RESOLVE_CHUNK_SIZE):
        chunk = communes[i: i + _RESOLVE_CHUNK_SIZE]
        try:
            placeholders = ", ".join("(%s, %s, %s)" for _ in chunk)
            params = []
            for (d, p, n) in chunk:
                params.extend([d, _normalize_code_postal_for_ref_communes(p), _normalize_name_canonical(n)])
            #_debug_log("[_resolve_communes_to_ref] params : %s", params)
            sql = (
                "SELECT c.code_insee, c.dep_code AS code_dept, c.code_postal, c.nom_standard AS commune, "
                "r.nom_region AS reg_nom, d.nom_dept AS dep_nom, "
                "GREATEST(COALESCE(c.population, 0)::int, 1) AS population "
                "FROM foncier.ref_communes c "
                "LEFT JOIN foncier.ref_departements d ON d.code_dept = c.dep_code "
                "LEFT JOIN foncier.ref_regions r ON r.code_region = d.code_region "
                "WHERE (c.dep_code, c.code_postal, " + _sql_norm_name_canonical("nom_standard_majuscule") + ") IN (" + placeholders + ")"
            )
            cur.execute(sql, params)
            out.extend(dict(r) for r in cur.fetchall())
        except psycopg2.Error:
            pass  # on continue les chunks suivants meme en cas d erreur partielle
    #_debug_log("[_resolve_communes_to_ref] out : %s", out)
    return out


_READ_INDIC_CHUNK_SIZE = 10000  # 10000 codes INSEE x 1 param = 10000 < limite PostgreSQL de 32767

def _read_indicateurs_communes(cur, code_insee_list: List[str]) -> dict:
    """Retourne un dict code_insee -> ligne (champs pour comparaison_scores).
    Traite par chunks de _READ_INDIC_CHUNK_SIZE pour rester sous la limite PostgreSQL de 32767 parametres."""
    if not code_insee_list:
        return {}
    out: dict = {}
    _base_select_communes = (
        "SELECT code_insee, code_dept, code_postal, commune, reg_nom, dep_nom, population, nb_locaux, nb_ventes_dvf, indicateurs_par_periode, "
        "renta_brute, renta_nette, renta_brute_maisons, renta_nette_maisons, renta_brute_appts, renta_nette_appts, "
        "renta_brute_parking, renta_nette_parking, renta_brute_local_indus, renta_nette_local_indus, "
        "renta_brute_terrain, renta_nette_terrain, renta_brute_immeuble, renta_nette_immeuble, "
        + ", ".join(TRANCHE_RENTA_COLS)
        + ", taux_tfb, taux_teom FROM foncier.indicateurs_communes WHERE code_insee IN "
    )
    _full_select_communes = (
        "SELECT code_insee, code_dept, code_postal, commune, reg_nom, dep_nom, population, nb_locaux, nb_locaux_maisons, nb_locaux_appts, nb_ventes_dvf, indicateurs_par_periode, "
        "renta_brute, renta_nette, renta_brute_maisons, renta_nette_maisons, renta_brute_appts, renta_nette_appts, "
        "renta_brute_parking, renta_nette_parking, renta_brute_local_indus, renta_nette_local_indus, "
        "renta_brute_terrain, renta_nette_terrain, renta_brute_immeuble, renta_nette_immeuble, "
        + ", ".join(TRANCHE_RENTA_COLS) + ", "
        + ", ".join(NB_LOCAUX_TRANCHE_COLS)
        + ", taux_tfb, taux_teom FROM foncier.indicateurs_communes WHERE code_insee IN "
    )
    for i in range(0, len(code_insee_list), _READ_INDIC_CHUNK_SIZE):
        chunk = code_insee_list[i: i + _READ_INDIC_CHUNK_SIZE]
        try:
            placeholders = ",".join(["%s"] * len(chunk))
            cur.execute("SAVEPOINT _rc_communes")
            try:
                cur.execute(_full_select_communes + "(" + placeholders + ")", chunk)
                rows = cur.fetchall()
                cur.execute("RELEASE SAVEPOINT _rc_communes")
            except psycopg2.Error:
                cur.execute("ROLLBACK TO SAVEPOINT _rc_communes")
                cur.execute("RELEASE SAVEPOINT _rc_communes")
                cur.execute(_base_select_communes + "(" + placeholders + ")", chunk)
                rows = cur.fetchall()
            for r in rows:
                ci = r.get("code_insee")
                if ci:
                    ipp = r.get("indicateurs_par_periode")
                    if isinstance(ipp, str):
                        try:
                            ipp = json.loads(ipp)
                        except (TypeError, ValueError):
                            ipp = None
                    row_d = {
                        "code_insee": str(ci).strip() if ci is not None else None,
                        "code_dept": r.get("code_dept"),
                        "code_postal": r.get("code_postal"),
                        "commune": r.get("commune"),
                        "region": r.get("reg_nom") or "",
                        "nb_locaux": int(r["nb_locaux"]) if r.get("nb_locaux") is not None else None,
                        "nb_locaux_maisons": int(r["nb_locaux_maisons"]) if r.get("nb_locaux_maisons") is not None else None,
                        "nb_locaux_appts": int(r["nb_locaux_appts"]) if r.get("nb_locaux_appts") is not None else None,
                        "nb_ventes_dvf": int(r["nb_ventes_dvf"]) if r.get("nb_ventes_dvf") is not None else None,
                        "indicateurs_par_periode": ipp if isinstance(ipp, dict) else None,
                        "renta_brute": float(r["renta_brute"]) if r.get("renta_brute") is not None else None,
                        "renta_nette": float(r["renta_nette"]) if r.get("renta_nette") is not None else None,
                        "renta_brute_maisons": float(r["renta_brute_maisons"]) if r.get("renta_brute_maisons") is not None else None,
                        "renta_nette_maisons": float(r["renta_nette_maisons"]) if r.get("renta_nette_maisons") is not None else None,
                        "renta_brute_appts": float(r["renta_brute_appts"]) if r.get("renta_brute_appts") is not None else None,
                        "renta_nette_appts": float(r["renta_nette_appts"]) if r.get("renta_nette_appts") is not None else None,
                        "renta_brute_parking": float(r["renta_brute_parking"]) if r.get("renta_brute_parking") is not None else None,
                        "renta_nette_parking": float(r["renta_nette_parking"]) if r.get("renta_nette_parking") is not None else None,
                        "renta_brute_local_indus": float(r["renta_brute_local_indus"]) if r.get("renta_brute_local_indus") is not None else None,
                        "renta_nette_local_indus": float(r["renta_nette_local_indus"]) if r.get("renta_nette_local_indus") is not None else None,
                        "renta_brute_terrain": float(r["renta_brute_terrain"]) if r.get("renta_brute_terrain") is not None else None,
                        "renta_nette_terrain": float(r["renta_nette_terrain"]) if r.get("renta_nette_terrain") is not None else None,
                        "renta_brute_immeuble": float(r["renta_brute_immeuble"]) if r.get("renta_brute_immeuble") is not None else None,
                        "renta_nette_immeuble": float(r["renta_nette_immeuble"]) if r.get("renta_nette_immeuble") is not None else None,
                        "taux_tfb": float(r["taux_tfb"]) if r.get("taux_tfb") is not None else None,
                        "taux_teom": float(r["taux_teom"]) if r.get("taux_teom") is not None else None,
                    }
                    _append_tranche_floats_from_db_row(r, row_d)
                    out[ci] = row_d
        except psycopg2.Error:
            pass  # on continue les chunks suivants meme en cas d erreur partielle
    return out


_INDIC_IDENTITY_COLS = ("code_insee", "code_dept", "code_postal", "commune", "reg_nom", "dep_nom", "population", "nb_locaux", "nb_locaux_maisons", "nb_locaux_appts", "nb_ventes_dvf")
_INDIC_RENTA_BASE_COLS = (
    "renta_brute", "renta_nette",
    "renta_brute_maisons", "renta_nette_maisons", "renta_brute_appts", "renta_nette_appts",
    "renta_brute_parking", "renta_nette_parking", "renta_brute_local_indus", "renta_nette_local_indus",
    "renta_brute_terrain", "renta_nette_terrain", "renta_brute_immeuble", "renta_nette_immeuble",
    "taux_tfb", "taux_teom",
)
_INDIC_ALL_SCORE_COLS = _INDIC_RENTA_BASE_COLS + TRANCHE_RENTA_COLS + NB_LOCAUX_TRANCHE_COLS

# Cache des colonnes existantes dans indicateurs_communes (peuplé au premier appel)
_indic_communes_cols_cache: Optional[Set[str]] = None


def _invalidate_indic_cols_cache() -> None:
    """Force le rechargement du cache de colonnes au prochain appel."""
    global _indic_communes_cols_cache
    _indic_communes_cols_cache = None


def _get_indic_communes_existing_cols(cur) -> Optional[Set[str]]:
    """Retourne l'ensemble des colonnes existantes dans foncier.indicateurs_communes (mis en cache).
    Retourne None si la requête échoue (on inclura alors toutes les colonnes sans filtrage)."""
    global _indic_communes_cols_cache
    if _indic_communes_cols_cache is not None:
        return _indic_communes_cols_cache
    try:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'foncier' AND table_name = 'indicateurs_communes'"
        )
        _indic_communes_cols_cache = {r["column_name"] for r in cur.fetchall()}
        _debug_log("[col-cache] colonnes indicateurs_communes: %d colonnes, nb_locaux_maisons_s1 present=%s",
                   len(_indic_communes_cols_cache), "nb_locaux_maisons_s1" in _indic_communes_cols_cache)
        return _indic_communes_cols_cache
    except psycopg2.Error:
        return None  # Ne pas mettre en cache l'échec — réessayera à la prochaine requête


def _compute_needed_columns(score_principal: str, scores_secondaires: Optional[List[str]], periode_annees: int) -> Tuple[List[str], bool]:
    """Calcule les colonnes SQL nécessaires à partir des scores demandés.
    Retourne (liste_colonnes_score, need_indicateurs_par_periode)."""
    needed = set()
    needed.add(score_principal)
    needed.add("nb_locaux")  # toujours utile pour filtrage nb_locaux_min
    # Ajouter automatiquement la colonne nb_locaux correspondante au score principal
    nb_locaux_col = _nb_locaux_col_for_score_key(score_principal)
    needed.add(nb_locaux_col)
    if scores_secondaires:
        needed.update(scores_secondaires)
    # Toujours inclure taux_tfb/taux_teom si demandés via scores_secondaires
    # Filtrer pour ne garder que les colonnes qui existent dans la table
    all_valid = set(_INDIC_ALL_SCORE_COLS)
    score_cols = [c for c in _INDIC_ALL_SCORE_COLS if c in needed]
    # Si un score demandé n'est pas dans les colonnes connues, inclure toutes les colonnes
    # (sécurité pour ne pas casser un appel avec un score inconnu)
    _always_known = {"nb_locaux", "nb_locaux_maisons", "nb_locaux_appts"} | set(NB_LOCAUX_TRANCHE_COLS)
    unknown = needed - all_valid - _always_known
    if unknown:
        score_cols = list(_INDIC_ALL_SCORE_COLS)
    need_periode = (periode_annees != 1)
    return score_cols, need_periode


def _read_indicateurs_by_scope(
    cur,
    scope: str,
    code_dept_list: Optional[List[str]] = None,
    code_region_list: Optional[List[str]] = None,
    code_insee_list: Optional[List[str]] = None,
    exclude_code_insee: Optional[List[str]] = None,
    exclude_code_dept: Optional[List[str]] = None,
    score_cols: Optional[List[str]] = None,
    need_periode: bool = True,
) -> dict:
    """Lecture directe depuis indicateurs_communes par scope géographique.
    Retourne un dict code_insee -> row (même format que _read_indicateurs_communes).

    scope:
      - 'all_france' : toutes les communes
      - 'department' : filtre par code_dept IN (code_dept_list)
      - 'region'     : filtre par code_dept IN (SELECT code_dept FROM ref_departements WHERE code_region IN (...))
      - 'communes'   : filtre par code_insee IN (code_insee_list)
    """
    t0 = _time.monotonic()

    # Colonnes à sélectionner — filtrer celles qui n'existent pas encore en DB (migrations non exécutées)
    existing_cols = _get_indic_communes_existing_cols(cur)
    if existing_cols is not None:
        id_cols = [c for c in _INDIC_IDENTITY_COLS if c in existing_cols]
        _requested_data = list(score_cols) if score_cols else list(_INDIC_ALL_SCORE_COLS)
        data_cols = [c for c in _requested_data if c in existing_cols]
        extra = ["indicateurs_par_periode"] if (need_periode and "indicateurs_par_periode" in existing_cols) else []
    else:
        id_cols = list(_INDIC_IDENTITY_COLS)
        _requested_data = list(score_cols) if score_cols else list(_INDIC_ALL_SCORE_COLS)
        data_cols = _requested_data
        extra = ["indicateurs_par_periode"] if need_periode else []
    all_cols = id_cols + extra + data_cols
    _nb_loc_in_data = [c for c in data_cols if c.startswith("nb_locaux")]
    _debug_log("[read-indic] score_cols=%s data_cols(nb_locaux)=%s existing_cols_has_cache=%s",
               score_cols, _nb_loc_in_data, existing_cols is not None)
    select_sql = "SELECT " + ", ".join(all_cols) + " FROM foncier.indicateurs_communes"

    # Construire WHERE
    where_parts: List[str] = []
    params: List[Any] = []

    if scope == "all_france":
        pass  # pas de WHERE
    elif scope == "department":
        if not code_dept_list:
            return {}
        placeholders = ",".join(["%s"] * len(code_dept_list))
        where_parts.append(f"code_dept IN ({placeholders})")
        params.extend(code_dept_list)
    elif scope == "region":
        if not code_region_list:
            return {}
        placeholders = ",".join(["%s"] * len(code_region_list))
        where_parts.append(f"code_dept IN (SELECT code_dept FROM foncier.ref_departements WHERE code_region IN ({placeholders}))")
        params.extend(code_region_list)
    elif scope == "communes":
        if not code_insee_list:
            return {}
        # Chunking pour rester sous la limite PostgreSQL
        pass  # traité ci-dessous
    else:
        return {}

    # Exclusions
    if exclude_code_insee:
        ph = ",".join(["%s"] * len(exclude_code_insee))
        where_parts.append(f"code_insee NOT IN ({ph})")
        params.extend(exclude_code_insee)
    if exclude_code_dept:
        ph = ",".join(["%s"] * len(exclude_code_dept))
        where_parts.append(f"code_dept NOT IN ({ph})")
        params.extend(exclude_code_dept)

    out: dict = {}

    if scope == "communes" and code_insee_list:
        # Chunked read par code_insee (comme _read_indicateurs_communes)
        base_where = " AND ".join(where_parts) if where_parts else ""
        base_params = list(params)
        for i in range(0, len(code_insee_list), _READ_INDIC_CHUNK_SIZE):
            chunk = code_insee_list[i: i + _READ_INDIC_CHUNK_SIZE]
            ph = ",".join(["%s"] * len(chunk))
            chunk_where = f"code_insee IN ({ph})"
            if base_where:
                full_where = base_where + " AND " + chunk_where
            else:
                full_where = chunk_where
            sql = select_sql + " WHERE " + full_where
            try:
                cur.execute(sql, base_params + chunk)
                rows = cur.fetchall()
                for r in rows:
                    ci = r.get("code_insee")
                    if ci:
                        out[ci] = _build_indicateur_row_from_db(r, data_cols, need_periode)
            except psycopg2.Error:
                pass
    else:
        # Requête unique (all_france, department, region)
        if where_parts:
            sql = select_sql + " WHERE " + " AND ".join(where_parts)
        else:
            sql = select_sql
        try:
            cur.execute(sql, params if params else None)
            rows = cur.fetchall()
            t_query = _time.monotonic()
            _debug_log("[comparaison_scores] _read_indicateurs_by_scope SQL: %.3fs, %d lignes", t_query - t0, len(rows))
            for r in rows:
                ci = r.get("code_insee")
                if ci:
                    out[ci] = _build_indicateur_row_from_db(r, data_cols, need_periode)
            t_build = _time.monotonic()
            _debug_log("[comparaison_scores] _read_indicateurs_by_scope build dicts: %.3fs", t_build - t_query)
        except psycopg2.Error as e:
            _debug_log("[comparaison_scores] _read_indicateurs_by_scope erreur SQL: %s", e)

    t_end = _time.monotonic()
    _debug_log("[comparaison_scores] _read_indicateurs_by_scope total: %.3fs, %d résultats (scope=%s)", t_end - t0, len(out), scope)
    return out


def _build_indicateur_row_from_db(r: dict, data_cols: List[str], need_periode: bool) -> dict:
    """Construit un dict indicateur depuis une ligne DB, ne convertissant que les colonnes demandées."""
    _ci = r.get("code_insee")
    row_d = {
        "code_insee": str(_ci).strip() if _ci is not None and str(_ci).strip() != "" else None,
        "code_dept": r.get("code_dept"),
        "code_postal": r.get("code_postal"),
        "commune": r.get("commune"),
        "region": r.get("reg_nom") or "",
        "nb_locaux": int(r["nb_locaux"]) if r.get("nb_locaux") is not None else None,
        "nb_locaux_maisons": int(r["nb_locaux_maisons"]) if r.get("nb_locaux_maisons") is not None else None,
        "nb_locaux_appts": int(r["nb_locaux_appts"]) if r.get("nb_locaux_appts") is not None else None,
        "nb_ventes_dvf": int(r["nb_ventes_dvf"]) if r.get("nb_ventes_dvf") is not None else None,
    }
    if need_periode:
        ipp = r.get("indicateurs_par_periode")
        if isinstance(ipp, str):
            try:
                ipp = json.loads(ipp)
            except (TypeError, ValueError):
                ipp = None
        row_d["indicateurs_par_periode"] = ipp if isinstance(ipp, dict) else None
    for col in data_cols:
        val = r.get(col)
        row_d[col] = float(val) if val is not None else None
    return row_d


def _upsert_indicateurs_communes(conn, cur, row: dict, commit: bool = True) -> Tuple[bool, Optional[str]]:
    """Insère ou met à jour une ligne dans indicateurs_communes. Retourne (ok, erreur)."""
    sp_name = _UPSERT_INDICATEURS_COMMUNES_SAVEPOINT
    savepoint_open = False
    try:
        if not commit:
            cur.execute(f"SAVEPOINT {sp_name}")
            savepoint_open = True
        cur.execute(_UPSERT_SQL_INDICATEURS_COMMUNES, _tuple_params_indicateurs_communes(row))
        if not commit:
            cur.execute(f"RELEASE SAVEPOINT {sp_name}")
            savepoint_open = False
        if commit:
            conn.commit()
        return True, None
    except (psycopg2.Error, TypeError, KeyError) as e:
        if not commit and savepoint_open:
            try:
                cur.execute(f"ROLLBACK TO SAVEPOINT {sp_name}")
                cur.execute(f"RELEASE SAVEPOINT {sp_name}")
            except psycopg2.Error:
                conn.rollback()
        elif commit:
            conn.rollback()
        return False, str(e)


def _read_indicateurs_depts(cur, code_dept_list: List[str]) -> dict:
    """Retourne un dict code_dept -> ligne agrégée pour comparaison_scores."""
    if not code_dept_list:
        return {}
    try:
        placeholders = ",".join(["%s"] * len(code_dept_list))
        _suffix = ", taux_tfb, taux_teom FROM foncier.indicateurs_depts WHERE code_dept IN (" + placeholders + ")"
        _base_select = (
            "SELECT code_dept, dep_nom, reg_nom, code_region, population, nb_locaux, nb_ventes_dvf, indicateurs_par_periode, "
            "renta_brute, renta_nette, renta_brute_maisons, renta_nette_maisons, renta_brute_appts, renta_nette_appts, "
            "renta_brute_parking, renta_nette_parking, renta_brute_local_indus, renta_nette_local_indus, "
            "renta_brute_terrain, renta_nette_terrain, renta_brute_immeuble, renta_nette_immeuble, "
            + ", ".join(TRANCHE_RENTA_COLS) + _suffix
        )
        _full_select = (
            "SELECT code_dept, dep_nom, reg_nom, code_region, population, nb_locaux, nb_locaux_maisons, nb_locaux_appts, nb_ventes_dvf, indicateurs_par_periode, "
            "renta_brute, renta_nette, renta_brute_maisons, renta_nette_maisons, renta_brute_appts, renta_nette_appts, "
            "renta_brute_parking, renta_nette_parking, renta_brute_local_indus, renta_nette_local_indus, "
            "renta_brute_terrain, renta_nette_terrain, renta_brute_immeuble, renta_nette_immeuble, "
            + ", ".join(TRANCHE_RENTA_COLS) + _suffix
        )
        cur.execute("SAVEPOINT _rd_depts")
        try:
            cur.execute(_full_select, code_dept_list)
            rows = cur.fetchall()
            cur.execute("RELEASE SAVEPOINT _rd_depts")
        except psycopg2.Error:
            cur.execute("ROLLBACK TO SAVEPOINT _rd_depts")
            cur.execute("RELEASE SAVEPOINT _rd_depts")
            cur.execute(_base_select, code_dept_list)
            rows = cur.fetchall()
        out = {}
        for r in rows:
            code_d = r.get("code_dept")
            if not code_d:
                continue
            ipp_d = r.get("indicateurs_par_periode")
            if isinstance(ipp_d, str):
                try:
                    ipp_d = json.loads(ipp_d)
                except (TypeError, ValueError):
                    ipp_d = None
            out[str(code_d)] = {
                "mode": "departement",
                "code_dept": str(code_d),
                "dep_nom": r.get("dep_nom") or str(code_d),
                "region": r.get("reg_nom") or "",
                "code_region": r.get("code_region"),
                "population": int(r["population"]) if r.get("population") is not None else None,
                "nb_locaux": int(r["nb_locaux"]) if r.get("nb_locaux") is not None else None,
                "nb_locaux_maisons": int(r["nb_locaux_maisons"]) if r.get("nb_locaux_maisons") is not None else None,
                "nb_locaux_appts": int(r["nb_locaux_appts"]) if r.get("nb_locaux_appts") is not None else None,
                "nb_ventes_dvf": int(r["nb_ventes_dvf"]) if r.get("nb_ventes_dvf") is not None else None,
                "indicateurs_par_periode": ipp_d if isinstance(ipp_d, dict) else None,
                "renta_brute": float(r["renta_brute"]) if r.get("renta_brute") is not None else None,
                "renta_nette": float(r["renta_nette"]) if r.get("renta_nette") is not None else None,
                "renta_brute_maisons": float(r["renta_brute_maisons"]) if r.get("renta_brute_maisons") is not None else None,
                "renta_nette_maisons": float(r["renta_nette_maisons"]) if r.get("renta_nette_maisons") is not None else None,
                "renta_brute_appts": float(r["renta_brute_appts"]) if r.get("renta_brute_appts") is not None else None,
                "renta_nette_appts": float(r["renta_nette_appts"]) if r.get("renta_nette_appts") is not None else None,
                "renta_brute_parking": float(r["renta_brute_parking"]) if r.get("renta_brute_parking") is not None else None,
                "renta_nette_parking": float(r["renta_nette_parking"]) if r.get("renta_nette_parking") is not None else None,
                "renta_brute_local_indus": float(r["renta_brute_local_indus"]) if r.get("renta_brute_local_indus") is not None else None,
                "renta_nette_local_indus": float(r["renta_nette_local_indus"]) if r.get("renta_nette_local_indus") is not None else None,
                "renta_brute_terrain": float(r["renta_brute_terrain"]) if r.get("renta_brute_terrain") is not None else None,
                "renta_nette_terrain": float(r["renta_nette_terrain"]) if r.get("renta_nette_terrain") is not None else None,
                "renta_brute_immeuble": float(r["renta_brute_immeuble"]) if r.get("renta_brute_immeuble") is not None else None,
                "renta_nette_immeuble": float(r["renta_nette_immeuble"]) if r.get("renta_nette_immeuble") is not None else None,
                "taux_tfb": float(r["taux_tfb"]) if r.get("taux_tfb") is not None else None,
                "taux_teom": float(r["taux_teom"]) if r.get("taux_teom") is not None else None,
            }
            _append_tranche_floats_from_db_row(r, out[str(code_d)])
        return out
    except psycopg2.Error:
        return {}


def _upsert_indicateurs_depts(conn, cur, row: dict, commit: bool = True) -> None:
    """Insère ou met à jour une ligne dans indicateurs_depts."""
    try:
        cur.execute(_UPSERT_SQL_INDICATEURS_DEPTS, _tuple_params_indicateurs_depts(row))
        if commit:
            conn.commit()
    except (psycopg2.Error, TypeError, KeyError):
        if commit:
            conn.rollback()
        pass


def _read_indicateurs_regions(cur, code_region_list: List[str]) -> dict:
    """Retourne un dict code_region -> ligne agrégée pour comparaison_scores."""
    if not code_region_list:
        return {}
    try:
        placeholders = ",".join(["%s"] * len(code_region_list))
        _suffix = ", taux_tfb, taux_teom FROM foncier.indicateurs_regions WHERE code_region IN (" + placeholders + ")"
        _base_select = (
            "SELECT code_region, reg_nom, population, nb_locaux, nb_ventes_dvf, indicateurs_par_periode, "
            "renta_brute, renta_nette, renta_brute_maisons, renta_nette_maisons, renta_brute_appts, renta_nette_appts, "
            "renta_brute_parking, renta_nette_parking, renta_brute_local_indus, renta_nette_local_indus, "
            "renta_brute_terrain, renta_nette_terrain, renta_brute_immeuble, renta_nette_immeuble, "
            + ", ".join(TRANCHE_RENTA_COLS) + _suffix
        )
        _full_select = (
            "SELECT code_region, reg_nom, population, nb_locaux, nb_locaux_maisons, nb_locaux_appts, nb_ventes_dvf, indicateurs_par_periode, "
            "renta_brute, renta_nette, renta_brute_maisons, renta_nette_maisons, renta_brute_appts, renta_nette_appts, "
            "renta_brute_parking, renta_nette_parking, renta_brute_local_indus, renta_nette_local_indus, "
            "renta_brute_terrain, renta_nette_terrain, renta_brute_immeuble, renta_nette_immeuble, "
            + ", ".join(TRANCHE_RENTA_COLS) + _suffix
        )
        cur.execute("SAVEPOINT _rd_regions")
        try:
            cur.execute(_full_select, code_region_list)
            rows = cur.fetchall()
            cur.execute("RELEASE SAVEPOINT _rd_regions")
        except psycopg2.Error:
            cur.execute("ROLLBACK TO SAVEPOINT _rd_regions")
            cur.execute("RELEASE SAVEPOINT _rd_regions")
            cur.execute(_base_select, code_region_list)
            rows = cur.fetchall()
        out = {}
        for r in rows:
            code_r = r.get("code_region")
            if not code_r:
                continue
            ipp_r = r.get("indicateurs_par_periode")
            if isinstance(ipp_r, str):
                try:
                    ipp_r = json.loads(ipp_r)
                except (TypeError, ValueError):
                    ipp_r = None
            out[str(code_r)] = {
                "mode": "region",
                "code_region": str(code_r),
                "region": r.get("reg_nom") or str(code_r),
                "population": int(r["population"]) if r.get("population") is not None else None,
                "nb_locaux": int(r["nb_locaux"]) if r.get("nb_locaux") is not None else None,
                "nb_locaux_maisons": int(r["nb_locaux_maisons"]) if r.get("nb_locaux_maisons") is not None else None,
                "nb_locaux_appts": int(r["nb_locaux_appts"]) if r.get("nb_locaux_appts") is not None else None,
                "nb_ventes_dvf": int(r["nb_ventes_dvf"]) if r.get("nb_ventes_dvf") is not None else None,
                "indicateurs_par_periode": ipp_r if isinstance(ipp_r, dict) else None,
                "renta_brute": float(r["renta_brute"]) if r.get("renta_brute") is not None else None,
                "renta_nette": float(r["renta_nette"]) if r.get("renta_nette") is not None else None,
                "renta_brute_maisons": float(r["renta_brute_maisons"]) if r.get("renta_brute_maisons") is not None else None,
                "renta_nette_maisons": float(r["renta_nette_maisons"]) if r.get("renta_nette_maisons") is not None else None,
                "renta_brute_appts": float(r["renta_brute_appts"]) if r.get("renta_brute_appts") is not None else None,
                "renta_nette_appts": float(r["renta_nette_appts"]) if r.get("renta_nette_appts") is not None else None,
                "renta_brute_parking": float(r["renta_brute_parking"]) if r.get("renta_brute_parking") is not None else None,
                "renta_nette_parking": float(r["renta_nette_parking"]) if r.get("renta_nette_parking") is not None else None,
                "renta_brute_local_indus": float(r["renta_brute_local_indus"]) if r.get("renta_brute_local_indus") is not None else None,
                "renta_nette_local_indus": float(r["renta_nette_local_indus"]) if r.get("renta_nette_local_indus") is not None else None,
                "renta_brute_terrain": float(r["renta_brute_terrain"]) if r.get("renta_brute_terrain") is not None else None,
                "renta_nette_terrain": float(r["renta_nette_terrain"]) if r.get("renta_nette_terrain") is not None else None,
                "renta_brute_immeuble": float(r["renta_brute_immeuble"]) if r.get("renta_brute_immeuble") is not None else None,
                "renta_nette_immeuble": float(r["renta_nette_immeuble"]) if r.get("renta_nette_immeuble") is not None else None,
                "taux_tfb": float(r["taux_tfb"]) if r.get("taux_tfb") is not None else None,
                "taux_teom": float(r["taux_teom"]) if r.get("taux_teom") is not None else None,
            }
            _append_tranche_floats_from_db_row(r, out[str(code_r)])
        return out
    except psycopg2.Error:
        return {}


def _upsert_indicateurs_regions(conn, cur, row: dict, commit: bool = True) -> None:
    """Insère ou met à jour une ligne dans indicateurs_regions."""
    try:
        cur.execute(_UPSERT_SQL_INDICATEURS_REGIONS, _tuple_params_indicateurs_regions(row))
        if commit:
            conn.commit()
    except (psycopg2.Error, TypeError, KeyError):
        if commit:
            conn.rollback()
        pass


def _get_communes_for_aggregation(cur, code_depts: Optional[List[str]] = None, code_regions: Optional[List[str]] = None) -> List[dict]:
    """
    Retourne une liste de communes (code_dept, code_postal, commune, population, dep_nom, reg_nom, code_region)
    pour les départements ou régions donnés. Une commune n'apparaît qu'une fois (dédoublonnage par code_insee si présent).
    """
    if not code_depts and not code_regions:
        return []
    try:
        if code_regions:
            cur.execute(
                """
                SELECT DISTINCT ON (COALESCE(c.code_insee, c.dep_code || '-' || c.nom_standard))
                  c.code_insee, c.dep_code AS code_dept, c.code_postal, c.nom_standard AS commune,
                  GREATEST(COALESCE(c.population, 0)::numeric, 1) AS population,
                  d.nom_dept AS dep_nom, r.nom_region AS reg_nom, d.code_region
                FROM foncier.ref_communes c
                JOIN foncier.ref_departements d ON d.code_dept = c.dep_code
                LEFT JOIN foncier.ref_regions r ON r.code_region = d.code_region
                WHERE d.code_region = ANY(%s)
                ORDER BY COALESCE(c.code_insee, c.dep_code || '-' || c.nom_standard), c.code_postal
                """,
                (code_regions,),
            )
        else:
            cur.execute(
                """
                SELECT DISTINCT ON (COALESCE(c.code_insee, c.dep_code || '-' || c.nom_standard))
                  c.code_insee, c.dep_code AS code_dept, c.code_postal, c.nom_standard AS commune,
                  GREATEST(COALESCE(c.population, 0)::numeric, 1) AS population,
                  d.nom_dept AS dep_nom, r.nom_region AS reg_nom, d.code_region
                FROM foncier.ref_communes c
                LEFT JOIN foncier.ref_departements d ON d.code_dept = c.dep_code
                LEFT JOIN foncier.ref_regions r ON r.code_region = d.code_region
                WHERE c.dep_code = ANY(%s)
                ORDER BY COALESCE(c.code_insee, c.dep_code || '-' || c.nom_standard), c.code_postal
                """,
                (code_depts,),
            )
        rows = cur.fetchall()
        return [dict(r) for r in rows]
    except psycopg2.Error:
        return []


def _aggregate_indicators_weighted(commune_rows: List[dict], weight_key: str = "population") -> dict:
    """Agrège les indicateurs numériques en moyenne pondérée par weight_key (ex. population)."""
    def _to_float_or_none(v):
        if v is None:
            return None
        try:
            return float(v)
        except (TypeError, ValueError):
            return None

    def _row_has_any_indicator(row: dict) -> bool:
        keys = [
            "renta_brute", "renta_nette", "renta_brute_maisons", "renta_nette_maisons",
            "renta_brute_appts", "renta_nette_appts",
            "renta_brute_parking", "renta_nette_parking", "renta_brute_local_indus", "renta_nette_local_indus",
            "renta_brute_terrain", "renta_nette_terrain", "renta_brute_immeuble", "renta_nette_immeuble",
            "taux_tfb", "taux_teom",
        ] + list(TRANCHE_RENTA_COLS)
        for k in keys:
            if _to_float_or_none(row.get(k)) is not None:
                return True
        return False

    commune_rows = [r for r in (commune_rows or []) if _row_has_any_indicator(r)]
    if not commune_rows:
        return {}
    total_w = 0
    sums = {}
    numeric_keys = [
        "renta_brute", "renta_nette", "renta_brute_maisons", "renta_nette_maisons",
        "renta_brute_appts", "renta_nette_appts",
        "renta_brute_parking", "renta_nette_parking", "renta_brute_local_indus", "renta_nette_local_indus",
        "renta_brute_terrain", "renta_nette_terrain", "renta_brute_immeuble", "renta_nette_immeuble",
        "taux_tfb", "taux_teom",
    ] + list(TRANCHE_RENTA_COLS)
    for k in numeric_keys:
        sums[k] = 0.0
    first = commune_rows[0]
    out = {
        "region": first.get("region") or "",
        "dep_nom": first.get("dep_nom") or "",
        "code_region": first.get("code_region"),
    }
    for row in commune_rows:
        w = float(row.get(weight_key) or 1)
        if w <= 0:
            w = 1
        total_w += w
        for k in numeric_keys:
            v = row.get(k)
            if v is not None:
                try:
                    sums[k] += float(v) * w
                except (TypeError, ValueError):
                    pass
    if total_w <= 0:
        total_w = 1
    for k in numeric_keys:
        if sums[k] != 0:
            out[k] = round(sums[k] / total_w, 2)
        else:
            out[k] = None
    return out


def _to_float_or_none(v):
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _is_valid_rentability_row(row: dict) -> bool:
    """Ligne affichable: renta_brute + renta_nette déterminées et <= 100%."""
    rb = _to_float_or_none(row.get("renta_brute"))
    rn = _to_float_or_none(row.get("renta_nette"))
    """if rb is None or rn is None:
        return False
    if rb > 100 or rn > 100:
        return False"""
    return True


def _recompute_indicateurs_depts(conn, cur, code_depts: List[str]) -> int:
    """Recalcule les agrégats départements à partir de indicateurs_communes (sans appeler get_comparaison_scores)."""
    if not code_depts:
        return 0
    communes_ref = _get_communes_for_aggregation(cur, code_depts=code_depts, code_regions=None)
    code_insee_list = [str(c.get("code_insee")) for c in communes_ref if c.get("code_insee")]
    indic_by_insee = _read_indicateurs_communes(cur, code_insee_list)
    refreshed = 0
    for code_d in code_depts:
        subset = [c for c in communes_ref if (c.get("code_dept") or "").strip() == code_d]
        if not subset:
            continue
        commune_indicators = []
        for c in subset:
            code_insee = c.get("code_insee")
            if not code_insee or code_insee not in indic_by_insee:
                continue
            pop = int(float(c.get("population") or 1))
            row = dict(indic_by_insee[code_insee])
            row["population"] = pop
            row["dep_nom"] = c.get("dep_nom") or code_d
            row["code_region"] = c.get("code_region")
            if _is_valid_rentability_row(row):
                commune_indicators.append(row)
        agg = _aggregate_indicators_weighted(commune_indicators, "population")
        if not agg or not _is_valid_rentability_row(agg):
            continue
        agg["mode"] = "departement"
        agg["code_dept"] = code_d
        agg["dep_nom"] = agg.get("dep_nom") or code_d
        agg["region"] = agg.get("region") or ""
        agg["population"] = int(sum(int(float(r.get("population") or 1)) for r in commune_indicators)) if commune_indicators else None
        nl_vals = [r.get("nb_locaux") for r in commune_indicators]
        if all(v is None for v in nl_vals):
            agg["nb_locaux"] = None
        else:
            agg["nb_locaux"] = sum(int(v or 0) for v in nl_vals)
        agg_ip = _aggregate_par_periode_from_commune_rows(commune_indicators, "population")
        if agg_ip:
            agg["indicateurs_par_periode"] = agg_ip
            s1 = agg_ip.get("1") or {}
            if s1.get("nb_ventes_dvf") is not None:
                try:
                    agg["nb_ventes_dvf"] = int(s1["nb_ventes_dvf"])
                except (TypeError, ValueError):
                    agg["nb_ventes_dvf"] = None
            else:
                agg["nb_ventes_dvf"] = None
        else:
            agg["indicateurs_par_periode"] = None
            agg["nb_ventes_dvf"] = None
        _upsert_indicateurs_depts(conn, cur, agg, commit=False)
        refreshed += 1
    return refreshed


def _recompute_indicateurs_regions(conn, cur, code_regions: List[str]) -> int:
    """Recalcule les agrégats régions à partir de indicateurs_communes (sans appeler get_comparaison_scores)."""
    if not code_regions:
        return 0
    communes_ref = _get_communes_for_aggregation(cur, code_depts=None, code_regions=code_regions)
    code_insee_list = [str(c.get("code_insee")) for c in communes_ref if c.get("code_insee")]
    indic_by_insee = _read_indicateurs_communes(cur, code_insee_list)
    refreshed = 0
    for code_r in code_regions:
        subset = [c for c in communes_ref if (c.get("code_region") or "").strip() == code_r]
        if not subset:
            continue
        commune_indicators = []
        for c in subset:
            code_insee = c.get("code_insee")
            if not code_insee or code_insee not in indic_by_insee:
                continue
            pop = int(float(c.get("population") or 1))
            row = dict(indic_by_insee[code_insee])
            row["population"] = pop
            row["code_region"] = c.get("code_region")
            row["reg_nom"] = c.get("reg_nom")
            if _is_valid_rentability_row(row):
                commune_indicators.append(row)
        agg = _aggregate_indicators_weighted(commune_indicators, "population")
        if not agg or not _is_valid_rentability_row(agg):
            continue
        agg["mode"] = "region"
        agg["code_region"] = code_r
        agg["region"] = next((c.get("reg_nom") for c in subset if c.get("reg_nom")), code_r)
        agg["population"] = int(sum(int(float(r.get("population") or 1)) for r in commune_indicators)) if commune_indicators else None
        nl_vals = [r.get("nb_locaux") for r in commune_indicators]
        if all(v is None for v in nl_vals):
            agg["nb_locaux"] = None
        else:
            agg["nb_locaux"] = sum(int(v or 0) for v in nl_vals)
        agg_ip = _aggregate_par_periode_from_commune_rows(commune_indicators, "population")
        if agg_ip:
            agg["indicateurs_par_periode"] = agg_ip
            s1 = agg_ip.get("1") or {}
            if s1.get("nb_ventes_dvf") is not None:
                try:
                    agg["nb_ventes_dvf"] = int(s1["nb_ventes_dvf"])
                except (TypeError, ValueError):
                    agg["nb_ventes_dvf"] = None
            else:
                agg["nb_ventes_dvf"] = None
        else:
            agg["indicateurs_par_periode"] = None
            agg["nb_ventes_dvf"] = None
        _upsert_indicateurs_regions(conn, cur, agg, commit=False)
        refreshed += 1
    return refreshed


_COMPARAISON_BASE_SCORE_KEYS = frozenset(
    [
        "renta_brute",
        "renta_nette",
        "renta_brute_maisons",
        "renta_nette_maisons",
        "renta_brute_appts",
        "renta_nette_appts",
        "renta_brute_parking",
        "renta_nette_parking",
        "renta_brute_local_indus",
        "renta_nette_local_indus",
        "renta_brute_terrain",
        "renta_nette_terrain",
        "renta_brute_immeuble",
        "renta_nette_immeuble",
        "taux_tfb",
        "taux_teom",
    ]
)
COMPARAISON_SCORE_KEYS = _COMPARAISON_BASE_SCORE_KEYS | frozenset(TRANCHE_RENTA_COLS)


def _normalize_comparaison_score_key(score_principal: Optional[str]) -> str:
    sp = (score_principal or "renta_nette").strip()
    if sp in COMPARAISON_SCORE_KEYS:
        return sp
    # Sentinelle UI : pas de repli sur renta_nette agrégée (tri neutre, colonne absente).
    if sp == "__indicateur_non_calcule__":
        return sp
    return "renta_nette"


def _nb_locaux_col_for_score_key(score_key: str) -> str:
    """Retourne la colonne nb_locaux appropriée pour un score_principal donné."""
    import re
    if not score_key or score_key == "__indicateur_non_calcule__":
        return "nb_locaux"
    m = re.search(r"_(maisons|appts|agg)_(s[1-5]|t[1-5])$", score_key)
    if m:
        return f"nb_locaux_{m.group(1)}_{m.group(2)}"
    if "_maisons" in score_key:
        return "nb_locaux_maisons"
    if "_appts" in score_key:
        return "nb_locaux_appts"
    return "nb_locaux"


def _row_matches_nb_locaux_min(row: dict, nb_min: int, score_key: str = "renta_nette") -> bool:
    """True si la ligne a un nb_locaux numérique >= nb_min pour le score donné (sinon exclue)."""
    col = _nb_locaux_col_for_score_key(score_key)
    v = row.get(col)
    if v is None:
        v = row.get("nb_locaux")  # fallback global si la colonne spécifique est absente
    if v is None:
        return False
    try:
        return int(v) >= nb_min
    except (TypeError, ValueError):
        return False


def _row_matches_renta_mins(
    row: dict,
    renta_brute_min: Optional[float],
    renta_nette_min: Optional[float],
) -> bool:
    """Filtre optionnel sur renta_brute / renta_nette (après merge période). Valeur absente => ligne exclue si seuil défini."""
    if renta_brute_min is not None:
        v = row.get("renta_brute")
        if v is None:
            return False
        try:
            if float(v) < float(renta_brute_min):
                return False
        except (TypeError, ValueError):
            return False
    if renta_nette_min is not None:
        v = row.get("renta_nette")
        if v is None:
            return False
        try:
            if float(v) < float(renta_nette_min):
                return False
        except (TypeError, ValueError):
            return False
    return True


@app.get("/api/comparaison_scores")
def get_comparaison_scores(
    mode: str = Query("communes", description="Mode: communes, departements, regions"),
    scope: Optional[str] = Query(None, description="Scope géographique: communes (défaut), department, region, all_france"),
    code_dept: Optional[List[str]] = Query(None, description="Code département (répété pour chaque commune ou liste de depts)"),
    code_postal: Optional[List[str]] = Query(None, description="Code postal (répété pour chaque commune)"),
    commune: Optional[List[str]] = Query(None, description="Nom commune (répété pour chaque commune)"),
    code_region: Optional[List[str]] = Query(None, description="Code région (pour mode=regions, répété)"),
    code_insee: Optional[List[str]] = Query(None, description="Code INSEE (répété) — bypass la résolution triplets"),
    exclude_code_insee: Optional[List[str]] = Query(None, description="Codes INSEE à exclure"),
    exclude_code_dept: Optional[List[str]] = Query(None, description="Codes département à exclure"),
    score_principal: str = Query(
        "renta_nette",
        description="Clé de tri / colonne indicateur (renta_brute, renta_nette, colonnes tranches s1–s5 / t1–t5, etc.)",
    ),
    n_max: int = Query(100, ge=1, le=50000, description="Nombre max de lignes à retourner (optionnel)"),
    nb_locaux_min: Optional[int] = Query(
        None,
        ge=0,
        description="Si défini, exclut les lignes dont nb_locaux est absent ou strictement inférieur au seuil.",
    ),
    renta_brute_min: Optional[float] = Query(
        None,
        description="Si défini, exclut les lignes sans renta_brute ou avec renta_brute strictement inférieure.",
    ),
    renta_nette_min: Optional[float] = Query(
        None,
        description="Si défini, exclut les lignes sans renta_nette ou avec renta_nette strictement inférieure.",
    ),
    periode_annees: int = Query(
        1,
        description="Fenêtre DVF en années (1, 2, 3 ou 5) pour rentabilités / nb ventes / nb locaux affichés.",
    ),
    scores_secondaires: Optional[List[str]] = Query(None, description="Scores secondaires (taux_tfb, taux_teom, etc.)"),
    type_logt: Optional[str] = Query(None, description="Filtre ref_type_logts (codes) — réservé à une évolution future"),
    type_surf: Optional[str] = Query(None, description="Filtre ref_type_surf — réservé à une évolution future"),
    nb_pieces: Optional[str] = Query(None, description="Filtre ref_nb_pieces — réservé à une évolution future"),
):
    """
    Retourne le classement selon le score principal.
    - mode=communes : liste (code_dept, code_postal, commune) par paramètres répétés.
    - mode=departements : liste code_dept ; agrégation pondérée par population des communes du département.
    - mode=regions : liste code_region ; agrégation pondérée par population des communes de la région.

    Les lignes sont lues dans `indicateurs_*` (colonnes précalculées). Le tri utilise `score_principal`
    (ex. `renta_nette_maisons_s3` pour une tranche surface). Les filtres type_logt / type_surf / nb_pieces
    sont informatifs côté UI ; le score exact se choisit via `score_principal`.
    """
    if periode_annees not in (1, 2, 3, 5):
        raise HTTPException(status_code=400, detail="periode_annees doit être 1, 2, 3 ou 5.")
    score_key = _normalize_comparaison_score_key(score_principal)
    mode = (mode or "communes").strip().lower()
    if mode not in ("communes", "departements", "regions"):
        mode = "communes"

    if mode == "departements":
        code_depts = [str(x or "").strip() for x in (code_dept or []) if str(x or "").strip()]
        if not code_depts:
            raise HTTPException(status_code=400, detail="En mode départements, au moins un code département est requis.")
        conn = None
        try:
            conn = get_db_connection()
            cur = conn.cursor(cursor_factory=RealDictCursor)
            dept_rows_by_code = _read_indicateurs_depts(cur, code_depts)
            rows = []
            for code_d in code_depts:
                row = dept_rows_by_code.get(code_d)
                if row and _is_valid_rentability_row(row):
                    rows.append(row)
            cur.close()
            rows = [_merge_periode_into_row(dict(r), periode_annees) for r in rows]
            if nb_locaux_min is not None:
                rows = [r for r in rows if _row_matches_nb_locaux_min(r, nb_locaux_min, score_key)]
            rows.sort(key=lambda r: (r.get(score_key) is None, -(r.get(score_key) or 0)))
            if n_max and len(rows) > n_max:
                rows = rows[:n_max]
            return {"rows": rows}
        finally:
            if conn:
                conn.close()

    if mode == "regions":
        code_regions = [str(x or "").strip() for x in (code_region or []) if str(x or "").strip()]
        if not code_regions:
            raise HTTPException(status_code=400, detail="En mode régions, au moins un code région est requis.")
        conn = None
        try:
            conn = get_db_connection()
            cur = conn.cursor(cursor_factory=RealDictCursor)
            region_rows_by_code = _read_indicateurs_regions(cur, code_regions)
            rows = []
            for code_r in code_regions:
                row = region_rows_by_code.get(code_r)
                if row and _is_valid_rentability_row(row):
                    rows.append(row)
            cur.close()
            rows = [_merge_periode_into_row(dict(r), periode_annees) for r in rows]
            if nb_locaux_min is not None:
                rows = [r for r in rows if _row_matches_nb_locaux_min(r, nb_locaux_min, score_key)]
            rows = [r for r in rows if _row_matches_renta_mins(r, renta_brute_min, renta_nette_min)]
            rows.sort(key=lambda r: (r.get(score_key) is None, -(r.get(score_key) or 0)))
            if n_max and len(rows) > n_max:
                rows = rows[:n_max]
            return {"rows": rows}
        finally:
            if conn:
                conn.close()

    # mode=communes : lecture depuis indicateurs_communes
    t_start = _time.monotonic()

    # Déterminer le scope effectif
    effective_scope = (scope or "").strip().lower() if scope else None
    # Nettoyage des listes d'exclusion
    excl_insee = [str(x).strip() for x in (exclude_code_insee or []) if str(x or "").strip()]
    excl_dept = [str(x).strip() for x in (exclude_code_dept or []) if str(x or "").strip()]
    # Nettoyage code_insee direct
    direct_insee = [str(x).strip() for x in (code_insee or []) if str(x or "").strip()]

    # S1 : Chemin rapide par scope (bypass _resolve_communes_to_ref)
    if effective_scope in ("all_france", "department", "region") or direct_insee:
        score_cols, need_periode = _compute_needed_columns(score_key, scores_secondaires, periode_annees)
        _debug_log("[comparaison_scores] chemin scope=%s, %d score_cols, need_periode=%s",
                   effective_scope or "communes(code_insee)", len(score_cols), need_periode)

        conn = None
        rows = []
        try:
            conn = get_db_connection()
            cur = conn.cursor(cursor_factory=RealDictCursor)
            t_conn = _time.monotonic()
            _debug_log("[comparaison_scores] connexion DB: %.3fs", t_conn - t_start)

            if direct_insee:
                # code_insee fournis directement → scope=communes par code_insee
                indic_by_insee = _read_indicateurs_by_scope(
                    cur, scope="communes", code_insee_list=direct_insee,
                    exclude_code_insee=excl_insee or None, exclude_code_dept=excl_dept or None,
                    score_cols=score_cols, need_periode=need_periode,
                )
            else:
                # Préparer les listes selon le scope
                scope_dept_list = [str(x or "").strip() for x in (code_dept or []) if str(x or "").strip()] if effective_scope == "department" else None
                scope_region_list = [str(x or "").strip() for x in (code_region or []) if str(x or "").strip()] if effective_scope == "region" else None
                indic_by_insee = _read_indicateurs_by_scope(
                    cur, scope=effective_scope,
                    code_dept_list=scope_dept_list, code_region_list=scope_region_list,
                    exclude_code_insee=excl_insee or None, exclude_code_dept=excl_dept or None,
                    score_cols=score_cols, need_periode=need_periode,
                )

            t_read = _time.monotonic()
            _debug_log("[comparaison_scores] lecture indicateurs: %.3fs, %d communes", t_read - t_conn, len(indic_by_insee))

            for ci, cached_row in indic_by_insee.items():
                if _is_valid_rentability_row(cached_row):
                    rows.append(cached_row)
            cur.close()
        finally:
            if conn is not None:
                conn.close()

        t_filter_start = _time.monotonic()
        _debug_log("[comparaison_scores] nb communes valides: %s dans rows", len(rows))
        rows = [_merge_periode_into_row(dict(r), periode_annees) for r in rows]
        t_merge = _time.monotonic()
        _debug_log("[comparaison_scores] merge_periode: %.3fs", t_merge - t_filter_start)

        if nb_locaux_min is not None:
            rows = [r for r in rows if _row_matches_nb_locaux_min(r, nb_locaux_min, score_key)]
        rows = [r for r in rows if _row_matches_renta_mins(r, renta_brute_min, renta_nette_min)]
        rows.sort(key=lambda r: (r.get(score_key) is None, -(r.get(score_key) or 0)))
        if n_max and len(rows) > n_max:
            rows = rows[:n_max]
        t_end = _time.monotonic()
        _debug_log("[comparaison_scores] tri+filtrage: %.3fs, sortie: %s ligne(s) retournée(s), total: %.3fs",
                   t_end - t_merge, len(rows), t_end - t_start)
        return {"rows": rows}

    # Chemin legacy : résolution par triplets (code_dept, code_postal, commune)
    depts = [str(x or "").strip() for x in (code_dept or [])]
    postals = [str(x or "").strip() for x in (code_postal or [])]
    noms = [str(x or "").strip() for x in (commune or [])]
    n = max(len(depts), len(postals), len(noms))
    while len(depts) < n:
        depts.append("")
    while len(postals) < n:
        postals.append("")
    while len(noms) < n:
        noms.append("")
    communes = [(depts[i], postals[i], noms[i]) for i in range(n) if depts[i] and postals[i] and noms[i]]
    if not communes:
        raise HTTPException(status_code=400, detail="Au moins une commune est requise (code_dept, code_postal, commune pour chaque, ou utiliser scope/code_insee).")

    conn = None
    rows = []
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        t_conn = _time.monotonic()
        _debug_log("[comparaison_scores] chemin legacy, %d communes, connexion DB: %.3fs", len(communes), t_conn - t_start)
        ref_list = _resolve_communes_to_ref(cur, communes)
        t_resolve = _time.monotonic()
        _debug_log("[comparaison_scores] _resolve_communes_to_ref: %.3fs, %s dans communes, %s dans ref_list",
                   t_resolve - t_conn, len(communes), len(ref_list))
        ref_by_key = {}
        for r in ref_list:
            k = (r.get("code_dept"), r.get("code_postal"), _normalize_name_canonical(r.get("commune") or ""))
            ref_by_key[k] = r
        code_insee_list = [r["code_insee"] for r in ref_list if r.get("code_insee")]
        indic_by_insee = _read_indicateurs_communes(cur, code_insee_list)
        t_read = _time.monotonic()
        _debug_log("[comparaison_scores] _read_indicateurs_communes: %.3fs, %d indicateurs", t_read - t_resolve, len(indic_by_insee))
        for c_dept, c_postal, c_commune in communes:
            key = (c_dept, _normalize_code_postal_for_ref_communes(c_postal), _normalize_name_canonical(c_commune))
            ref = ref_by_key.get(key)
            code_insee_val = ref.get("code_insee") if ref else None
            if code_insee_val and code_insee_val in indic_by_insee:
                cached_row = indic_by_insee[code_insee_val]
                if _is_valid_rentability_row(cached_row):
                    rows.append(cached_row)
        cur.close()
    finally:
        if conn is not None:
            conn.close()

    t_filter_start = _time.monotonic()
    _debug_log("[comparaison_scores] nb communes : %s dans rows", len(rows))
    rows = [_merge_periode_into_row(dict(r), periode_annees) for r in rows]

    if nb_locaux_min is not None:
        rows = [r for r in rows if _row_matches_nb_locaux_min(r, nb_locaux_min, score_key)]
    rows = [r for r in rows if _row_matches_renta_mins(r, renta_brute_min, renta_nette_min)]
    rows.sort(key=lambda r: (r.get(score_key) is None, -(r.get(score_key) or 0)))
    if n_max and len(rows) > n_max:
        rows = rows[:n_max]
    t_end = _time.monotonic()
    _debug_log("[comparaison_scores] sortie: %s ligne(s) retournée(s), total: %.3fs", len(rows), t_end - t_start)
    return {"rows": rows}

# nouvelle version (Claude.ia) : ajout de la validation du body JSON
class ComparaisonScoresBody(BaseModel):
    mode: str = "communes"
    scope: Optional[str] = None  # communes, department, region, all_france
    code_dept: Optional[List[str]] = None
    code_postal: Optional[List[str]] = None
    commune: Optional[List[str]] = None
    code_region: Optional[List[str]] = None
    code_insee: Optional[List[str]] = None  # bypass résolution triplets
    exclude_code_insee: Optional[List[str]] = None
    exclude_code_dept: Optional[List[str]] = None
    score_principal: str = "renta_nette"
    n_max: int = 50000 # maximum de 50000 communes
    nb_locaux_min: Optional[int] = None
    renta_brute_min: Optional[float] = None
    renta_nette_min: Optional[float] = None
    periode_annees: int = 1
    scores_secondaires: Optional[List[str]] = None
    type_logt: Optional[str] = None
    type_surf: Optional[str] = None
    nb_pieces: Optional[str] = None


@app.post("/api/comparaison_scores")
def post_comparaison_scores(body: ComparaisonScoresBody):
    """Version POST de /api/comparaison_scores — identique mais accepte un body JSON.
    Nécessaire pour les sélections dépassant ~800 communes (limite URL GET).
    """
    return get_comparaison_scores(
        mode=body.mode,
        scope=body.scope,
        code_dept=body.code_dept,
        code_postal=body.code_postal,
        commune=body.commune,
        code_region=body.code_region,
        code_insee=body.code_insee,
        exclude_code_insee=body.exclude_code_insee,
        exclude_code_dept=body.exclude_code_dept,
        score_principal=body.score_principal,
        n_max=max(1, min(body.n_max, 50000)),
        nb_locaux_min=body.nb_locaux_min,
        renta_brute_min=body.renta_brute_min,
        renta_nette_min=body.renta_nette_min,
        periode_annees=body.periode_annees,
        scores_secondaires=body.scores_secondaires,
        type_logt=body.type_logt,
        type_surf=body.type_surf,
        nb_pieces=body.nb_pieces,
    )



# Noms de paramètres connus pour refresh-indicateurs (pour détecter les typos)
_REFRESH_INDICATEURS_KNOWN_PARAMS = {"code_insee_list", "limit", "batch_commit", "workers"}
_REFRESH_INDICATEURS_USAGE = (
    "Usage: POST /api/refresh-indicateurs?code_insee_list=<code1>&code_insee_list=<code2>&... "
    "(paramètre sans 'e' final). Exemple: POST /api/refresh-indicateurs?code_insee_list=97306"
)

def _refresh_indicateurs_impl(
    code_insee_list: Optional[List[str]],
    limit: Optional[int],
    _batch_commit: int,
    workers: int,
) -> dict:
    """
    Corps métier de POST /api/refresh-indicateurs (sans validation HTTP des query params).
    Appelable depuis d'autres endpoints (ex. force-recalcul) sans objet Request.
    _batch_commit : conservé pour compatibilité API (non utilisé : un COMMIT par upsert commune).
    """
    def _write_rejected_codes_csv(entries: List[dict]) -> Optional[str]:
        if not entries:
            return None
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        fname = f"liste_communes_rejetées_refresh_indicateurs_{ts}.csv"
        target = Path(__file__).resolve().parents[1] / fname
        with open(target, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f, delimiter=";")
            w.writerow(["code_insee", "motif", "erreur"])
            for e in entries:
                w.writerow([e.get("code_insee", ""), e.get("motif", ""), e.get("erreur", "")])
        return str(target)

    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        if code_insee_list:
            to_process = [str(x).strip() for x in code_insee_list if str(x).strip()]
        else:
            if limit and limit > 0:
                cur.execute(
                    "SELECT DISTINCT code_insee FROM foncier.ref_communes WHERE code_insee IS NOT NULL AND code_insee != '' ORDER BY code_insee LIMIT %s",
                    (limit,),
                )
            else:
                cur.execute(
                    "SELECT DISTINCT code_insee FROM foncier.ref_communes WHERE code_insee IS NOT NULL AND code_insee != '' ORDER BY code_insee",
                )
            to_process = [str(r["code_insee"]).strip() for r in cur.fetchall()]
        if not to_process:
            cur.close()
            return {
                "refreshed": 0,
                "requested": 0,
                "from_cache": 0,
                "codes_not_in_ref_communes": [],
                "codes_missing_dept_or_commune": [],
                "codes_fiche_indisponible": [],
                "codes_upsert_failed": [],
                "codes_rejected": 0,
                "rejected_csv_path": None,
            }

        # Une seule requête pour toutes les ref (une ligne par code_insee)
        placeholders = ",".join(["%s"] * len(to_process))
        cur.execute(
            "SELECT DISTINCT ON (c.code_insee) c.code_insee, c.dep_code AS code_dept, c.code_postal, c.nom_standard AS commune, "
            "d.nom_dept AS dep_nom, r.nom_region AS reg_nom, "
            "GREATEST(COALESCE(c.population, 0)::int, 1) AS population "
            "FROM foncier.ref_communes c "
            "LEFT JOIN foncier.ref_departements d ON d.code_dept = c.dep_code "
            "LEFT JOIN foncier.ref_regions r ON r.code_region = d.code_region "
            "WHERE c.code_insee IN (" + placeholders + ") ORDER BY c.code_insee, c.code_postal",
            to_process,
        )
        ref_by_insee = {str(r["code_insee"]): dict(r) for r in cur.fetchall()}
        codes_not_in_ref_communes = [ci for ci in to_process if ci not in ref_by_insee]

        # Une seule requête pour les payloads déjà en cache fiche
        cur.execute(
            "SELECT code_insee, payload FROM foncier.fiche_logement_cache WHERE code_insee IN (" + placeholders + ")",
            to_process,
        )
        cache_by_insee = {}
        for r in cur.fetchall():
            ci = r.get("code_insee")
            if ci:
                pl = r.get("payload")
                cache_by_insee[ci] = pl if isinstance(pl, dict) else (json.loads(pl) if isinstance(pl, str) else pl)

        was_in_cache = set(cache_by_insee.keys())
        # Fiches à calculer (hors cache) : calcul en parallèle pour accélérer
        def _fetch_fiche(code_insee: str, ref: dict) -> tuple:
            try:
                fiche = get_fiche_logement(
                    code_dept=ref["code_dept"],
                    code_postal=ref.get("code_postal") or "",
                    commune=ref["commune"],
                )
                return (code_insee, fiche)
            except Exception:
                return (code_insee, None)

        to_fetch = [
            (ci, ref_by_insee[ci])
            for ci in to_process
            if ci in ref_by_insee and ci not in cache_by_insee
            and ref_by_insee[ci].get("code_dept") and ref_by_insee[ci].get("commune")
        ]
        if to_fetch:
            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = {executor.submit(_fetch_fiche, ci, ref): ci for ci, ref in to_fetch}
                for fut in as_completed(futures):
                    code_insee, fiche = fut.result()
                    if fiche is not None:
                        cache_by_insee[code_insee] = fiche

        done = 0
        from_cache = 0
        codes_missing_dept_or_commune: List[str] = []
        codes_fiche_indisponible: List[str] = []
        codes_upsert_failed: List[str] = []
        rejected_entries: List[dict] = []
        for code_insee in to_process:
            ref = ref_by_insee.get(code_insee)
            if not ref:
                continue
            code_dept = ref.get("code_dept")
            code_postal = ref.get("code_postal")
            commune = ref.get("commune")
            if not code_dept or not commune:
                codes_missing_dept_or_commune.append(code_insee)
                rejected_entries.append({"code_insee": code_insee, "motif": "missing_dept_or_commune", "erreur": ""})
                continue
            fiche = cache_by_insee.get(code_insee)
            if not fiche:
                codes_fiche_indisponible.append(code_insee)
                rejected_entries.append({"code_insee": code_insee, "motif": "fiche_indisponible", "erreur": ""})
                continue
            if code_insee in was_in_cache:
                from_cache += 1
            pop = ref.get("population")
            if isinstance(pop, Decimal):
                pop = int(pop)

            row = _indicators_from_fiche_payload(
                fiche, code_insee, code_dept, code_postal or "", commune,
                reg_nom=ref.get("reg_nom"), dep_nom=ref.get("dep_nom"), population=pop,
            )
            # Ne loguer que les communes pour lesquelles on a recalculé la fiche (pas déjà en cache), pour éviter de polluer les logs.
            if code_insee not in was_in_cache:
                _debug_log("[refresh-indicateurs] code_insee=%s upsert: renta_brute=%s renta_nette=%s",
                    code_insee, row.get("renta_brute"), row.get("renta_nette"))
            # Un commit par commune : après une erreur SQL, psycopg2 laisse souvent la connexion
            # « aborted » malgré ROLLBACK TO SAVEPOINT ; un commit par upsert isole chaque ligne.
            # batch_commit est conservé en paramètre API pour compatibilité mais n’est plus utilisé ici.
            ok, err = _upsert_indicateurs_communes(conn, cur, row, commit=True)
            if ok:
                done += 1
            else:
                codes_upsert_failed.append(code_insee)
                rejected_entries.append({"code_insee": code_insee, "motif": "upsert_failed", "erreur": err or ""})
                _debug_log("[refresh-indicateurs] code_insee=%s upsert FAILED: %s", code_insee, err)
        rejected_csv_path = _write_rejected_codes_csv(rejected_entries)
        cur.close()
        return {
            "refreshed": done,
            "requested": len(to_process),
            "from_cache": from_cache,
            "codes_not_in_ref_communes": codes_not_in_ref_communes,
            "codes_missing_dept_or_commune": codes_missing_dept_or_commune,
            "codes_fiche_indisponible": codes_fiche_indisponible,
            "codes_upsert_failed": codes_upsert_failed,
            "codes_rejected": len(rejected_entries),
            "rejected_csv_path": rejected_csv_path,
        }
    except psycopg2.Error as e:
        if conn:
            conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            conn.close()


@app.post("/api/refresh-indicateurs")
def refresh_indicateurs(
    request: Request,
    code_insee_list: Optional[List[str]] = Query(
        None,
        description="Liste de code_insee à traiter. Syntaxe : répéter le paramètre (ex. ?code_insee_list=75056&code_insee_list=13001) ou en JSON body si besoin.",
    ),
    limit: Optional[int] = Query(None, ge=0, le=50000, description="Max communes à traiter ; absent ou 0 = sans limite (toutes)"),
    batch_commit: int = Query(
        50,
        ge=1,
        le=500,
        description="(Obsolète) Conservé pour compatibilité. Chaque commune est commitée séparément pour isoler les erreurs.",
    ),
    workers: int = Query(4, ge=1, le=16, description="Nombre de workers parallèles pour calculer les fiches (cache fiche + indicateurs)"),
):
    """
    Remplit ou met à jour fiche_logement_cache et indicateurs_communes pour les communes demandées.
    Si code_insee_list est fourni, traite uniquement ces code_insee. Sinon traite les communes de ref_communes
    (sans limite si limit absent ou 0, sinon limit communes).
    Lit d'abord le cache fiche quand il existe ; les fiches manquantes sont calculées en parallèle (workers).
    """
    query_keys = set(request.query_params.keys())
    typo = query_keys & {"code_insee_liste", "code_insee_listes"}
    unknown = query_keys - _REFRESH_INDICATEURS_KNOWN_PARAMS
    unknown_code_insee = [k for k in unknown if k.startswith("code_insee")]
    if typo or unknown_code_insee:
        bad = list(typo or unknown_code_insee)[0]
        raise HTTPException(
            status_code=400,
            detail=f"Paramètre inconnu: '{bad}'. Utilisez 'code_insee_list' (sans 'e' final). {_REFRESH_INDICATEURS_USAGE}",
        )
    _invalidate_indic_cols_cache()
    return _refresh_indicateurs_impl(code_insee_list, limit, batch_commit, workers)


@app.post("/api/refresh-indicateurs-agreges")
def refresh_indicateurs_agreges(
    code_dept_list: Optional[List[str]] = Query(None, description="Codes département à rafraîchir (optionnel)"),
    code_region_list: Optional[List[str]] = Query(None, description="Codes région à rafraîchir (optionnel)"),
    refresh_all: bool = Query(False, description="Si true, rafraîchit tous les départements et toutes les régions"),
    force: bool = Query(False, description="Si true, supprime les lignes existantes ciblées avant recalcul"),
):
    """
    Rafraîchit les agrégats `indicateurs_depts` et `indicateurs_regions`.
    - Par défaut: upsert des lignes manquantes/obsolètes selon la logique de comparaison_scores.
    - force=true: supprime d'abord les lignes ciblées puis recalcule entièrement ces cibles.
    """
    conn = None
    try:
        # Déterminer les cibles
        target_depts = [str(x or "").strip() for x in (code_dept_list or []) if str(x or "").strip()]
        target_regions = [str(x or "").strip() for x in (code_region_list or []) if str(x or "").strip()]
        if refresh_all:
            conn = get_db_connection()
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute("SELECT code_dept FROM foncier.ref_departements ORDER BY code_dept")
            target_depts = [str(r.get("code_dept") or "").strip() for r in cur.fetchall() if str(r.get("code_dept") or "").strip()]
            cur.execute("SELECT code_region FROM foncier.ref_regions ORDER BY code_region")
            target_regions = [str(r.get("code_region") or "").strip() for r in cur.fetchall() if str(r.get("code_region") or "").strip()]
            cur.close()
            conn.close()
            conn = None

        if not target_depts and not target_regions:
            raise HTTPException(
                status_code=400,
                detail="Fournir code_dept_list et/ou code_region_list, ou utiliser refresh_all=true.",
            )

        # Option force : supprimer les cibles avant recalcul
        if force:
            conn = get_db_connection()
            cur = conn.cursor(cursor_factory=RealDictCursor)
            if target_depts:
                cur.execute(
                    "DELETE FROM foncier.indicateurs_depts WHERE code_dept = ANY(%s)",
                    (target_depts,),
                )
            if target_regions:
                cur.execute(
                    "DELETE FROM foncier.indicateurs_regions WHERE code_region = ANY(%s)",
                    (target_regions,),
                )
            conn.commit()
            cur.close()
            conn.close()
            conn = None

        refreshed_depts = 0
        refreshed_regions = 0

        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        if target_depts:
            refreshed_depts = _recompute_indicateurs_depts(conn, cur, target_depts)
        if target_regions:
            refreshed_regions = _recompute_indicateurs_regions(conn, cur, target_regions)
        conn.commit()
        cur.close()
        conn.close()
        conn = None

        return {
            "departements_requested": len(target_depts),
            "regions_requested": len(target_regions),
            "departements_refreshed": refreshed_depts,
            "regions_refreshed": refreshed_regions,
            "force": force,
        }
    except psycopg2.Error as e:
        if conn:
            conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            conn.close()


@app.post("/api/force-recalcul-indicateurs")
def force_recalcul_indicateurs(
    mode: str = Query("communes", description="communes | departements | regions"),
    code_dept: Optional[List[str]] = Query(None, description="Codes département (ou communes)"),
    code_postal: Optional[List[str]] = Query(None, description="Codes postaux (mode=communes)"),
    commune: Optional[List[str]] = Query(None, description="Communes (mode=communes)"),
    code_region: Optional[List[str]] = Query(None, description="Codes région (mode=regions)"),
    workers: int = Query(4, ge=1, le=16, description="Workers pour mode=communes"),
    batch_commit: int = Query(
        50,
        ge=1,
        le=500,
        description="(Obsolète) Conservé pour compatibilité ; refresh communes commit chaque upsert.",
    ),
):
    """
    Force le recalcul selon le mode courant de comparaison :
    - communes: purge cache fiche + indicateurs_communes des communes ciblées puis refresh ciblé
    - departements / regions: force=true sur refresh_indicateurs_agreges des cibles
    """
    mode = (mode or "communes").strip().lower()
    if mode == "departements":
        targets = [str(x or "").strip() for x in (code_dept or []) if str(x or "").strip()]
        if not targets:
            raise HTTPException(status_code=400, detail="En mode départements, fournir au moins un code_dept.")
        return refresh_indicateurs_agreges(code_dept_list=targets, code_region_list=None, refresh_all=False, force=True)
    if mode == "regions":
        targets = [str(x or "").strip() for x in (code_region or []) if str(x or "").strip()]
        if not targets:
            raise HTTPException(status_code=400, detail="En mode régions, fournir au moins un code_region.")
        return refresh_indicateurs_agreges(code_dept_list=None, code_region_list=targets, refresh_all=False, force=True)

    # Invalider le cache de colonnes (les migrations ont pu ajouter des colonnes depuis le dernier appel)
    _invalidate_indic_cols_cache()
    # mode=communes
    depts = [str(x or "").strip() for x in (code_dept or [])]
    postals = [str(x or "").strip() for x in (code_postal or [])]
    noms = [str(x or "").strip() for x in (commune or [])]
    n = max(len(depts), len(postals), len(noms))
    while len(depts) < n:
        depts.append("")
    while len(postals) < n:
        postals.append("")
    while len(noms) < n:
        noms.append("")
    communes_in = [(depts[i], postals[i], noms[i]) for i in range(n) if depts[i] and postals[i] and noms[i]]
    if not communes_in:
        raise HTTPException(status_code=400, detail="En mode communes, fournir au moins un triplet code_dept/code_postal/commune.")

    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        ref_list = _resolve_communes_to_ref(cur, communes_in)
        code_insee_list = [str(r.get("code_insee")) for r in ref_list if r.get("code_insee")]
        if not code_insee_list:
            cur.close()
            return {"requested": len(communes_in), "resolved": 0, "refreshed": 0}
        cur.execute("DELETE FROM foncier.fiche_logement_cache WHERE code_insee = ANY(%s)", (code_insee_list,))
        cur.execute("DELETE FROM foncier.indicateurs_communes WHERE code_insee = ANY(%s)", (code_insee_list,))
        conn.commit()
        cur.close()
    finally:
        if conn:
            conn.close()

    out = _refresh_indicateurs_impl(
        code_insee_list=code_insee_list,
        limit=None,
        _batch_commit=batch_commit,
        workers=workers,
    )
    out["requested"] = len(communes_in)
    out["resolved"] = len(code_insee_list)
    return out


# --- Distances adresse → communes (OSRM + centre IGN/API géo) ---------------------------------
def _normalize_insee_fr_api(code: Optional[str]) -> str:
    if code is None:
        return ""
    s = str(code).strip().replace(" ", "")
    if s.isdigit() and len(s) <= 5:
        return s.zfill(5)
    return s


_GOUV_COMMUNE_CENTRE_CACHE: Dict[str, Optional[Dict[str, Any]]] = {}


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Distance orthodromique (km), secours si OSRM indisponible."""
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlamb = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlamb / 2) ** 2
    c = 2 * math.asin(min(1.0, math.sqrt(a)))
    return 6371.0 * c


def _fetch_commune_centre_geo_gouv(code_insee: str) -> Optional[Dict[str, Any]]:
    """Centre administratif (lon, lat) et métadonnées via api.gouv.fr."""
    ci = _normalize_insee_fr_api(code_insee)
    if not ci:
        return None
    if ci in _GOUV_COMMUNE_CENTRE_CACHE:
        return _GOUV_COMMUNE_CENTRE_CACHE[ci]
    url = "https://geo.api.gouv.fr/communes/" + urllib.parse.quote(ci) + "?fields=nom,centre,code,codesPostaux"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "webapp-foncier-distances/1.0"})
        with urllib.request.urlopen(req, timeout=12) as resp:
            data = json.loads(resp.read().decode())
        centre = data.get("centre") or {}
        coords = centre.get("coordinates")
        if not coords or len(coords) < 2:
            _GOUV_COMMUNE_CENTRE_CACHE[ci] = None
            return None
        out = {
            "lon": float(coords[0]),
            "lat": float(coords[1]),
            "nom": data.get("nom"),
            "code_insee": data.get("code") or ci,
            "codes_postaux": data.get("codesPostaux") or [],
        }
        _GOUV_COMMUNE_CENTRE_CACHE[ci] = out
        return out
    except Exception:
        _GOUV_COMMUNE_CENTRE_CACHE[ci] = None
        return None


# L'API publique OSRM limite fortement le débit : trop d'appels concurrents → 429 / timeouts →
# échec silencieux et retombée sur le haversine (distance sans durée). D'où sémaphore + backoff.
_OSRM_PUBLIC_SEM = threading.BoundedSemaphore(
    max(1, min(8, int(os.environ.get("OSRM_MAX_CONCURRENT", "3"))))
)


def _osrm_route_km_minutes(lon1: float, lat1: float, lon2: float, lat2: float) -> Tuple[Optional[float], Optional[float]]:
    base = (os.environ.get("OSRM_BASE_URL") or "https://router.project-osrm.org").rstrip("/")
    path = f"/route/v1/driving/{lon1},{lat1};{lon2},{lat2}?overview=false"
    url = base + path
    max_attempts = max(1, min(8, int(os.environ.get("OSRM_ROUTE_MAX_ATTEMPTS", "4"))))
    backoff_base = float(os.environ.get("OSRM_ROUTE_BACKOFF_SEC", "0.35"))
    pacing = float(os.environ.get("OSRM_ROUTE_PACING_SEC", "0.12"))
    with _OSRM_PUBLIC_SEM:
        _time.sleep(pacing)
        for attempt in range(max_attempts):
            try:
                req = urllib.request.Request(url, headers={"User-Agent": "webapp-foncier-distances/1.0"})
                with urllib.request.urlopen(req, timeout=28) as resp:
                    data = json.loads(resp.read().decode())
                routes = data.get("routes") or []
                if not routes:
                    return None, None
                r0 = routes[0]
                dist_m = r0.get("distance")
                dur_s = r0.get("duration")
                if dist_m is None:
                    return None, None
                km = float(dist_m) / 1000.0
                minutes = float(dur_s) / 60.0 if dur_s is not None else None
                return km, minutes
            except Exception:
                if attempt < max_attempts - 1:
                    _time.sleep(backoff_base * (2**attempt))
                    continue
                return None, None


def _distances_adresse_hash(lat: float, lon: float, adresse_label: Optional[str]) -> str:
    """
    Hash stable pour adresse_hash (nouvelle origine sans ligne en base).
    Aligné sur une origine BAN : 7 décimales + libellé.
    Les lignes déjà en base sont retrouvées par égalité sur (lat, lon) arrondis.
    """
    la = f"{float(lat):.7f}"
    lo = f"{float(lon):.7f}"
    lab = (adresse_label or "").strip()
    raw = f"{la}|{lo}|{lab}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _distances_resolve_adresse_hash(
    conn, alat: float, alon: float, adresse_label: Optional[str]
) -> str:
    """Réutilise un adresse_hash déjà stocké pour ce couple (lat, lon) à 7 décimales, sinon en crée un."""
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT adresse_hash
            FROM distances_communes
            WHERE round(adresse_lat::numeric, 7) = round(%s::numeric, 7)
              AND round(adresse_lon::numeric, 7) = round(%s::numeric, 7)
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            (alat, alon),
        )
        row = cur.fetchone()
        if row and row[0]:
            return str(row[0])
    finally:
        cur.close()
    return _distances_adresse_hash(alat, alon, adresse_label)


def _distance_row_empty(ci: str) -> Dict[str, Any]:
    z = _normalize_insee_fr_api(ci)
    return {
        "code_insee": z,
        "code_dept": None,
        "code_postal": None,
        "commune": None,
        "distance_km": None,
        "duree_minutes": None,
    }


def _distance_compute_one_commune(alat: float, alon: float, ci_raw: str) -> Dict[str, Any]:
    """Centre api.gouv.fr + OSRM ; secours haversine (sans durée)."""
    meta = _fetch_commune_centre_geo_gouv(ci_raw)
    ci = _normalize_insee_fr_api(ci_raw)
    base_out: Dict[str, Any] = {
        "code_insee": ci,
        "code_dept": None,
        "code_postal": None,
        "commune": None,
        "distance_km": None,
        "duree_minutes": None,
    }
    if not meta:
        return base_out
    base_out["commune"] = meta.get("nom")
    cps = meta.get("codes_postaux") or []
    if cps:
        base_out["code_postal"] = str(cps[0])
    lon2, lat2 = meta["lon"], meta["lat"]
    cd = ci[:2] if len(ci) >= 2 and ci[:2].isdigit() else None
    if ci.startswith("97") or ci.startswith("98"):
        cd = ci[:3] if len(ci) >= 3 else cd
    base_out["code_dept"] = cd

    km, minutes = _osrm_route_km_minutes(alon, alat, lon2, lat2)
    src = "osrm"
    if km is None:
        km = _haversine_km(alat, alon, lat2, lon2)
        minutes = None
        src = "haversine"
    base_out["distance_km"] = round(km, 3)
    if minutes is not None:
        base_out["duree_minutes"] = round(minutes, 2)
    base_out["_source"] = src
    return base_out


def _distances_load_from_db(
    conn, alat: float, alon: float, insees: List[str]
) -> Dict[str, Dict[str, Any]]:
    """Lit le cache table réelle (adresse_lat/lon à 7 décimales), sans colonnes commune/postal/dept."""
    if not insees:
        return {}
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(
        """
        SELECT DISTINCT ON (code_insee)
            code_insee, distance_km, duree_minutes
        FROM distances_communes
        WHERE code_insee = ANY(%s)
          AND round(adresse_lat::numeric, 7) = round(%s::numeric, 7)
          AND round(adresse_lon::numeric, 7) = round(%s::numeric, 7)
        ORDER BY code_insee, updated_at DESC
        """,
        (insees, alat, alon),
    )
    rows = cur.fetchall()
    cur.close()
    out: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        ci = _normalize_insee_fr_api(str(r["code_insee"] or ""))
        out[ci] = {
            "code_insee": ci,
            "code_dept": None,
            "code_postal": None,
            "commune": None,
            "distance_km": float(r["distance_km"]) if r["distance_km"] is not None else None,
            "duree_minutes": float(r["duree_minutes"]) if r["duree_minutes"] is not None else None,
        }
    return out


def _distances_upsert_db(
    conn,
    adresse_hash: str,
    adresse_label: Optional[str],
    alat: float,
    alon: float,
    rows: List[Dict[str, Any]],
) -> None:
    """Schéma : adresse_hash, adresse_label, adresse_lat/lon, code_insee, distance_km, duree_minutes, source."""
    if not rows:
        return
    lab = (adresse_label or "").strip() or "—"
    cur = conn.cursor()
    for res in rows:
        ci = _normalize_insee_fr_api(str(res.get("code_insee") or ""))
        dk = res.get("distance_km")
        if dk is None:
            continue
        dm = res.get("duree_minutes")
        src = str(res.get("_source") or "osrm")
        if src not in ("osrm", "haversine"):
            src = "osrm"
        dk_stored = round(float(dk), 2)
        dm_stored = round(float(dm), 1) if dm is not None else None
        cur.execute(
            """
            INSERT INTO distances_communes (
                adresse_hash, adresse_label, adresse_lat, adresse_lon, code_insee,
                distance_km, duree_minutes, "source"
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (adresse_hash, code_insee) DO UPDATE SET
                adresse_label = EXCLUDED.adresse_label,
                adresse_lat = EXCLUDED.adresse_lat,
                adresse_lon = EXCLUDED.adresse_lon,
                distance_km = EXCLUDED.distance_km,
                duree_minutes = EXCLUDED.duree_minutes,
                "source" = EXCLUDED."source",
                updated_at = clock_timestamp()
            """,
            (
                adresse_hash,
                lab,
                alat,
                alon,
                ci,
                dk_stored,
                dm_stored,
                src,
            ),
        )
    cur.close()


class DistancesCommunesBody(BaseModel):
    adresse_label: Optional[str] = None
    adresse_lat: float
    adresse_lon: float
    code_insee_list: List[str]
    force_recalcul: bool = False


@app.post("/api/distances-communes")
def post_distances_communes(body: DistancesCommunesBody):
    """
    Distance et durée routière (OSRM, voir OSRM_BASE_URL) entre un point adresse (BAN) et chaque commune
    (centre api.gouv.fr). Cache : table `distances_communes` (adresse_hash + code_insee, recherche aussi
    par lat/lon à 7 décimales), puis calcul OSRM / haversine pour les manquants, avec écriture en base.
    """
    if not body.code_insee_list:
        return {"results": [], "adresse_label": body.adresse_label}
    if len(body.code_insee_list) > 2000:
        raise HTTPException(status_code=400, detail="code_insee_list limité à 2000 entrées par requête.")

    alat, alon = float(body.adresse_lat), float(body.adresse_lon)
    normalized_list = [_normalize_insee_fr_api(c) for c in body.code_insee_list]

    conn = None
    try:
        conn = get_db_connection()
    except HTTPException:
        conn = None

    db_by_insee: Dict[str, Dict[str, Any]] = {}
    if conn is not None and not body.force_recalcul:
        try:
            db_by_insee = _distances_load_from_db(conn, alat, alon, normalized_list)
        except Exception as e:
            _debug_log("distances DB lecture: %s", e)
            db_by_insee = {}

    missing_unique: List[str] = []
    seen_mu = set()
    for ci in normalized_list:
        if ci in db_by_insee:
            continue
        if ci in seen_mu:
            continue
        seen_mu.add(ci)
        missing_unique.append(ci)

    computed: List[Dict[str, Any]] = []
    if missing_unique:
        pool_workers = max(1, min(6, len(missing_unique)))

        def _compute_one(ci: str) -> Dict[str, Any]:
            return _distance_compute_one_commune(alat, alon, ci)

        with ThreadPoolExecutor(max_workers=pool_workers) as ex:
            computed = list(ex.map(_compute_one, missing_unique))

    if conn is not None and computed:
        try:
            adresse_hash = _distances_resolve_adresse_hash(conn, alat, alon, body.adresse_label)
            _distances_upsert_db(conn, adresse_hash, body.adresse_label, alat, alon, computed)
            conn.commit()
        except Exception as e:
            _debug_log("distances DB écriture: %s", e)
            try:
                conn.rollback()
            except Exception:
                pass
    if conn is not None:
        try:
            conn.close()
        except Exception:
            pass

    merged: Dict[str, Dict[str, Any]] = dict(db_by_insee)
    for row in computed:
        ci = _normalize_insee_fr_api(str(row.get("code_insee") or ""))
        merged[ci] = row

    ordered: List[Dict[str, Any]] = []
    for ci in normalized_list:
        if ci in merged:
            row = dict(merged[ci])
            row.pop("_source", None)
            ordered.append(row)
        else:
            ordered.append(_distance_row_empty(ci))

    return {"results": ordered, "adresse_label": body.adresse_label}


@app.get("/api/ventes", response_model=List[Vente])
def rechercher_ventes(
    lat: float = Query(..., description="Latitude de l'adresse centrale"),
    lon: float = Query(..., description="Longitude de l'adresse centrale"),
    rayon_km: float = Query(2.0, gt=0, le=20, description="Rayon de recherche en kilomètres"),
    type_local: Optional[str] = Query(None, description="Type de local (Appartement, Maison, etc.)"),
    surf_min: Optional[float] = Query(None, ge=0, description="Surface minimale (m²)"),
    surf_max: Optional[float] = Query(None, ge=0, description="Surface maximale (m²)"),
    date_min: Optional[date] = Query(None, description="Date de mutation minimale"),
    date_max: Optional[date] = Query(None, description="Date de mutation maximale"),
    limit: int = Query(50, gt=0, le=250, description="Nombre maximum de résultats"),
):
    """
    Recherche les ventes autour d'un point donné, dans un rayon en km.

    Hypothèse : la table `valeursfoncieres` contient des colonnes `latitude` et `longitude`
    (par exemple alimentées via géocodage BAN lors de l'ETL).
    """
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Préfiltre par cadre (bounding box) pour limiter les lignes avant le calcul de distance
        # ~111 km par degré de latitude ; longitude ajustée par cos(lat)
        deg_per_km = 1.0 / 111.0
        delta_lat = rayon_km * deg_per_km
        delta_lon = rayon_km * deg_per_km / max(0.01, math.cos(math.radians(lat)))
        lat_min, lat_max = lat - delta_lat, lat + delta_lat
        lon_min, lon_max = lon - delta_lon, lon + delta_lon

        sql = """
        SELECT
            id,
            date_mutation,
            nature_mutation,
            valeur_fonciere,
            type_local,
            surface_reelle_bati,
            surface_terrain,
            code_postal,
            commune,
            voie,
            type_de_voie,
            no_voie,
            latitude,
            longitude,
            (
              6371 * ACOS(
                COS(RADIANS(%s)) * COS(RADIANS(latitude)) *
                COS(RADIANS(longitude) - RADIANS(%s)) +
                SIN(RADIANS(%s)) * SIN(RADIANS(latitude))
              )
            ) AS distance_km
        FROM foncier.valeursfoncieres
        WHERE latitude IS NOT NULL
          AND longitude IS NOT NULL
          AND latitude BETWEEN %s AND %s
          AND longitude BETWEEN %s AND %s
        """

        params: list = [lat, lon, lat, lat_min, lat_max, lon_min, lon_max]

        if type_local:
            sql += " AND " + _sql_norm_name_canonical("type_local") + " = %s"
            params.append(_normalize_name_canonical(type_local))

        if surf_min is not None:
            sql += " AND surface_reelle_bati >= %s"
            params.append(surf_min)

        if surf_max is not None:
            sql += " AND surface_reelle_bati <= %s"
            params.append(surf_max)

        if date_min is not None:
            sql += " AND date_mutation >= %s"
            params.append(date_min)

        if date_max is not None:
            sql += " AND date_mutation <= %s"
            params.append(date_max)

        sql = f"""
        SELECT * FROM (
            {sql}
        ) AS t
        WHERE distance_km <= %s
        ORDER BY distance_km ASC, date_mutation DESC
        LIMIT %s
        """
        params.extend([rayon_km, limit])

        cur.execute(sql, params)
        rows = cur.fetchall()

        def _norm(v: Any) -> Any:
            if isinstance(v, Decimal):
                return float(v)
            if isinstance(v, datetime):
                return v.date() if hasattr(v, "date") else v
            return v

        out = []
        for row in rows:
            d = {k: _norm(v) for k, v in row.items()}
            out.append(Vente(**d))
        return out
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Erreur PostgreSQL : {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur : {e}")
    finally:
        if conn is not None:
            conn.close()


@app.get("/health")
def health_check():
    return {"status": "ok"}


# Servir le frontend (évite CORS / "failed to fetch" quand on ouvre la page depuis le même serveur)
_frontend_dir = os.path.join(os.path.dirname(__file__), "..", "frontend")
if os.path.isdir(_frontend_dir):
    from fastapi.staticfiles import StaticFiles
    app.mount("/", StaticFiles(directory=_frontend_dir, html=True), name="frontend")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True,
                  h11_max_incomplete_event_size=20_000_000)  # 20 Mo — nécessaire pour les sélections >7000 communes

