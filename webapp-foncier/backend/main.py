import json
import math
import os
import re
import unicodedata
from pathlib import Path
from datetime import date, datetime
from decimal import Decimal
from typing import Optional, List, Any, Literal

import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv


load_dotenv()

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
    Même logique que la SQL : désaccentuer d'abord (é→e, ç→c, etc.), puis ne garder que A-Z.
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


def _get_regions_and_depts(cur) -> tuple[list[dict], list[dict]]:
    """Régions (id, nom, departements) et liste des départements { code, nom }.
    Source unique : foncier.ref_regions et foncier.ref_departements (aucune liste en dur)."""
    # Liste des départements présents dans les données agrégées foncier.vf_communes
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
            region_depts = [d for d, r in ref_depts.items() if r == code_region and d in depts_in_data]
            if region_depts:
                region_depts.sort()
                regions.append({"id": code_region, "nom": nom_region, "departements": region_depts})
        departements = [{"code": c, "nom": dept_noms.get(c, c)} for c in depts_in_data]
        return regions, departements
    except (psycopg2.Error, ValueError):
        pass
    departements = [{"code": c, "nom": c} for c in depts_in_data]
    return [], departements


@app.get("/api/geo")
def get_geo():
    """Régions (métropole) et liste des départements (code + nom) présents dans vf_communes.
    Utilise ref_regions/ref_departements si présentes (ex. 38 rattaché à Auvergne-Rhône-Alpes)."""
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


@app.get("/api/communes")
def get_communes(
    code_dept: Optional[str] = Query(None, description="Code département (optionnel)"),
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
                        SELECT dep_code AS code_dept, code_postal, nom_standard_majuscule AS commune
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
                        SELECT DISTINCT code_dept, code_postal, commune
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
                        SELECT dep_code AS code_dept, code_postal, nom_standard_majuscule AS commune
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
                        SELECT DISTINCT code_dept, code_postal, commune
                        FROM foncier.vf_communes
                        WHERE """ + _sql_norm_name_canonical("commune") + """ LIKE %s
                           OR code_postal::text LIKE %s
                        ORDER BY commune, code_postal
                        LIMIT 25
                        """,
                        (search_norm_like, search_cp_like),
                    )
        elif code_dept:
            cur.execute(
                """
                SELECT DISTINCT code_dept, code_postal, commune
                FROM foncier.vf_communes
                WHERE code_dept = %s
                ORDER BY commune, code_postal
                """,
                (code_dept.strip(),),
            )
        else:
            try:
                cur.execute(
                    """
                    SELECT dep_code AS code_dept, code_postal, nom_standard_majuscule AS commune
                    FROM foncier.ref_communes
                    ORDER BY nom_standard_majuscule, code_postal
                    """
                )
            except psycopg2.Error:
                cur.execute(
                    """
                    SELECT DISTINCT code_dept, code_postal, commune
                    FROM foncier.vf_communes
                    ORDER BY commune, code_postal
                    """
                )
        rows = [{"code_dept": r[0], "code_postal": r[1], "commune": r[2]} for r in cur.fetchall()]
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


def _build_renta_lignes(
    ventes_lignes: List[dict],
    loc_lignes: List[dict],
    parc_data: Optional[dict],
    loyer_ref: Optional[float],
    taux_tfb: Optional[float],
    taux_teom: Optional[float],
    use_median: bool = True,
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
    charges_pre = {}
    for type_label in ["Maisons", "Appartements"]:
        v = ventes_by_type.get(type_label, {})
        l = loc_by_type.get(type_label, {})
        surf = _float(v.get(surface_key))
        loyer_m2 = _float(l.get(loyer_key))
        if type_label == "Maisons":
            if taux_teom is not None and surf is not None and surf > 0 and loyer_ref is not None:
                charges_pre[type_label] = (taux_teom / 100.0) * surf * 3 * loyer_ref
            else:
                charges_pre[type_label] = None
        else:
            charges_pre[type_label] = (0.10 * loyer_m2 * surf * 12) if (loyer_m2 is not None and surf is not None and surf > 0) else None
    if charges_pre.get("Maisons") is not None and charges_pre.get("Appartements") is not None and total_log > 0:
        charges_pre["Maisons/Appart."] = (nb_maisons_agreg * charges_pre["Maisons"] + nb_apparts_agreg * charges_pre["Appartements"]) / total_log
    else:
        charges_pre["Maisons/Appart."] = charges_pre.get("Maisons") if nb_apparts_agreg == 0 else (charges_pre.get("Appartements") if nb_maisons_agreg == 0 else None)

    out = []
    for type_label, _poids in [
        ("Maisons/Appart.", total_log),
        ("Maisons", nb_maisons_agreg),
        ("Appartements", nb_apparts_agreg),
    ]:
        v = ventes_by_type.get(type_label, {})
        l = loc_by_type.get(type_label, {})
        prix_m2 = _float(v.get(prix_m2_key))
        prix_total = _float(v.get(prix_total_key))
        loyer_m2 = _float(l.get(loyer_key))
        surface_mediane = _float(v.get("surface_mediane"))
        charges = charges_pre.get(type_label)
        if prix_m2 is None or prix_m2 <= 0:
            out.append({"type_bien": type_label, "renta_brute": None, "renta_hc": None, "charges_mediane": None, "renta_nette": None})
            continue
        renta_brute = (loyer_m2 * 12 / prix_m2 * 100) if loyer_m2 is not None else None
        denom = (prix_total * 1.10) if (prix_total is not None and prix_total > 0) else None
        renta_hc = None
        if loyer_m2 is not None and surface_mediane is not None and surface_mediane > 0 and denom:
            renta_hc = (loyer_m2 * surface_mediane * 12 - (tf_mediane or 0)) / denom * 100
        renta_nette = None
        if loyer_m2 is not None and surface_mediane is not None and surface_mediane > 0 and denom and charges is not None:
            renta_nette = (loyer_m2 * surface_mediane * 12 * 0.75 - 0.25 * charges - (tf_mediane or 0)) / denom * 100
        out.append({
            "type_bien": type_label,
            "renta_brute": round(renta_brute, 2) if renta_brute is not None else None,
            "renta_hc": round(renta_hc, 2) if renta_hc is not None else None,
            "charges_mediane": round(charges, 2) if charges is not None else None,
            "renta_nette": round(renta_nette, 2) if renta_nette is not None else None,
        })
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
        t = pieces_cat.upper()
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
        return {
            "nb_ventes": total_ventes,
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
        placeholders_dept = ",".join(["%s"] * len(dept_list))
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
        params = list(dept_list) + list(types_norm) + [annee_min, annee_max]
        if niveau == "commune":
            sql += " AND " + _sql_norm_name_canonical("commune") + " = %s"
            params.extend([_normalize_name_canonical(commune)])
        print("[stats] SQL (exécutable):", _debug_sql_params(sql, params))
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


@app.get("/api/fiche-logement")
def get_fiche_logement(
    code_dept: str = Query(..., description="Code département"),
    code_postal: str = Query(..., description="Code postal"),
    commune: str = Query(..., description="Nom de la commune"),
):
    """
    Données pour le Panorama Logement : parc (agreg_communes_dvf), ventes (vf_communes),
    locations (loyers_communes), rentabilités (médianes et moyennes).
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
            return {
                "code_insee": None,
                "parc": None,
                "ventes": None,
                "locations": None,
                "fiscalite": None,
                "rentabilite_mediane": None,
                "rentabilite_moyenne": None,
            }

        def _float(v: Any) -> Optional[float]:
            if v is None: return None
            try: return float(v)
            except (TypeError, ValueError): return None

        # 2) Parc logements (agreg_communes_dvf) — année max pour cette commune
        parc_annee = None
        parc_data = None
        cur.execute(
            "SELECT annee, nb_maisons, nb_apparts, prop_maison, prop_appart, surface_moy "
            "FROM foncier.agreg_communes_dvf WHERE insee_com = %s ORDER BY annee DESC LIMIT 1",
            (code_insee,),
        )
        parc_row = cur.fetchone()
        if parc_row:
            parc_annee = int(parc_row["annee"])
            nb_m = _int(parc_row.get("nb_maisons"))
            nb_a = _int(parc_row.get("nb_apparts"))
            prop_m = _float(parc_row.get("prop_maison")) or 0
            prop_a = _float(parc_row.get("prop_appart")) or 0
            total_n = nb_m + nb_a
            ratio_proprio = (prop_m * nb_m + prop_a * nb_a) / total_n if total_n else None
            parc_data = {
                "annee": parc_annee,
                "nb_maisons": nb_m,
                "nb_apparts": nb_a,
                "ratio_proprietaires": round(ratio_proprio, 2) if ratio_proprio is not None else None,
                "surface_moy": _float(parc_row.get("surface_moy")),
            }

        # 3) Ventes (vf_communes) — année max pour (code_dept, code_postal, commune)
        cur.execute(
            "SELECT MAX(annee) AS annee_max FROM foncier.vf_communes "
            "WHERE code_dept = %s AND code_postal = %s AND " + _sql_norm_name_canonical("commune") + " = %s",
            (code_dept, code_postal, _normalize_name_canonical(commune)),
        )
        ventes_annee_row = cur.fetchone()
        ventes_annee = int(ventes_annee_row["annee_max"]) if ventes_annee_row and ventes_annee_row.get("annee_max") else None
        ventes_lignes = []
        if ventes_annee:
            cur.execute(
                "SELECT type_local, nb_ventes, prix_median, prix_m2_mediane, surface_mediane, prix_moyen, prix_m2_moyenne, surface_moyenne "
                "FROM foncier.vf_communes "
                "WHERE code_dept = %s AND code_postal = %s AND " + _sql_norm_name_canonical("commune") + " = %s AND annee = %s",
                (code_dept, code_postal, _normalize_name_canonical(commune), ventes_annee),
            )
            vf_rows = cur.fetchall()
            # Mapper type_local (Appartement, Maison) vers clé
            by_type = {}
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
            # Ligne totale (Maisons + Appartements)
            if vf_rows:
                sw = sum(_int(r.get("nb_ventes")) for r in vf_rows) or 1
                def wavg_total(col):
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
                rows = by_type.get(key, [])
                if not rows:
                    ventes_lignes.append({"type": label, "nb_ventes": 0, "prix_median": None, "prix_m2_mediane": None, "surface_mediane": None, "prix_moyen": None, "prix_m2_moyenne": None, "surface_moyenne": None})
                    continue
                sw = sum(_int(r.get("nb_ventes")) for r in rows) or 1
                def wavg2b(rows, key):
                    total = sum((_float(r.get(key)) or 0) * _int(r.get("nb_ventes")) for r in rows)
                    return round(total / sw, 2) if total is not None and sw else None
                ventes_lignes.append({
                    "type": label,
                    "nb_ventes": sum(_int(r.get("nb_ventes")) for r in rows),
                    "prix_median": wavg2b(rows, "prix_median"),
                    "prix_m2_mediane": wavg2b(rows, "prix_m2_mediane"),
                    "surface_mediane": wavg2b(rows, "surface_mediane"),
                    "prix_moyen": wavg2b(rows, "prix_moyen"),
                    "prix_m2_moyenne": wavg2b(rows, "prix_m2_moyenne"),
                    "surface_moyenne": wavg2b(rows, "surface_moyenne"),
                })

        ventes_payload = {"annee": ventes_annee, "lignes": ventes_lignes} if ventes_annee else None

        # 4) Locations (loyers_communes) — année max pour insee_c
        cur.execute(
            "SELECT MAX(annee) AS annee_max FROM foncier.loyers_communes WHERE insee_c = %s",
            (code_insee,),
        )
        loc_annee_row = cur.fetchone()
        loc_annee = int(loc_annee_row["annee_max"]) if loc_annee_row and loc_annee_row.get("annee_max") else None
        loc_lignes = []
        if loc_annee:
            cur.execute(
                "SELECT type_bien, segment_surface, nbobs_com, loypredm2, lwr_ipm2, upr_ipm2 "
                "FROM foncier.loyers_communes WHERE insee_c = %s AND annee = %s",
                (code_insee, loc_annee),
            )
            loc_rows = cur.fetchall()
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
        # Taxe foncière simulée par année pour la colonne Fiscalité (surface_moy × 3 × loypredm2 × taux_tfb/100, valeur locative cadastrale)
        if surface_moy_agreg is not None and loyer_ref is not None:
            for f in fiscalite_list:
                tfb = f.get("taux_tfb")
                f["taxe_fonciere_simulee"] = round(surface_moy_agreg * 3 * loyer_ref * (tfb / 100.0), 2) if tfb is not None else None

        # 6) Rentabilités (médianes et moyennes) par type
        renta_med = _build_renta_lignes(ventes_lignes, loc_lignes, parc_data, loyer_ref, taux_tfb, taux_teom, use_median=True) if ventes_lignes and loc_lignes else []
        renta_moy = _build_renta_lignes(ventes_lignes, loc_lignes, parc_data, loyer_ref, taux_tfb, taux_teom, use_median=False) if ventes_lignes and loc_lignes else []

        cur.close()
        return {
            "code_insee": code_insee,
            "parc": parc_data,
            "ventes": ventes_payload,
            "locations": locations_payload,
            "fiscalite": fiscalite_list if fiscalite_list else None,
            "rentabilite_mediane": {"lignes": renta_med} if renta_med else None,
            "rentabilite_moyenne": {"lignes": renta_moy} if renta_moy else None,
        }
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Erreur PostgreSQL : {e}")
    finally:
        if conn is not None:
            conn.close()


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
    if fiche.get("rentabilite_mediane") and fiche["rentabilite_mediane"].get("lignes"):
        lignes = fiche["rentabilite_mediane"]["lignes"]
        if len(lignes) > 0:
            renta_nette = lignes[0].get("renta_nette")
        if len(lignes) > 1:
            renta_brute_maisons = lignes[1].get("renta_brute")
            renta_nette_maisons = lignes[1].get("renta_nette")
        if len(lignes) > 2:
            renta_brute_appts = lignes[2].get("renta_brute")
            renta_nette_appts = lignes[2].get("renta_nette")
    taux_tfb = taux_teom = None
    if fiche.get("fiscalite") and len(fiche["fiscalite"]) > 0:
        taux_tfb = fiche["fiscalite"][0].get("taux_tfb")
        taux_teom = fiche["fiscalite"][0].get("taux_teom")
    region = stats.get("reg_nom") or ""
    nom_commune = stats.get("nom_standard") or c_commune
    return {
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
        "taux_tfb": float(taux_tfb) if taux_tfb is not None else None,
        "taux_teom": float(taux_teom) if taux_teom is not None else None,
    }


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
                  c.dep_code AS code_dept, c.code_postal, c.nom_standard AS commune,
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
                  c.dep_code AS code_dept, c.code_postal, c.nom_standard AS commune,
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
    if not commune_rows:
        return {}
    total_w = 0
    sums = {}
    numeric_keys = [
        "renta_brute", "renta_nette", "renta_brute_maisons", "renta_nette_maisons",
        "renta_brute_appts", "renta_nette_appts", "taux_tfb", "taux_teom",
    ]
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


@app.get("/api/comparaison_scores")
def get_comparaison_scores(
    mode: str = Query("communes", description="Mode: communes, departements, regions"),
    code_dept: Optional[List[str]] = Query(None, description="Code département (répété pour chaque commune ou liste de depts)"),
    code_postal: Optional[List[str]] = Query(None, description="Code postal (répété pour chaque commune)"),
    commune: Optional[List[str]] = Query(None, description="Nom commune (répété pour chaque commune)"),
    code_region: Optional[List[str]] = Query(None, description="Code région (pour mode=regions, répété)"),
    score_principal: str = Query("renta_nette", description="Score principal: renta_brute, renta_nette"),
    n_max: int = Query(100, ge=1, le=500, description="Nombre max de lignes à retourner (optionnel)"),
    scores_secondaires: Optional[List[str]] = Query(None, description="Scores secondaires (taux_tfb, taux_teom, etc.)"),
):
    """
    Retourne le classement selon le score principal.
    - mode=communes : liste (code_dept, code_postal, commune) par paramètres répétés.
    - mode=departements : liste code_dept ; agrégation pondérée par population des communes du département.
    - mode=regions : liste code_region ; agrégation pondérée par population des communes de la région.
    """
    if score_principal not in ("renta_brute", "renta_nette"):
        score_principal = "renta_nette"
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
            communes_ref = _get_communes_for_aggregation(cur, code_depts=code_depts, code_regions=None)
            cur.close()
        finally:
            if conn:
                conn.close()
        rows = []
        for code_d in code_depts:
            subset = [c for c in communes_ref if (c.get("code_dept") or "").strip() == code_d]
            if not subset:
                rows.append({
                    "mode": "departement",
                    "code_dept": code_d,
                    "dep_nom": code_d,
                    "region": "",
                    "renta_brute": None, "renta_nette": None,
                    "renta_brute_maisons": None, "renta_nette_maisons": None,
                    "renta_brute_appts": None, "renta_nette_appts": None,
                    "taux_tfb": None, "taux_teom": None,
                })
                continue
            commune_indicators = []
            for c in subset:
                c_dept = str(c.get("code_dept") or "")
                c_postal = str(c.get("code_postal") or "")
                c_commune = str(c.get("commune") or "")
                try:
                    stats = get_stats(
                        niveau="commune", region_id=None, code_dept=c_dept, code_postal=c_postal, commune=c_commune,
                        type_local=None, surface_cat=None, pieces_cat=None, annee_min=None, annee_max=None,
                    )
                    fiche = get_fiche_logement(code_dept=c_dept, code_postal=c_postal, commune=c_commune)
                except Exception:
                    continue
                row = _compute_one_commune_indicators(stats, fiche, c_dept, c_postal, c_commune)
                row["population"] = int(float(c.get("population") or 1))
                row["dep_nom"] = c.get("dep_nom") or code_d
                row["code_region"] = c.get("code_region")
                commune_indicators.append(row)
            agg = _aggregate_indicators_weighted(commune_indicators, "population")
            agg["mode"] = "departement"
            agg["code_dept"] = code_d
            agg["dep_nom"] = agg.get("dep_nom") or code_d
            agg["region"] = agg.get("region") or ""
            rows.append(agg)
        rows.sort(key=lambda r: (r.get(score_principal) is None, -(r.get(score_principal) or 0)))
        if n_max and len(rows) > n_max:
            rows = rows[:n_max]
        return {"rows": rows}

    if mode == "regions":
        code_regions = [str(x or "").strip() for x in (code_region or []) if str(x or "").strip()]
        if not code_regions:
            raise HTTPException(status_code=400, detail="En mode régions, au moins un code région est requis.")
        conn = None
        try:
            conn = get_db_connection()
            cur = conn.cursor(cursor_factory=RealDictCursor)
            communes_ref = _get_communes_for_aggregation(cur, code_depts=None, code_regions=code_regions)
            cur.close()
        finally:
            if conn:
                conn.close()
        rows = []
        for code_r in code_regions:
            subset = [c for c in communes_ref if (c.get("code_region") or "").strip() == code_r]
            if not subset:
                rows.append({
                    "mode": "region",
                    "code_region": code_r,
                    "region": code_r,
                    "renta_brute": None, "renta_nette": None,
                    "renta_brute_maisons": None, "renta_nette_maisons": None,
                    "renta_brute_appts": None, "renta_nette_appts": None,
                    "taux_tfb": None, "taux_teom": None,
                })
                continue
            commune_indicators = []
            for c in subset:
                c_dept = str(c.get("code_dept") or "")
                c_postal = str(c.get("code_postal") or "")
                c_commune = str(c.get("commune") or "")
                try:
                    stats = get_stats(
                        niveau="commune", region_id=None, code_dept=c_dept, code_postal=c_postal, commune=c_commune,
                        type_local=None, surface_cat=None, pieces_cat=None, annee_min=None, annee_max=None,
                    )
                    fiche = get_fiche_logement(code_dept=c_dept, code_postal=c_postal, commune=c_commune)
                except Exception:
                    continue
                row = _compute_one_commune_indicators(stats, fiche, c_dept, c_postal, c_commune)
                row["population"] = int(float(c.get("population") or 1))
                row["code_region"] = c.get("code_region")
                row["reg_nom"] = c.get("reg_nom")
                commune_indicators.append(row)
            agg = _aggregate_indicators_weighted(commune_indicators, "population")
            agg["mode"] = "region"
            agg["code_region"] = code_r
            agg["region"] = next((c.get("reg_nom") for c in subset if c.get("reg_nom")), code_r)
            rows.append(agg)
        rows.sort(key=lambda r: (r.get(score_principal) is None, -(r.get(score_principal) or 0)))
        if n_max and len(rows) > n_max:
            rows = rows[:n_max]
        return {"rows": rows}

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
    communes = [(depts[i], postals[i], noms[i]) for i in range(n) if depts[i] and postals[i] and noms[i]]
    if not communes:
        raise HTTPException(status_code=400, detail="Au moins une commune est requise (code_dept, code_postal, commune pour chaque).")

    rows = []
    for c_dept, c_postal, c_commune in communes:
        print("[comparaison_scores] traitement: dept=%s cp=%s commune=%s" % (c_dept, c_postal, c_commune))
        try:
            stats = get_stats(
                niveau="commune",
                region_id=None,
                code_dept=c_dept,
                code_postal=c_postal,
                commune=c_commune,
                type_local=None,
                surface_cat=None,
                pieces_cat=None,
                annee_min=None,
                annee_max=None,
            )
            fiche = get_fiche_logement(code_dept=c_dept, code_postal=c_postal, commune=c_commune)
        except Exception as e:
            print("[comparaison_scores] erreur pour %s (%s %s): %s" % (c_commune, c_dept, c_postal, e))
            continue
        out = _compute_one_commune_indicators(stats, fiche, c_dept, c_postal, c_commune)
        rows.append(out)
    # Tri par score principal décroissant (nulls en dernier)
    rows.sort(key=lambda r: (r.get(score_principal) is None, -(r.get(score_principal) or 0)))
    if n_max and len(rows) > n_max:
        rows = rows[:n_max]
    print("[comparaison_scores] sortie: %s ligne(s) retournée(s)" % len(rows))
    return {"rows": rows}


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

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)

