import json
import math
import os
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
def get_communes(code_dept: Optional[str] = Query(None, description="Code département (optionnel ; si absent, toutes les communes)")):
    """Liste (code_dept, code_postal, commune).

    Si code_dept fourni, filtre par département (dans vf_communes) ;
    sinon, toutes les communes depuis ref_communes (nouvelle structure Villedereve),
    avec les colonnes remappées vers (code_dept, code_postal, commune).
    """
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        if code_dept:
            # Filtrage par département dans foncier.vf_communes (structure existante agrégée)
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
            # Nouvelle table foncier.ref_communes (structure Villedereve) :
            # dep_code        -> code_dept
            # code_postal     -> code_postal
            # nom_standard_majuscule (ou nom_standard) -> commune
            try:
                cur.execute(
                    """
                    SELECT
                        dep_code AS code_dept,
                        code_postal,
                        nom_standard_majuscule AS commune
                    FROM foncier.ref_communes
                    ORDER BY nom_standard_majuscule, code_postal
                    """
                )
            except psycopg2.Error:
                # Fallback de sécurité si ref_communes n'est pas disponible :
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
            if not code_dept or not code_postal or not commune:
                raise HTTPException(status_code=400, detail="code_dept, code_postal et commune requis pour niveau=commune")
            dept_list = [code_dept]
        # Type
        if type_local:
            types = [type_local]
        else:
            types = ["Appartement", "Maison"]
        placeholders_dept = ",".join(["%s"] * len(dept_list))
        placeholders_type = ",".join(["%s"] * len(types))
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
          AND type_local IN ({placeholders_type})
          AND annee BETWEEN %s AND %s
        """
        params = list(dept_list) + list(types) + [annee_min, annee_max]
        if niveau == "commune":
            sql += " AND code_postal = %s AND commune = %s"
            params.extend([code_postal, commune])
        cur.execute(sql, params)
        # Avec dictionary=True, fetchall() renvoie déjà des dicts (clés = noms de colonnes)
        rows = cur.fetchall()
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
        return {"global": global_agg, "series": series}
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Erreur PostgreSQL : {e}")
    finally:
        if conn is not None:
            conn.close()


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
            sql += " AND type_local = %s"
            params.append(type_local)

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

