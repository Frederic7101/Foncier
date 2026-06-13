import csv
import json
import os
import re
import subprocess
import sys
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from datetime import date, datetime
from decimal import Decimal
from typing import Optional, List, Any, Literal, Tuple, Dict, Set

import psycopg2
from psycopg2.extras import RealDictCursor, Json
from fastapi import FastAPI, Query, HTTPException, Response, Request, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv

# .env racine puis backend/.env — override pour que le fichier projet prime sur le shell
# (ex. CHAT_CURSOR_MODEL=claude-opus-4-7 laissé dans une session PowerShell).
load_dotenv()
load_dotenv(Path(__file__).resolve().parent / ".env", override=True)

from debug_log import DEBUG, debug_log as _debug_log
from text_norm import (
    _normalize_name_canonical,
    _normalize_commune_name_for_map_match,
    _debug_sql_params,
    _sql_norm_name,
    _normalize_code_dept_for_vf,
    _normalize_code_postal_for_vf,
    _normalize_code_postal_for_ref_communes,
    _sql_norm_name_canonical,
    _sql_norm_name_canonical_commune_vf,
    _sql_libgeo_ville_canonical,
    _normalize_name,
)
from db import get_db_connection
from models import Vente, ComparaisonScoresBody, DistancesCommunesBody
from services.period import fetch_period
from services.geo import fetch_geo, fetch_refs_comparaison_logement, fetch_communes
from services.ventes import search_ventes
from services.ign_tiles import fetch_ign_tile
from services.stats import fetch_stats
from vf_agg import _agg_rows, _float, _int
from vf_ventes import (
    PERIODES_DVF_ANNEES,
    _build_ventes_lignes_for_tranche,
    _build_ventes_lignes_from_vf_rows,
    _fetch_vf_communes_range,
    _vf_extra_type_label,
    EXTRA_VENTE_TYPE_ORDER,
)
from services.panorama import (
    fetch_castorus_ventes,
    fetch_leboncoin_locations,
    fetch_licitor_ventes,
    fetch_panorama_ventes,
    run_leboncoin_extract,
)

from renta import _extract_rentas_from_lignes
from services.fiche import fetch_fiche_logement
from indicateurs_cols import NB_LOCAUX_TRANCHE_COLS, TRANCHE_RENTA_COLS
from indicateurs_read import (
    _append_tranche_floats_from_db_row,
    _is_valid_rentability_row,
    _merge_periode_into_row,
    _read_indicateurs_communes,
    _resolve_communes_to_ref,
)
from services.comparaison import fetch_comparaison_scores
from services.distances import compute_distances_communes
from chat.router import router as chat_router




app = FastAPI(title="API Foncier", version="1.0.0")
app.include_router(chat_router)

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
    return fetch_period()



@app.get("/api/ign-tiles/{z}/{x}/{y}.png")
def get_ign_tile_cached(
    z: int,
    x: int,
    y: int,
    refresh: bool = Query(False, description="Forcer le rechargement IGN en ignorant le cache local"),
):
    """Retourne une tuile IGN via cache local disque."""
    return fetch_ign_tile(z, x, y, refresh=refresh)


@app.get("/api/geo")
def get_geo():
    """Régions et départements depuis ref_regions / ref_departements."""
    return fetch_geo()



@app.get("/api/refs-comparaison-logement")
def get_refs_comparaison_logement():
    """Listes ref_type_logts, ref_type_surf, ref_nb_pieces."""
    return fetch_refs_comparaison_logement()



@app.get("/api/communes")
def get_communes(
    code_dept: Optional[str] = Query(None, description="Code département (optionnel)"),
    code_region: Optional[str] = Query(None, description="Code région (optionnel)"),
    all_France: bool = Query(False, description="Retourner toutes les communes de la France (optionnel)"),
    q: Optional[str] = Query(None, description="Recherche par nom de commune ou code postal (optionnel, filtre ILIKE/LIKE)"),
):
    """Liste (code_dept, code_postal, commune)."""
    return fetch_communes(
        code_dept=code_dept,
        code_region=code_region,
        all_france=all_France,
        q=q,
    )




_INDICATEURS_COMMUNES_DATA_COLS: Tuple[str, ...] = (
    "code_insee",
    "code_dept",
    "code_postal",
    "commune",
    "reg_nom",
    "dep_nom",
    "population",
    "nb_locaux",
    "nb_locaux_parking",
    "nb_locaux_local_indus",
    "nb_locaux_terrain",
    "nb_locaux_immeuble",
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
        ["code_insee", "code_dept", "code_postal", "commune", "reg_nom", "dep_nom", "population", "nb_locaux", "nb_locaux_maisons", "nb_locaux_appts", "nb_locaux_parking", "nb_locaux_local_indus", "nb_locaux_terrain", "nb_locaux_immeuble", "nb_ventes_dvf", "indicateurs_par_periode"]
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
# SAVEPOINT pour refresh batch (commit=False) : une erreur SQL n???annule pas toute la transaction.
_UPSERT_INDICATEURS_COMMUNES_SAVEPOINT = "sp_upsert_indicateurs_communes"


def _clamp_numeric_6_2(v: Any) -> Optional[float]:
    """Colonnes NUMERIC(6,2) en base : |valeur| doit ??tre < 10^4 (bornes ??9999.99).

    Important : `round(x, 2)` en Python peut produire 10000.0 pour des entr??es proches
    de la borne (ex. round(9999.995, 2) -> 10000.0), ce qui d??passe encore NUMERIC(6,2).
    On borne donc avant **et** apr??s l'arrondi.
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
        row.get("nb_locaux_parking"),
        row.get("nb_locaux_local_indus"),
        row.get("nb_locaux_terrain"),
        row.get("nb_locaux_immeuble"),
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
    for col in ("nb_locaux_parking", "nb_locaux_local_indus", "nb_locaux_terrain", "nb_locaux_immeuble"):
        col_sum = 0
        for snap, _ in buckets:
            if snap.get(col) is not None:
                try:
                    col_sum += int(snap[col])
                except (TypeError, ValueError):
                    pass
        out[col] = col_sum if col_sum else None
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
    """Agrégats vf_communes selon niveau (région/département/commune)."""
    return fetch_stats(
        niveau=niveau,
        region_id=region_id,
        code_dept=code_dept,
        code_postal=code_postal,
        commune=commune,
        type_local=type_local,
        surface_cat=surface_cat,
        pieces_cat=pieces_cat,
        annee_min=annee_min,
        annee_max=annee_max,
    )




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
    return fetch_fiche_logement(
        code_dept=code_dept,
        code_postal=code_postal,
        commune=commune,
    )






@app.get("/api/panorama-ventes")
def get_panorama_ventes(
    code_dept: str = Query(..., description="Code département"),
    code_postal: str = Query(..., description="Code postal"),
    commune: str = Query(..., description="Nom de la commune"),
    periode_annees: int = Query(1, ge=1, le=5, description="Période en années (1, 2, 3 ou 5)"),
    surface_cat: str = Query("", description="Catégorie surface (S1-S5) ou vide"),
    pieces_cat: str = Query("", description="Catégorie pièces (T1-T5) ou vide"),
):
    return fetch_panorama_ventes(
        code_dept=code_dept,
        code_postal=code_postal,
        commune=commune,
        periode_annees=periode_annees,
        surface_cat=surface_cat,
        pieces_cat=pieces_cat,
    )


@app.get("/api/licitor-ventes")
def get_licitor_ventes(
    code_dept: str = Query(..., description="Code département"),
    commune: str = Query(..., description="Nom de la commune"),
    periode_annees: int = Query(1, ge=1, le=5, description="Période en années (1-5)"),
):
    return fetch_licitor_ventes(code_dept=code_dept, commune=commune, periode_annees=periode_annees)


@app.get("/api/castorus-ventes")
def get_castorus_ventes(
    code_dept: str = Query(..., description="Code département"),
    commune: str = Query(..., description="Nom de la commune"),
    periode_annees: int = Query(1, ge=1, le=5, description="Période en années (1-5)"),
):
    return fetch_castorus_ventes(
        code_dept=code_dept,
        commune=commune,
        periode_annees=periode_annees,
    )


@app.get("/api/leboncoin-locations")
def get_leboncoin_locations(
    code_dept: str = Query(..., description="Code département"),
    code_postal: str = Query(..., description="Code postal"),
    commune: str = Query(..., description="Nom de la commune"),
    periode_annees: int = Query(1, ge=1, le=5, description="Période en années (1-5)"),
    surface_cat: str = Query("", description="Catégorie surface (S1-S5) ou vide"),
    pieces_cat: str = Query("", description="Catégorie pièces (T1-T5) ou vide"),
):
    return fetch_leboncoin_locations(
        code_dept=code_dept,
        code_postal=code_postal,
        commune=commune,
        periode_annees=periode_annees,
        surface_cat=surface_cat,
        pieces_cat=pieces_cat,
    )


@app.post("/api/leboncoin-extract")
def post_leboncoin_extract(
    commune: str = Query(..., description="Nom de la commune (ex. Noisy-le-Grand)"),
    code_postal: str = Query(..., description="Code postal à 5 chiffres"),
):
    return run_leboncoin_extract(commune=commune, code_postal=code_postal)


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
    nb_locaux_parking = nb_locaux_local_indus = nb_locaux_terrain = nb_locaux_immeuble = None
    if fiche.get("rentabilite_mediane") and fiche["rentabilite_mediane"].get("lignes"):
        lignes = fiche["rentabilite_mediane"]["lignes"]
        rx = _extract_rentas_from_lignes(lignes)
        nb_locaux = rx.get("nb_locaux")
        nb_locaux_maisons = rx.get("nb_locaux_maisons")
        nb_locaux_appts = rx.get("nb_locaux_appts")
        nb_locaux_parking = rx.get("nb_locaux_parking")
        nb_locaux_local_indus = rx.get("nb_locaux_local_indus")
        nb_locaux_terrain = rx.get("nb_locaux_terrain")
        nb_locaux_immeuble = rx.get("nb_locaux_immeuble")
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
        "nb_locaux_parking": int(nb_locaux_parking) if nb_locaux_parking is not None else None,
        "nb_locaux_local_indus": int(nb_locaux_local_indus) if nb_locaux_local_indus is not None else None,
        "nb_locaux_terrain": int(nb_locaux_terrain) if nb_locaux_terrain is not None else None,
        "nb_locaux_immeuble": int(nb_locaux_immeuble) if nb_locaux_immeuble is not None else None,
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
    """Retourne le classement selon le score principal."""
    return fetch_comparaison_scores(
        mode=mode,
        scope=scope,
        code_dept=code_dept,
        code_postal=code_postal,
        commune=commune,
        code_region=code_region,
        code_insee=code_insee,
        exclude_code_insee=exclude_code_insee,
        exclude_code_dept=exclude_code_dept,
        score_principal=score_principal,
        n_max=n_max,
        nb_locaux_min=nb_locaux_min,
        renta_brute_min=renta_brute_min,
        renta_nette_min=renta_nette_min,
        periode_annees=periode_annees,
        scores_secondaires=scores_secondaires,
        type_logt=type_logt,
        type_surf=type_surf,
        nb_pieces=nb_pieces,
    )


@app.post("/api/comparaison_scores")
def post_comparaison_scores(body: ComparaisonScoresBody):
    """Version POST de /api/comparaison_scores — identique mais accepte un body JSON."""
    return fetch_comparaison_scores(
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
_REFRESH_INDICATEURS_KNOWN_PARAMS = {"code_insee_list", "limit", "batch_commit", "workers", "force"}
_REFRESH_INDICATEURS_USAGE = (
    "Usage: POST /api/refresh-indicateurs?code_insee_list=<code1>&code_insee_list=<code2>&... "
    "(paramètre sans 'e' final). Exemple: POST /api/refresh-indicateurs?code_insee_list=97306"
)

def _refresh_indicateurs_impl(
    code_insee_list: Optional[List[str]],
    limit: Optional[int],
    _batch_commit: int,
    workers: int,
    force: bool = False,
) -> dict:
    """
    Corps métier de POST /api/refresh-indicateurs (sans validation HTTP des query params).
    Appelable depuis d'autres endpoints (ex. force-recalcul) sans objet Request.
    _batch_commit : conservé pour compatibilité API (non utilisé : un COMMIT par upsert commune).
    force : si True, purge fiche_logement_cache des communes ciblées avant lecture pour forcer
    une régénération complète du payload fiche (utile après un changement de formule, ex. rentabilité).
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

        # Option force : purge du cache fiche pour les communes ciblées, forçant une régénération
        # complète des payloads (utile après un changement de formule dans _build_renta_lignes).
        if force:
            cur.execute(
                "DELETE FROM foncier.fiche_logement_cache WHERE code_insee IN (" + placeholders + ")",
                to_process,
            )
            conn.commit()
            _debug_log("[refresh-indicateurs] force=True : cache fiche purgé pour %d communes", len(to_process))

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
                fiche = fetch_fiche_logement(
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
    force: bool = Query(False, description="Si true, purge fiche_logement_cache avant traitement pour forcer une régénération complète (utile après un changement de formule de calcul, ex. rentabilité)."),
):
    """
    Remplit ou met à jour fiche_logement_cache et indicateurs_communes pour les communes demandées.
    Si code_insee_list est fourni, traite uniquement ces code_insee. Sinon traite les communes de ref_communes
    (sans limite si limit absent ou 0, sinon limit communes).
    Lit d'abord le cache fiche quand il existe ; les fiches manquantes sont calculées en parallèle (workers).
    force=true : purge le cache fiche des communes ciblées avant la lecture, obligeant la régénération
    des payloads (nécessaire après un changement de formule dans _build_renta_lignes, etc.).
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
    return _refresh_indicateurs_impl(code_insee_list, limit, batch_commit, workers, force=force)


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
    - departements / regions: purge cache fiche + indicateurs_communes de TOUTES les communes
      des départements/régions ciblés, refresh ciblé, puis recalcule les agrégats.
      Cela garantit que les changements de formule (ex. _build_renta_lignes) soient bien répercutés
      jusqu'aux agrégats, qui lisent depuis indicateurs_communes.
    """
    mode = (mode or "communes").strip().lower()
    if mode == "departements":
        targets = [str(x or "").strip() for x in (code_dept or []) if str(x or "").strip()]
        if not targets:
            raise HTTPException(status_code=400, detail="En mode départements, fournir au moins un code_dept.")
        # 1) Lister les communes des départements cibles
        conn = None
        try:
            conn = get_db_connection()
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute(
                "SELECT DISTINCT code_insee FROM foncier.ref_communes "
                "WHERE dep_code = ANY(%s) AND code_insee IS NOT NULL AND code_insee != ''",
                (targets,),
            )
            code_insee_list = [str(r["code_insee"]).strip() for r in cur.fetchall()]
            cur.close()
        finally:
            if conn:
                conn.close()
        # 2) Refresh des communes avec force=True (purge fiche cache + regen)
        _invalidate_indic_cols_cache()
        out_communes = _refresh_indicateurs_impl(
            code_insee_list=code_insee_list,
            limit=None,
            _batch_commit=batch_commit,
            workers=workers,
            force=True,
        )
        # 3) Recalculer les agrégats départements à partir des indicateurs_communes fraîchement écrits
        out_depts = refresh_indicateurs_agreges(
            code_dept_list=targets, code_region_list=None, refresh_all=False, force=True
        )
        return {
            "mode": "departements",
            "communes_refreshed": out_communes.get("refreshed", 0),
            "communes_requested": out_communes.get("requested", 0),
            "departements_refreshed": out_depts.get("departements_refreshed", 0) if isinstance(out_depts, dict) else None,
        }
    if mode == "regions":
        targets = [str(x or "").strip() for x in (code_region or []) if str(x or "").strip()]
        if not targets:
            raise HTTPException(status_code=400, detail="En mode régions, fournir au moins un code_region.")
        # 1) Lister les communes des régions cibles
        conn = None
        try:
            conn = get_db_connection()
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute(
                "SELECT DISTINCT c.code_insee FROM foncier.ref_communes c "
                "LEFT JOIN foncier.ref_departements d ON d.code_dept = c.dep_code "
                "WHERE d.code_region = ANY(%s) AND c.code_insee IS NOT NULL AND c.code_insee != ''",
                (targets,),
            )
            code_insee_list = [str(r["code_insee"]).strip() for r in cur.fetchall()]
            cur.close()
        finally:
            if conn:
                conn.close()
        # 2) Refresh des communes avec force=True (purge fiche cache + regen)
        _invalidate_indic_cols_cache()
        out_communes = _refresh_indicateurs_impl(
            code_insee_list=code_insee_list,
            limit=None,
            _batch_commit=batch_commit,
            workers=workers,
            force=True,
        )
        # 3) Recalculer les agrégats régions à partir des indicateurs_communes fraîchement écrits
        out_regs = refresh_indicateurs_agreges(
            code_dept_list=None, code_region_list=targets, refresh_all=False, force=True
        )
        return {
            "mode": "regions",
            "communes_refreshed": out_communes.get("refreshed", 0),
            "communes_requested": out_communes.get("requested", 0),
            "regions_refreshed": out_regs.get("regions_refreshed", 0) if isinstance(out_regs, dict) else None,
        }

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
        force=True,
    )
    out["requested"] = len(communes_in)
    out["resolved"] = len(code_insee_list)
    return out


@app.post("/api/distances-communes")
def post_distances_communes(body: DistancesCommunesBody):
    """Distance et durée routière (OSRM) entre une adresse BAN et chaque commune (centre api.gouv.fr)."""
    return compute_distances_communes(body)


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
    """Recherche les ventes autour d'un point donné, dans un rayon en km."""
    return search_ventes(
        lat=lat,
        lon=lon,
        rayon_km=rayon_km,
        type_local=type_local,
        surf_min=surf_min,
        surf_max=surf_max,
        date_min=date_min,
        date_max=date_max,
        limit=limit,
    )



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

