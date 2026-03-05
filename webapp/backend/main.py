import os
from datetime import date, datetime
from decimal import Decimal
from typing import Optional, List, Any

import mysql.connector
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv


load_dotenv()


def get_db_connection():
    try:
        conn = mysql.connector.connect(
            host=os.getenv("DB_HOST", "localhost"),
            port=int(os.getenv("DB_PORT", "3306")),
            user=os.getenv("DB_USER", "root"),
            password=os.getenv("DB_PASSWORD", "secret"),
            database=os.getenv("DB_NAME", "foncier"),
        )
        return conn
    except mysql.connector.Error as e:
        raise HTTPException(status_code=500, detail=f"Erreur de connexion MySQL : {e}")


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
        cur.execute(
            "SELECT COALESCE(MIN(annee), 2020) AS annee_min, COALESCE(MAX(annee), 2025) AS annee_max FROM vf_communes"
        )
        row = cur.fetchone()
        cur.close()
        return {"annee_min": row[0], "annee_max": row[1]}
    except mysql.connector.Error as e:
        raise HTTPException(status_code=500, detail=f"Erreur MySQL : {e}")
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
        cur = conn.cursor(dictionary=True)

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
        FROM valeursfoncieres
        WHERE latitude IS NOT NULL
          AND longitude IS NOT NULL
        """

        params: list = [lat, lon, lat]

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
    except mysql.connector.Error as e:
        raise HTTPException(status_code=500, detail=f"Erreur MySQL : {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur : {e}")
    finally:
        if conn is not None:
            conn.close()


@app.get("/health")
def health_check():
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)

