from __future__ import annotations

import argparse
import csv
import json
import itertools
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Callable

import psycopg2
from psycopg2 import extras as pg_extras
import requests
from dotenv import load_dotenv

# Forcer un bundle CA valide pour HTTPS (API BAN).
# Évite OSError quand SSL_CERT_FILE / REQUESTS_CA_BUNDLE pointent vers un chemin invalide (autre logiciel).
try:
    import certifi
    _CA_BUNDLE = certifi.where()
    # Forcer ce bundle pour tout le processus (API BAN)
    os.environ["SSL_CERT_FILE"] = _CA_BUNDLE
    os.environ["REQUESTS_CA_BUNDLE"] = _CA_BUNDLE
except ImportError:
    _CA_BUNDLE = True

# Affichage unique de l'avertissement 429 (rate limit BAN)
_rate_limit_warned = False
_rate_limit_lock = threading.Lock()

# Ajustement auto des workers : 429 → réduire de 30 % ; après 30 min → remonter de 30 %
_workers_429_seen = False
_workers_ramp_lock = threading.Lock()
_workers_last_ramp_time = 0.0


load_dotenv()

YEARS = (2020, 2021, 2022, 2023, 2024, 2025)

# Schéma PostgreSQL (compatibilité : noms de tables qualifiés)
DB_SCHEMA = "ventes_notaire"

# Dossiers où chercher config.json (script dir, webapp-foncier/, racine projet)
_CONFIG_DIRS = (
    Path(__file__).resolve().parent,           # webapp-foncier/scripts/
    Path(__file__).resolve().parent.parent,   # webapp-foncier/
    Path(__file__).resolve().parent.parent.parent,  # racine projet
)


def _load_db_config() -> dict:
    """Charge la config DB depuis config.json ou variables d'environnement (.env)."""
    for base in _CONFIG_DIRS:
        config_path = base / "config.postgres.json"
        if config_path.is_file():
            try:
                with open(config_path, encoding="utf-8") as f:
                    #print(f"Loading config from {config_path}")
                    data = json.load(f)
                db = data.get("database") or data
                #print(f"Config loaded from {config_path}: {db}")
                return {
                    "host": db.get("host"),
                    "port": int(db.get("port") or 5432),
                    "user": db.get("user"),
                    "password": db.get("password"),
                    "database": db.get("database"),
                    "schema": db.get("schema", "ventes_notaire"),
                }
                print(f"Config loaded: {config}")
            except (json.JSONDecodeError, OSError):
                print(f"Error loading config from {config_path}: {e}")
                pass
    # Pas de config.json valide : tout depuis l'environnement
    print("No config.postgres.json found, using environment variables")
    return {
        "host": os.getenv("DB_HOST", ""),
        "port": int(os.getenv("DB_PORT", "5432") or 5432),
        "user": os.getenv("DB_USER", ""),
        "password": os.getenv("DB_PASSWORD", ""),
        "database": os.getenv("DB_NAME", ""),
        "schema": os.getenv("DB_SCHEMA", "ventes_notaire"),
    }


def get_connection():
    cfg = _load_db_config()
    conn = psycopg2.connect(
        host=cfg["host"],
        port=cfg["port"],
        user=cfg["user"],
        password=cfg["password"],
        dbname=cfg["database"],
    )
    schema = cfg.get("schema", "ventes_notaire")
    with conn.cursor() as cur:
        # ventes_notaire en premier (tables), public pour digest() du trigger (pgcrypto)
        cur.execute("SET search_path TO %s, public", (schema,))
        # Extension requise par le trigger vf_set_dedup_key sur valeursfoncieres (digest)
        try:
            cur.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")
        except psycopg2.Error as e:
            conn.rollback()
            if "permission" in str(e).lower() or "right" in str(e).lower():
                raise SystemExit(
                    "L'extension pgcrypto est requise (trigger sur valeursfoncieres).\n"
                    "Exécutez une fois en superutilisateur : CREATE EXTENSION IF NOT EXISTS pgcrypto;"
                ) from e
            with conn.cursor() as cur2:
                cur2.execute("SET search_path TO %s, public", (schema,))
    conn.commit()
    return conn


def update_valeursfoncieres_for_year(conn, year: int) -> int:
    """Recopie lat/lon de adresses_geocodees vers valeursfoncieres pour une année."""
    with conn.cursor() as cur:
        sql = f"""
            UPDATE {DB_SCHEMA}.valeursfoncieres v
            SET latitude = a.latitude, longitude = a.longitude
            FROM {DB_SCHEMA}.adresses_geocodees a
            WHERE a.adresse_norm = CONCAT_WS(' ',
                  COALESCE(v.no_voie, ''),
                  COALESCE(v.type_de_voie, ''),
                  COALESCE(v.voie, ''),
                  v.code_postal,
                  v.commune
              )
              AND (v.latitude IS NULL OR v.longitude IS NULL)
              AND (a.latitude != 0 OR a.longitude != 0)
              AND v.date_mutation >= %s AND v.date_mutation < %s
        """
        date_min = f"{year}-01-01"
        date_max = f"{year + 1}-01-01"
        cur.execute(sql, (date_min, date_max))
        affected = cur.rowcount
    conn.commit()
    return affected


def run_update_by_year():
    """Lance l'UPDATE valeursfoncieres par année (2020 à 2025)."""
    conn = get_connection()
    try:
        for year in YEARS:
            print(f"Update année {year}...", end=" ", flush=True)
            n = update_valeursfoncieres_for_year(conn, year)
            print(f"{n} ligne(s) mises à jour.")
    finally:
        conn.close()
    print("Terminé.")


def load_proxy_list(proxy_file: str | None) -> list[str]:
    """
    Charge la liste de proxies : --proxy-file (une URL par ligne) ou variable d'env PROXY_LIST (séparée par des virgules).
    Format par proxy : http://host:port, https://host:port, ou socks5://host:port (nécessite PySocks pour SOCKS).
    """
    urls: list[str] = []
    if proxy_file and os.path.isfile(proxy_file):
        with open(proxy_file, encoding="utf-8") as f:
            for line in f:
                u = line.strip()
                if u and not u.startswith("#"):
                    urls.append(u)
    env_list = os.getenv("PROXY_LIST", "").strip()
    if env_list:
        urls.extend(u.strip() for u in env_list.split(",") if u.strip())
    return urls


def _make_proxy_getter(proxy_urls: list[str]):
    """Retourne un callable thread-safe qui renvoie à chaque appel le prochain proxy (round-robin)."""
    if not proxy_urls:
        return None
    cycle = itertools.cycle(proxy_urls)
    lock = threading.Lock()

    def getter():
        with lock:
            url = next(cycle)
        return {"http": url, "https": url}

    return getter


def _normalize_address(addr: str) -> str:
    """Normalise l'adresse pour l'API BAN : espaces, encodage, caractères problématiques."""
    if not addr or not isinstance(addr, str):
        return ""
    # Espaces insécables / caractères de contrôle → espace ordinaire
    s = addr.strip().replace("\u00a0", " ").replace("\r", " ").replace("\n", " ")
    # Supprimer les caractères nuls ou non imprimables
    s = "".join(c for c in s if c != "\x00" and (c.isprintable() or c.isspace()))
    return " ".join(s.split())  # normaliser les espaces multiples


def _geocode_one(
    row: dict,
    sleep_seconds: float = 0.1,
    proxy_getter: Callable[[], dict[str, str]] | None = None,
) -> tuple:
    """Appelle l'API BAN pour une adresse. Retourne (id, lat, lon) ou (id, None, None)."""
    global _rate_limit_warned, _workers_429_seen
    addr_id = row["id"]
    addr = _normalize_address(row["adresse_norm"])
    if not addr:
        return (addr_id, None, None)
    params = {"q": addr, "limit": 3}
    url = "https://api-adresse.data.gouv.fr/search/"
    for attempt in range(2):
        proxies = proxy_getter() if proxy_getter else None
        try:
            r = requests.get(url, params=params, timeout=15, proxies=proxies, verify=_CA_BUNDLE)
            if r.status_code == 429:
                with _rate_limit_lock:
                    if not _rate_limit_warned:
                        _rate_limit_warned = True
                        print(
                            "\n[ATTENTION] API BAN : trop de requêtes (429). Pause 60s puis retry. "
                            "Réduisez --workers et augmentez --sleep pour éviter le blocage.",
                            flush=True,
                        )
                with _workers_ramp_lock:
                    _workers_429_seen = True
                time.sleep(60)
                continue
            r.raise_for_status()
            data = r.json()
            feats = data.get("features", [])
            if not feats:
                return (addr_id, None, None)
            coords = feats[0]["geometry"]["coordinates"]
            lon, lat = coords[0], coords[1]
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)
            return (addr_id, lat, lon)
        except requests.RequestException as e:
            # Timeout, ProxyError, ConnectionError, HTTPError (4xx/5xx) : log pour diagnostic
            # print(f"[BAN] id={addr_id} {type(e).__name__}: {e}", flush=True)
            if attempt == 0:
                time.sleep(5)
                continue
            return (addr_id, None, None)
        except Exception as e:
            # ex. JSONDecodeError si la réponse n'est pas du JSON
            print(f"[BAN] id={addr_id} {type(e).__name__}: {e}", flush=True)
            return (addr_id, None, None)
    return (addr_id, None, None)


def _log_progress(
    processed: int,
    total: int,
    ok: int,
    skip: int,
    start_time: float,
    log_every: int | None,
    log_interval: float | None,
    last_log_time: float,
    last_log_processed: int,
) -> tuple[float, int]:
    """Affiche un message de progression si log_every ou log_interval est déclenché. Retourne (last_log_time, last_log_processed)."""
    now = time.monotonic()
    elapsed = now - start_time
    do_log = False
    if log_every and processed > 0 and (processed - last_log_processed) >= log_every:
        do_log = True
    if log_interval and processed > 0 and (now - last_log_time) >= log_interval:
        do_log = True
    if not do_log or processed == 0:
        return last_log_time, last_log_processed
    rate = processed / elapsed if elapsed > 0 else 0
    ts = time.strftime("%H:%M:%S", time.localtime())
    print(f"[{ts}] {processed}/{total} traitées — {ok} OK, {skip} sans résultat — {rate:.1f} adresses/s", flush=True)
    return now, processed


def _ensure_geocode_failed_column(conn) -> None:
    """Vérifie que la colonne geocode_failed existe ; sinon lève une erreur explicite."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT id FROM {DB_SCHEMA}.adresses_geocodees WHERE COALESCE(geocode_failed::int, 0) = 0 LIMIT 0"
            )
    except psycopg2.Error as e:
        if "geocode_failed" in str(e).lower() or "column" in str(e).lower():
            raise SystemExit(
                "La colonne 'geocode_failed' est absente. Exécutez la migration SQL.\n"
                "Exemple : psql -U postgres -d foncier -f webapp-foncier/sql/add_geocode_failed.sql"
            ) from e
        raise


def run_geocode(
    batch_size: int = 1000,
    sleep_seconds: float = 0.1,
    workers: int = 1,
    commit_every: int = 100,
    log_every: int | None = None,
    log_interval: float | None = None,
    proxy_file: str | None = None,
):
    """Géocode uniquement les adresses restant à géocoder (lat=0, lon=0, geocode_failed=0).
    Les adresses pour lesquelles BAN ne renvoie rien sont marquées geocode_failed=1
    et ne seront plus retraitées aux exécutions suivantes."""
    global _workers_429_seen, _workers_last_ramp_time
    conn0 = get_connection()
    try:
        _ensure_geocode_failed_column(conn0)
    finally:
        conn0.close()

    proxy_list = load_proxy_list(proxy_file)
    proxy_getter = _make_proxy_getter(proxy_list) if proxy_list else None
    if proxy_list:
        print(f"Rotation de {len(proxy_list)} proxy(s) activée.")

    batch_num = 0
    total_ok, total_skip = 0, 0
    max_workers = workers
    current_workers = workers
    RAMP_INTERVAL = 30 * 60  # 30 minutes en secondes

    while True:
        batch_num += 1
        conn = get_connection()
        with conn.cursor(cursor_factory=pg_extras.RealDictCursor) as cur:
            cur.execute(
                f"""
                SELECT id, adresse_norm
                FROM {DB_SCHEMA}.adresses_geocodees
                WHERE latitude = 0 AND longitude = 0
                  AND COALESCE(geocode_failed::int, 0) = 0
                LIMIT %s
                """,
                (batch_size,),
            )
            rows = cur.fetchall()

        if not rows:
            conn.close()
            if batch_num == 1:
                print("Aucune adresse à géocoder.")
            else:
                print("Toutes les adresses ont été géocodées.")
                print(f"Récap: {total_ok} mis à jour, {total_skip} sans résultat ou erreur.")
            return

        # Ajustement auto des workers : 429 → -30 % ; après 30 min sans 429 → +30 %
        with _workers_ramp_lock:
            if _workers_429_seen:
                _workers_429_seen = False
                old_w = current_workers
                current_workers = max(1, int(current_workers * 0.7))
                _workers_last_ramp_time = time.monotonic()
                print(f"\n[429] Réduction des workers : {old_w} → {current_workers} (remontée dans 30 min si pas de 429).", flush=True)
            elif current_workers < max_workers and (time.monotonic() - _workers_last_ramp_time) >= RAMP_INTERVAL:
                old_w = current_workers
                current_workers = min(max_workers, max(1, int(current_workers * 1.3)))
                _workers_last_ramp_time = time.monotonic()
                print(f"\n[Remontée] Workers augmentés : {old_w} → {current_workers} (max {max_workers}).", flush=True)

        workers = current_workers
        total = len(rows)
        print(f"\n--- Batch {batch_num}: {total} adresse(s) (workers={workers}, commit tous les {commit_every}) ---")
        if workers > 15 or sleep_seconds < 0.15:
            print("(Recommandation: workers ≤10 et --sleep ≥0.2 pour limiter le risque de blocage par l'API BAN.)")
        if log_every or log_interval:
            print("Progression: log_every={}, log_interval={}s".format(log_every or "off", log_interval or "off"))

        start_time = time.monotonic()
        last_log_time = start_time
        last_log_processed = 0
        progress_ok, progress_skip = 0, 0

        if workers <= 1:
            results = []
            for i, r in enumerate(rows):
                res = _geocode_one(r, sleep_seconds, proxy_getter)
                results.append(res)
                if res[1] is not None and res[2] is not None:
                    progress_ok += 1
                else:
                    progress_skip += 1
                processed = i + 1
                last_log_time, last_log_processed = _log_progress(
                    processed, total, progress_ok, progress_skip,
                    start_time, log_every, log_interval, last_log_time, last_log_processed,
                )
        else:
            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = {executor.submit(_geocode_one, r, sleep_seconds, proxy_getter): r for r in rows}
                results = []
                for fut in as_completed(futures):
                    res = fut.result()
                    results.append(res)
                    if res[1] is not None and res[2] is not None:
                        progress_ok += 1
                    else:
                        progress_skip += 1
                    processed = len(results)
                    last_log_time, last_log_processed = _log_progress(
                        processed, total, progress_ok, progress_skip,
                        start_time, log_every, log_interval, last_log_time, last_log_processed,
                    )

        # Batch UPDATE en base : succès (lat/lon) ou marquage échec (geocode_failed)
        update_ok_sql = (
            f"UPDATE {DB_SCHEMA}.adresses_geocodees "
            "SET latitude = %s, longitude = %s, last_refreshed = NOW(), geocode_failed = FALSE "
            "WHERE id = %s"
        )
        row_by_id = {r["id"]: r.get("adresse_norm", "") for r in rows}
        ok, skip = 0, 0
        batch_ok = []
        ids_failed = []
        rejets = []  # (id, adresse_norm) pour le CSV
        for addr_id, lat, lon in results:
            if lat is None or lon is None:
                skip += 1
                ids_failed.append(addr_id)
                rejets.append((addr_id, row_by_id.get(addr_id, "")))
                continue
            batch_ok.append((lat, lon, addr_id))
            if len(batch_ok) >= commit_every:
                with conn.cursor() as cur:
                    cur.executemany(update_ok_sql, batch_ok)
                conn.commit()
                ok += len(batch_ok)
                batch_ok = []
        if batch_ok:
            with conn.cursor() as cur:
                cur.executemany(update_ok_sql, batch_ok)
            conn.commit()
            ok += len(batch_ok)
        if ids_failed:
            with conn.cursor() as cur:
                placeholders = ",".join(["%s"] * len(ids_failed))
                cur.execute(
                    f"UPDATE {DB_SCHEMA}.adresses_geocodees SET geocode_failed = TRUE WHERE id IN ({placeholders})",
                    ids_failed,
                )
            conn.commit()
        conn.close()

        if rejets:
            rejets_path = Path(__file__).resolve().parent / "rejets_geoloc_BAN.csv"
            file_exists = rejets_path.exists()
            with open(rejets_path, "a", newline="", encoding="utf-8") as f:
                w = csv.writer(f, delimiter=";", quoting=csv.QUOTE_MINIMAL)
                if not file_exists:
                    w.writerow(["id", "adresse_norm"])
                for rid, adresse_norm in rejets:
                    w.writerow([rid, adresse_norm])

        total_ok += ok
        total_skip += skip
        elapsed = time.monotonic() - start_time
        print(f"Batch {batch_num} terminé: {ok} mis à jour, {skip} sans résultat — {elapsed:.1f}s")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Géocodage BAN et mise à jour lat/lon valeursfoncieres")
    parser.add_argument(
        "--update",
        action="store_true",
        help="Exécuter l'UPDATE valeursfoncieres par année (2020-2025) uniquement",
    )
    parser.add_argument("--batch", type=int, default=1000, help="Taille du batch de géocodage (défaut: 1000)")
    parser.add_argument("--sleep", type=float, default=0.1, help="Pause en secondes entre deux appels BAN (défaut: 0.1)")
    parser.add_argument(
        "--workers",
        type=int,
        default=5,
        help="Nombre d'appels BAN en parallèle (défaut: 5). Garder ≤10 et --sleep ≥0.2 pour éviter le blocage par l'API.",
    )
    parser.add_argument("--commit-every", type=int, default=100, help="Commit PostgreSQL tous les N enregistrements (défaut: 100)")
    parser.add_argument("--log-every", type=int, default=None, metavar="N", help="Afficher la progression tous les N adresses traitées")
    parser.add_argument("--log-interval", type=float, default=None, metavar="SEC", help="Afficher la progression au plus toutes les SEC secondes")
    parser.add_argument(
        "--proxy-file",
        type=str,
        default=None,
        metavar="FICHIER",
        help="Fichier listant un proxy par ligne (http:// ou socks5://). Alternatif : variable d'env PROXY_LIST (séparée par des virgules). Pour SOCKS5 : pip install requests[socks].",
    )
    args = parser.parse_args()

    if args.update:
        run_update_by_year()
    else:
        run_geocode(
            batch_size=args.batch,
            sleep_seconds=args.sleep,
            workers=args.workers,
            commit_every=args.commit_every,
            log_every=args.log_every,
            log_interval=args.log_interval,
            proxy_file=args.proxy_file,
        )

