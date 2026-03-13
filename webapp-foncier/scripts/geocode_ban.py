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

import mysql.connector
import requests
from dotenv import load_dotenv

# Forcer un bundle CA valide pour HTTPS (API BAN) et pour MySQL si connexion SSL.
# Évite OSError quand SSL_CERT_FILE / REQUESTS_CA_BUNDLE pointent vers un chemin invalide (autre logiciel).
try:
    import certifi
    _CA_BUNDLE = certifi.where()
    # Forcer ce bundle pour tout le processus (API BAN + MySQL si SSL)
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

# Dossiers où chercher config.json (script dir, webapp-foncier/, racine projet)
_CONFIG_DIRS = (
    Path(__file__).resolve().parent,           # webapp-foncier/scripts/
    Path(__file__).resolve().parent.parent,   # webapp-foncier/
    Path(__file__).resolve().parent.parent.parent,  # racine projet
)


def _load_db_config() -> dict:
    """Charge la config DB depuis config.json ou variables d'environnement (.env)."""
    for base in _CONFIG_DIRS:
        config_path = base / "config.json"
        if config_path.is_file():
            try:
                with open(config_path, encoding="utf-8") as f:
                    data = json.load(f)
                db = data.get("database") or data
                return {
                    "host": db.get("host"),
                    "port": int(db.get("port")),
                    "user": db.get("user"),
                    "password": db.get("password"),
                    "database": db.get("database"),
                }
            except (json.JSONDecodeError, OSError):
                pass
    # Pas de config.json valide : tout depuis l'environnement
    return {
        "host": os.getenv("DB_HOST", ""),
        "port": int(os.getenv("DB_PORT", "")),
        "user": os.getenv("DB_USER", ""),
        "password": os.getenv("DB_PASSWORD", ""),
        "database": os.getenv("DB_NAME", ""),
    }


def get_connection():
    cfg = _load_db_config()
    return mysql.connector.connect(
        host=cfg["host"],
        port=cfg["port"],
        user=cfg["user"],
        password=cfg["password"],
        database=cfg["database"],
    )


def update_valeursfoncieres_for_year(conn, year: int) -> int:
    """Recopie lat/lon de adresses_geocodees vers valeursfoncieres pour une année."""
    cur = conn.cursor()
    sql = """
        UPDATE valeursfoncieres v
        JOIN adresses_geocodees a
          ON a.adresse_norm = CONCAT_WS(' ',
                COALESCE(v.no_voie, ''),
                COALESCE(v.type_de_voie, ''),
                COALESCE(v.voie, ''),
                v.code_postal,
                v.commune
             )
        SET v.latitude = a.latitude, v.longitude = a.longitude
        WHERE (v.latitude IS NULL OR v.longitude IS NULL)
          AND (a.latitude != 0 OR a.longitude != 0)
          AND v.date_mutation >= %s AND v.date_mutation < %s
    """
    date_min = f"{year}-01-01"
    date_max = f"{year + 1}-01-01"
    cur.execute(sql, (date_min, date_max))
    affected = cur.rowcount
    conn.commit()
    cur.close()
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
    env_list = os.getenv("PROXY_LIST", "")
    if env_list:
        for u in env_list.split(","):
            u = u.strip()
            if u:
                urls.append(u)
    return urls


def _make_proxy_getter(proxy_list: list[str]) -> Callable[[], str | None]:
    """Retourne une fonction qui renvoie le prochain proxy en rotation (round-robin)."""
    it = itertools.cycle(proxy_list) if proxy_list else iter([])
    return lambda: next(it, None)


def _geocode_one(row: dict, sleep_seconds: float, proxy_getter: Callable[[], str | None]) -> tuple:
    """Interroge l'API BAN pour une adresse. Retourne (id, lat, lon) avec lat/lon à None si échec."""
    global _rate_limit_warned
    addr_id = row["id"]
    adresse_norm = row.get("adresse_norm", "")
    if not adresse_norm or not adresse_norm.strip():
        return (addr_id, None, None)
    time.sleep(sleep_seconds)
    proxy = proxy_getter() if proxy_getter else None
    proxies = {"http": proxy, "https": proxy} if proxy else None
    url = "https://api-adresse.data.gouv.fr/search/"
    params = {"q": adresse_norm, "limit": 1}
    try:
        r = requests.get(url, params=params, timeout=10, proxies=proxies)
        if r.status_code == 429:
            with _rate_limit_lock:
                if not _rate_limit_warned:
                    _rate_limit_warned = True
                    print("\n[429] Rate limit API BAN. Réessayez plus tard ou utilisez des proxies.", flush=True)
            return (addr_id, None, None)
        r.raise_for_status()
        data = r.json()
        features = data.get("features") or []
        if not features:
            return (addr_id, None, None)
        coords = features[0].get("geometry", {}).get("coordinates")
        if not coords or len(coords) < 2:
            return (addr_id, None, None)
        lon, lat = float(coords[0]), float(coords[1])
        return (addr_id, lat, lon)
    except requests.RequestException as e:
        # Timeout, ProxyError, ConnectionError, HTTPError (4xx/5xx) : log pour diagnostic
        return (addr_id, None, None)
    except Exception as e:
        # ex. JSONDecodeError si la réponse n'est pas du JSON
        return (addr_id, None, None)


def _log_progress(
    processed: int,
    total: int,
    progress_ok: int,
    progress_skip: int,
    start_time: float,
    log_every: int | None,
    log_interval: float | None,
    last_log_time: float,
    last_log_processed: int,
) -> tuple[float, int]:
    now = time.monotonic()
    elapsed = now - start_time
    do_log = False
    if log_every and processed > 0 and processed % log_every == 0 and processed != last_log_processed:
        do_log = True
    if log_interval and (now - last_log_time) >= log_interval and processed != last_log_processed:
        do_log = True
    if not do_log:
        return last_log_time, last_log_processed
    rate = processed / elapsed if elapsed > 0 else 0
    ts = time.strftime("%H:%M:%S", time.localtime())
    print(f"[{ts}] {processed}/{total} traitées — {progress_ok} OK, {progress_skip} sans résultat — {rate:.1f} adresses/s", flush=True)
    return now, processed


def _ensure_geocode_failed_column(conn) -> None:
    """Vérifie que la colonne geocode_failed existe ; sinon lève une erreur explicite."""
    cur = conn.cursor(buffered=True)
    try:
        cur.execute(
            "SELECT id FROM adresses_geocodees WHERE COALESCE(geocode_failed, 0) = 0 LIMIT 0"
        )
        cur.fetchall()  # consommer le résultat pour éviter "Unread result found"
    except mysql.connector.Error as e:
        if "geocode_failed" in str(e).lower() or "Unknown column" in str(e):
            raise SystemExit(
                "La colonne 'geocode_failed' est absente. Exécutez la migration.\n"
                "PowerShell (à la racine du projet) :\n"
                "  Get-Content .\\webapp-foncier\\sql\\add_geocode_failed.sql -Raw | mysql -u root -p foncier\n"
                "CMD :  cmd /c \"mysql -u root -p foncier < webapp-foncier\\sql\\add_geocode_failed.sql\""
            ) from e
        raise
    finally:
        cur.close()


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
        cur = conn.cursor(dictionary=True, buffered=True)

        cur.execute(
            """
            SELECT id, adresse_norm
            FROM adresses_geocodees
            WHERE latitude = 0 AND longitude = 0
              AND COALESCE(geocode_failed, 0) = 0
            LIMIT %s
            """,
            (batch_size,),
        )
        rows = cur.fetchall()
        cur.close()

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
            "UPDATE adresses_geocodees "
            "SET latitude = %s, longitude = %s, last_refreshed = NOW(), geocode_failed = 0 "
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
                cur = conn.cursor()
                cur.executemany(update_ok_sql, batch_ok)
                conn.commit()
                ok += len(batch_ok)
                batch_ok = []
                cur.close()
        if batch_ok:
            cur = conn.cursor()
            cur.executemany(update_ok_sql, batch_ok)
            conn.commit()
            ok += len(batch_ok)
            cur.close()
        if ids_failed:
            placeholders = ",".join(["%s"] * len(ids_failed))
            cur = conn.cursor()
            cur.execute(
                f"UPDATE adresses_geocodees SET geocode_failed = 1 WHERE id IN ({placeholders})",
                ids_failed,
            )
            conn.commit()
            cur.close()
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
    parser.add_argument("--commit-every", type=int, default=100, help="Commit MySQL tous les N enregistrements (défaut: 100)")
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
