from __future__ import annotations

import argparse
import itertools
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable

import mysql.connector
import requests
from dotenv import load_dotenv

# Affichage unique de l'avertissement 429 (rate limit BAN)
_rate_limit_warned = False
_rate_limit_lock = threading.Lock()

# Ajustement auto des workers : 429 → réduire de 30 % ; après 30 min → remonter de 30 %
_workers_429_seen = False
_workers_ramp_lock = threading.Lock()
_workers_last_ramp_time = 0.0


load_dotenv()

YEARS = (2020, 2021, 2022, 2023, 2024, 2025)


def get_connection():
    return mysql.connector.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "3306")),
        user=os.getenv("DB_USER", "root"),
        password=os.getenv("DB_PASSWORD", "secret"),
        database=os.getenv("DB_NAME", "foncier"),
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


def _geocode_one(
    row: dict,
    sleep_seconds: float = 0.1,
    proxy_getter: Callable[[], dict[str, str]] | None = None,
) -> tuple:
    """Appelle l'API BAN pour une adresse. Retourne (id, lat, lon) ou (id, None, None)."""
    global _rate_limit_warned, _workers_429_seen
    addr_id = row["id"]
    addr = row["adresse_norm"]
    params = {"q": addr, "limit": 1}
    url = "https://api-adresse.data.gouv.fr/search/"
    for attempt in range(2):
        proxies = proxy_getter() if proxy_getter else None
        try:
            r = requests.get(url, params=params, timeout=15, proxies=proxies)
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
        except requests.RequestException:
            if attempt == 0:
                time.sleep(5)
                continue
            return (addr_id, None, None)
        except Exception:
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


def run_geocode(
    batch_size: int = 1000,
    sleep_seconds: float = 0.1,
    workers: int = 1,
    commit_every: int = 100,
    log_every: int | None = None,
    log_interval: float | None = None,
    proxy_file: str | None = None,
):
    """Géocode toutes les adresses restantes (boucle batch par batch jusqu'à épuisement)."""
    global _workers_429_seen, _workers_last_ramp_time
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
        cur = conn.cursor(dictionary=True)

        cur.execute(
            """
            SELECT id, adresse_norm
            FROM adresses_geocodees
            WHERE latitude = 0 AND longitude = 0
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

        # Batch UPDATE en base
        update_sql = (
            "UPDATE adresses_geocodees "
            "SET latitude = %s, longitude = %s, last_refreshed = NOW() "
            "WHERE id = %s"
        )
        cur = conn.cursor()
        ok, skip = 0, 0
        batch = []
        for addr_id, lat, lon in results:
            if lat is None or lon is None:
                skip += 1
                continue
            batch.append((lat, lon, addr_id))
            if len(batch) >= commit_every:
                cur.executemany(update_sql, batch)
                conn.commit()
                ok += len(batch)
                batch = []
        if batch:
            cur.executemany(update_sql, batch)
            conn.commit()
            ok += len(batch)
        cur.close()
        conn.close()

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

