#!/usr/bin/env python3
"""
Scraping des annonces de location depuis www.leboncoin.fr → PostgreSQL (foncier.leboncoin_locations_brut).

Récupère les annonces de location immobilière publiées sur Leboncoin pour une
ou plusieurs communes et les insère dans la table foncier.leboncoin_locations_brut.
Les annonces déjà présentes en base sont ignorées (ON CONFLICT DO NOTHING sur url_annonce).

ATTENTION : Leboncoin utilise DataDome (anti-bot agressif). Le scraping peut
être bloqué par CAPTCHA ou bannissement IP. Ce script utilise des headers
réalistes et des délais aléatoires pour minimiser la détection.

Stratégie : extraire le JSON embarqué dans <script id="__NEXT_DATA__"> (SSR Next.js)
plutôt que parser le HTML, ce qui est plus fiable et complet.

Usage :
    python scrap_leboncoin.py --commune Laon --code-postal 02000
    python scrap_leboncoin.py --commune Laon --code-postal 02000 --max-pages 3
    python scrap_leboncoin.py --from-db --max-communes 5
    python scrap_leboncoin.py --from-file communes.csv --max-communes 10
    python scrap_leboncoin.py --dry-run --commune Laon --code-postal 02000
"""

import argparse
import csv
import json
import random
import re
import sys
import time
from datetime import datetime, date as date_type
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote

import requests
from bs4 import BeautifulSoup

SCRIPT_DIR = Path(__file__).resolve().parent

BASE_URL = "https://www.leboncoin.fr"

# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────
_CONFIG_DIRS = (SCRIPT_DIR, SCRIPT_DIR.parent, SCRIPT_DIR.parent.parent)

# Headers réalistes pour éviter la détection DataDome
_HEADERS_TEMPLATES = [
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
        "Cache-Control": "max-age=0",
    },
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "fr,fr-FR;q=0.8,en-US;q=0.5,en;q=0.3",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
    },
    {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "fr-FR,fr;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    },
]


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


# ─────────────────────────────────────────────────────────────────────────────
# HTTP avec anti-détection
# ─────────────────────────────────────────────────────────────────────────────
_session = requests.Session()


def _safe_request(url: str, retries: int = 3) -> Optional[requests.Response]:
    """GET avec délai aléatoire long, retries et rotation des headers complets."""
    for attempt in range(retries):
        try:
            # Délai plus long que pour Castorus/Licitor (DataDome)
            time.sleep(random.uniform(3.0, 7.0))
            headers = random.choice(_HEADERS_TEMPLATES).copy()
            headers["Referer"] = "https://www.leboncoin.fr/"
            _session.headers.update(headers)
            resp = _session.get(url, timeout=30)

            # Vérifier si DataDome a bloqué
            if resp.status_code == 403:
                print(f"  [!] 403 Forbidden (DataDome?) — tentative {attempt + 1}/{retries}")
                if attempt < retries - 1:
                    wait = 15 * (2 ** attempt)
                    print(f"      Pause longue {wait}s...")
                    time.sleep(wait)
                continue

            if resp.status_code == 429:
                print(f"  [!] 429 Too Many Requests — tentative {attempt + 1}/{retries}")
                if attempt < retries - 1:
                    wait = 30 * (2 ** attempt)
                    print(f"      Pause très longue {wait}s...")
                    time.sleep(wait)
                continue

            resp.raise_for_status()
            return resp

        except requests.exceptions.HTTPError as exc:
            if attempt < retries - 1:
                wait = 10 * (2 ** attempt)
                print(f"  [!] {exc} — retry dans {wait}s")
                time.sleep(wait)
            else:
                print(f"  [!] Échec après {retries} tentatives: {exc}")
                return None
        except Exception as exc:
            if attempt < retries - 1:
                wait = 5 * (2 ** attempt)
                print(f"  [!] {exc} — retry dans {wait}s")
                time.sleep(wait)
            else:
                print(f"  [!] Échec après {retries} tentatives: {exc}")
                return None
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Construction des URLs
# ─────────────────────────────────────────────────────────────────────────────
def _build_search_url(commune: str, code_postal: str, page: int = 1) -> str:
    """Construit l'URL de recherche de locations pour une commune."""
    slug = commune.lower().strip().replace(" ", "-").replace("'", "-")
    slug = re.sub(r"[^a-z0-9-]", "", slug)
    url = f"{BASE_URL}/cl/locations/cp_{slug}_{code_postal}/"
    if page > 1:
        url += f"p-{page}"
    return url


def _build_search_url_alt(commune: str, code_postal: str, page: int = 1) -> str:
    """URL alternative via le format /recherche."""
    params = {
        "category": "10",
        "locations": f"{commune}__{code_postal}",
    }
    if page > 1:
        params["page"] = str(page)
    qs = "&".join(f"{k}={quote(v)}" for k, v in params.items())
    return f"{BASE_URL}/recherche?{qs}"


# ─────────────────────────────────────────────────────────────────────────────
# Extraction des données depuis __NEXT_DATA__ JSON
# ─────────────────────────────────────────────────────────────────────────────
def _extract_next_data(html: str) -> Optional[dict]:
    """Extrait le JSON __NEXT_DATA__ depuis le HTML."""
    soup = BeautifulSoup(html, "html.parser")
    script = soup.find("script", id="__NEXT_DATA__")
    if script and script.string:
        try:
            return json.loads(script.string)
        except json.JSONDecodeError:
            pass

    # Fallback : regex
    m = re.search(r'<script\s+id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.S)
    if m:
        try:
            return json.loads(m.group(1))
        except json.JSONDecodeError:
            pass
    return None


def _extract_ads_from_next_data(data: dict) -> List[dict]:
    """Navigue dans __NEXT_DATA__ pour trouver la liste des annonces."""
    # Chemins possibles dans la structure Next.js
    paths = [
        lambda d: d["props"]["pageProps"]["initialProps"]["searchData"]["ads"],
        lambda d: d["props"]["pageProps"]["searchData"]["ads"],
        lambda d: d["props"]["pageProps"]["ads"],
        lambda d: d["props"]["pageProps"]["initialProps"]["ads"],
    ]
    for path_fn in paths:
        try:
            ads = path_fn(data)
            if isinstance(ads, list):
                return ads
        except (KeyError, TypeError):
            continue
    return []


def _extract_pagination_info(data: dict) -> Tuple[int, int]:
    """Retourne (total_annonces, nb_pages) depuis __NEXT_DATA__."""
    paths = [
        lambda d: d["props"]["pageProps"]["initialProps"]["searchData"],
        lambda d: d["props"]["pageProps"]["searchData"],
    ]
    for path_fn in paths:
        try:
            search = path_fn(data)
            total = search.get("total", search.get("total_all", 0))
            # LBC affiche 35 résultats par page
            per_page = search.get("limit", 35)
            nb_pages = (total + per_page - 1) // per_page if per_page > 0 else 1
            return int(total), int(nb_pages)
        except (KeyError, TypeError):
            continue
    return 0, 0


# ─────────────────────────────────────────────────────────────────────────────
# Parsing d'une annonce LBC
# ─────────────────────────────────────────────────────────────────────────────
_TYPE_BIEN_MAP = {
    1: "Maison",
    2: "Appartement",
    3: "Terrain",
    4: "Parking",
    5: "Commerce",
    6: "Local",
    7: "Bureau",
    8: "Loft",
    9: "Château",
}

_DPE_CLASSES = {1: "A", 2: "B", 3: "C", 4: "D", 5: "E", 6: "F", 7: "G"}


def _parse_ad(ad: dict, code_postal: str) -> Optional[dict]:
    """Parse un objet annonce LBC depuis __NEXT_DATA__."""
    try:
        ad_id = str(ad.get("list_id", ad.get("id", "")))
        url = ad.get("url", "")
        if not url and ad_id:
            url = f"{BASE_URL}/ad/locations/{ad_id}.htm"
        elif url and url.startswith("/"):
            url = BASE_URL + url

        if not url:
            return None

        # Attributs de base
        titre = ad.get("subject", ad.get("title", ""))
        prix = ad.get("price", [None])
        loyer = int(prix[0]) if isinstance(prix, list) and prix else (int(prix) if prix else None)

        # Localisation
        location = ad.get("location", {})
        commune = location.get("city", "")
        cp = location.get("zipcode", code_postal)
        code_dept = cp[:2] if cp else None

        # Attributs spécifiques (dans "attributes" array)
        attrs = {}
        for attr in ad.get("attributes", []):
            key = attr.get("key", attr.get("key_label", ""))
            value = attr.get("value", attr.get("value_label", ""))
            attrs[key] = value

        # Type de bien
        re_type = attrs.get("real_estate_type", "")
        if isinstance(re_type, (int, float)):
            type_bien = _TYPE_BIEN_MAP.get(int(re_type), str(re_type))
        elif isinstance(re_type, str) and re_type:
            # String numérique ("2") → lookup dans le mapping
            if re_type.isdigit():
                type_bien = _TYPE_BIEN_MAP.get(int(re_type), re_type)
            else:
                type_bien = re_type.capitalize()
        else:
            type_bien = _guess_type_from_title(titre)

        # Surface, pièces, chambres
        surface = _safe_float(attrs.get("square", attrs.get("surface")))
        nb_pieces = _safe_int(attrs.get("rooms", attrs.get("nb_rooms")))
        nb_chambres = _safe_int(attrs.get("bedrooms", attrs.get("nb_bedrooms")))

        # Charges et meublé
        charges_str = attrs.get("charges_included", attrs.get("charges", ""))
        charges_incluses = None
        charges = None
        loyer_hc = None
        if isinstance(charges_str, str):
            if charges_str.lower() in ("1", "oui", "yes", "true"):
                charges_incluses = True
            elif charges_str.lower() in ("0", "non", "no", "false"):
                charges_incluses = False
        elif isinstance(charges_str, bool):
            charges_incluses = charges_str

        charges_amount = _safe_int(attrs.get("charges_amount", attrs.get("rental_charges")))
        if charges_amount:
            charges = charges_amount
            if loyer and charges_incluses:
                loyer_hc = loyer - charges
            elif loyer and not charges_incluses:
                loyer_hc = loyer

        meuble_str = attrs.get("furnished", attrs.get("is_furnished", ""))
        meuble = None
        if isinstance(meuble_str, str):
            if meuble_str.lower() in ("1", "oui", "yes", "true", "meublé"):
                meuble = True
            elif meuble_str.lower() in ("0", "non", "no", "false", "non meublé"):
                meuble = False
        elif isinstance(meuble_str, bool):
            meuble = meuble_str

        # DPE / GES
        dpe_classe = attrs.get("energy_rate", attrs.get("dpe", None))
        dpe_valeur = _safe_int(attrs.get("energy_value", attrs.get("dpe_value")))
        ges_classe = attrs.get("ges", attrs.get("ges_rate", None))
        ges_valeur = _safe_int(attrs.get("ges_value"))

        if isinstance(dpe_classe, (int, float)):
            dpe_classe = _DPE_CLASSES.get(int(dpe_classe), str(dpe_classe))
        if isinstance(ges_classe, (int, float)):
            ges_classe = _DPE_CLASSES.get(int(ges_classe), str(ges_classe))

        # Annonceur
        owner = ad.get("owner", {})
        annonceur_type = owner.get("type", "")
        if annonceur_type == "pro":
            annonceur_type = "pro"
        elif annonceur_type in ("private", ""):
            annonceur_type = "particulier"
        annonceur_nom = owner.get("name", owner.get("store_name", ""))

        # Date de publication
        date_pub = None
        first_pub = ad.get("first_publication_date", ad.get("index_date", ""))
        if first_pub:
            date_pub = _parse_iso_date(first_pub)

        return {
            "url_annonce": url,
            "ref_leboncoin": ad_id,
            "type_bien": type_bien,
            "titre": titre,
            "commune": commune,
            "code_postal": cp,
            "code_dept": code_dept,
            "nb_pieces": nb_pieces,
            "nb_chambres": nb_chambres,
            "surface": surface,
            "loyer": loyer,
            "loyer_hc": loyer_hc,
            "charges": charges,
            "charges_incluses": charges_incluses,
            "meuble": meuble,
            "dpe_classe": dpe_classe,
            "dpe_valeur": dpe_valeur,
            "ges_classe": ges_classe,
            "ges_valeur": ges_valeur,
            "annonceur_type": annonceur_type,
            "annonceur_nom": annonceur_nom,
            "lien_source": url,
            "description": ad.get("body", ad.get("description", "")),
            "date_publication": date_pub,
        }

    except Exception as exc:
        print(f"  [!] Erreur parsing annonce: {exc}")
        return None


def _guess_type_from_title(titre: str) -> Optional[str]:
    """Devine le type de bien depuis le titre."""
    t = (titre or "").lower()
    if "appartement" in t or "studio" in t or "f1" in t or "f2" in t or "t1" in t or "t2" in t:
        return "Appartement"
    if "maison" in t or "villa" in t or "pavillon" in t:
        return "Maison"
    if "parking" in t or "garage" in t or "box" in t:
        return "Parking"
    if "local" in t or "commerce" in t or "bureau" in t:
        return "Local"
    if "terrain" in t:
        return "Terrain"
    return None


def _safe_int(val: Any) -> Optional[int]:
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _safe_float(val: Any) -> Optional[float]:
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _parse_iso_date(s: str) -> Optional[date_type]:
    """Parse '2026-04-10T14:30:00' → date."""
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).date()
    except (ValueError, TypeError):
        # Fallback DD/MM/YYYY
        m = re.search(r"(\d{2})[-/](\d{2})[-/](\d{4})", s)
        if m:
            try:
                return date_type(int(m.group(3)), int(m.group(2)), int(m.group(1)))
            except ValueError:
                pass
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Fallback : parsing HTML classique (si __NEXT_DATA__ absent)
# ─────────────────────────────────────────────────────────────────────────────
def _extract_ads_from_html(html: str, code_postal: str) -> List[dict]:
    """Fallback : extraire les annonces depuis le HTML si pas de __NEXT_DATA__."""
    soup = BeautifulSoup(html, "html.parser")
    listings = []

    # Chercher les liens vers /ad/locations/
    links = soup.select("a[href*='/ad/'], a[href*='/locations/']")
    for link in links:
        href = link.get("href", "")
        if not href or not re.search(r"/ad/|/locations/\d", href):
            continue
        url = BASE_URL + href if href.startswith("/") else href

        parent = link.find_parent("div") or link.find_parent("article")
        if not parent:
            continue

        text = parent.get_text(" ", strip=True)
        titre = link.get_text(strip=True)

        loyer = None
        m_prix = re.search(r"(\d[\d\s]*)\s*€", text)
        if m_prix:
            loyer = _safe_int(m_prix.group(1).replace(" ", ""))

        surface = None
        m_surf = re.search(r"(\d+)\s*m[²2]", text)
        if m_surf:
            surface = _safe_float(m_surf.group(1))

        nb_pieces = None
        m_pieces = re.search(r"(\d+)\s*(?:pi[èe]ces?|p\.?)", text, re.I)
        if m_pieces:
            nb_pieces = _safe_int(m_pieces.group(1))

        code_dept = code_postal[:2] if code_postal else None

        listings.append({
            "url_annonce": url,
            "ref_leboncoin": re.search(r"/(\d+)\.htm", url).group(1) if re.search(r"/(\d+)\.htm", url) else None,
            "type_bien": _guess_type_from_title(titre),
            "titre": titre,
            "commune": "",
            "code_postal": code_postal,
            "code_dept": code_dept,
            "nb_pieces": nb_pieces,
            "nb_chambres": None,
            "surface": surface,
            "loyer": loyer,
            "loyer_hc": None,
            "charges": None,
            "charges_incluses": None,
            "meuble": None,
            "dpe_classe": None,
            "dpe_valeur": None,
            "ges_classe": None,
            "ges_valeur": None,
            "annonceur_type": None,
            "annonceur_nom": None,
            "lien_source": url,
            "description": None,
            "date_publication": None,
        })

    return listings


# ─────────────────────────────────────────────────────────────────────────────
# Scraping d'une commune
# ─────────────────────────────────────────────────────────────────────────────
def _scrape_commune(commune: str, code_postal: str, max_pages: int = 5) -> List[dict]:
    """Scrape toutes les pages de résultats pour une commune."""
    all_listings = []

    # Essayer les deux formats d'URL
    urls_to_try = [
        _build_search_url(commune, code_postal, 1),
        _build_search_url_alt(commune, code_postal, 1),
    ]

    working_url_builder = None
    for url_builder, url in [(lambda c, cp, p: _build_search_url(c, cp, p), urls_to_try[0]),
                              (lambda c, cp, p: _build_search_url_alt(c, cp, p), urls_to_try[1])]:
        print(f"  Essai {url}")
        resp = _safe_request(url)
        if resp and resp.status_code == 200:
            working_url_builder = url_builder
            break
    else:
        print(f"  [!] Aucune URL fonctionnelle pour {commune}")
        return []

    # Parser la première page
    html = resp.text
    next_data = _extract_next_data(html)

    if next_data:
        ads = _extract_ads_from_next_data(next_data)
        total, nb_pages = _extract_pagination_info(next_data)
        print(f"  {total} annonces trouvées ({nb_pages} pages)")
    else:
        ads = _extract_ads_from_html(html, code_postal)
        nb_pages = 1
        print(f"  [i] Pas de __NEXT_DATA__, fallback HTML : {len(ads)} annonces")

    for ad in ads:
        parsed = _parse_ad(ad, code_postal) if next_data else ad
        if parsed:
            all_listings.append(parsed)

    # Pages suivantes
    pages_to_scrape = min(nb_pages, max_pages)
    for page in range(2, pages_to_scrape + 1):
        url = working_url_builder(commune, code_postal, page)
        print(f"  Page {page}/{pages_to_scrape}...")
        resp = _safe_request(url)
        if not resp:
            print(f"  [!] Abandon page {page}")
            break

        next_data = _extract_next_data(resp.text)
        if next_data:
            ads = _extract_ads_from_next_data(next_data)
        else:
            ads = _extract_ads_from_html(resp.text, code_postal)

        for ad in ads:
            parsed = _parse_ad(ad, code_postal) if next_data else ad
            if parsed:
                all_listings.append(parsed)

        if not ads:
            print(f"  [i] Page {page} vide, arrêt")
            break

    return all_listings


# ─────────────────────────────────────────────────────────────────────────────
# Insertion en base
# ─────────────────────────────────────────────────────────────────────────────
_INSERT_COLS = (
    "url_annonce", "ref_leboncoin", "type_bien", "titre",
    "commune", "code_postal", "code_dept",
    "nb_pieces", "nb_chambres", "surface",
    "loyer", "loyer_hc", "charges", "charges_incluses", "meuble",
    "dpe_classe", "dpe_valeur", "ges_classe", "ges_valeur",
    "annonceur_type", "annonceur_nom", "lien_source", "description",
    "date_publication", "date_scraping",
)

_INSERT_SQL = (
    f"INSERT INTO foncier.leboncoin_locations_brut ({', '.join(_INSERT_COLS)}) "
    f"VALUES ({', '.join(['%s'] * len(_INSERT_COLS))}) "
    "ON CONFLICT (url_annonce) DO NOTHING"
)


def _build_insert_row(listing: dict, now: datetime) -> tuple:
    """Construit le tuple d'insertion."""
    return tuple(
        listing.get(col) if col != "date_scraping" else now
        for col in _INSERT_COLS
    )


# ─────────────────────────────────────────────────────────────────────────────
# Récupération des communes depuis la base
# ─────────────────────────────────────────────────────────────────────────────
def _get_communes_from_db(cur, max_communes: Optional[int] = None) -> List[Tuple[str, str]]:
    """Retourne les (commune, code_postal) distinctes depuis vf_communes."""
    sql = """
        SELECT DISTINCT commune, code_postal
        FROM foncier.vf_communes
        WHERE code_postal IS NOT NULL AND commune IS NOT NULL
        ORDER BY commune
    """
    if max_communes:
        sql += f" LIMIT {max_communes}"
    cur.execute(sql)
    return [(row[0], row[1]) for row in cur.fetchall()]


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="Scraping des annonces de location Leboncoin → PostgreSQL"
    )
    parser.add_argument("--commune", type=str, help="Nom de la commune à scraper")
    parser.add_argument("--code-postal", type=str, help="Code postal de la commune")
    parser.add_argument("--from-db", action="store_true",
                        help="Scraper les communes présentes dans vf_communes")
    parser.add_argument("--from-file", type=str, default=None,
                        help="Fichier CSV (commune,code_postal) de communes à scraper")
    parser.add_argument("--max-communes", type=int, default=None,
                        help="Nombre max de communes à scraper")
    parser.add_argument("--max-pages", type=int, default=5,
                        help="Nombre max de pages par commune (défaut 5)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Afficher sans écrire en base")
    args = parser.parse_args()

    if not args.commune and not args.from_db and not args.from_file:
        parser.error("Spécifiez --commune NOM --code-postal CP, --from-db ou --from-file FICHIER.csv")
    if args.commune and not args.code_postal:
        parser.error("--code-postal requis avec --commune")

    import psycopg2
    from psycopg2.extras import execute_batch

    # ── Connexion DB ──
    db_cfg = _get_db_config()
    conn = psycopg2.connect(**db_cfg)
    conn.autocommit = False

    # ── Liste des communes ──
    communes: List[Tuple[str, str]] = []
    if args.commune:
        communes = [(args.commune, args.code_postal)]
    elif args.from_file:
        with open(args.from_file, encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader, None)
            for row in reader:
                if len(row) >= 2:
                    communes.append((row[0].strip(), row[1].strip()))
        if args.max_communes:
            communes = communes[:args.max_communes]
        print(f"{len(communes)} communes lues depuis {args.from_file}")
    elif args.from_db:
        with conn.cursor() as cur:
            communes = _get_communes_from_db(cur, args.max_communes)
        print(f"{len(communes)} communes trouvées en base")

    if not communes:
        print("Aucune commune à scraper.")
        conn.close()
        return

    # ── Scraping ──
    now = datetime.now()
    total_inserted = 0
    total_skipped = 0
    total_blocked = 0

    try:
        for i, (commune, code_postal) in enumerate(communes, 1):
            print(f"\n[{i}/{len(communes)}] {commune} ({code_postal})")

            listings = _scrape_commune(commune, code_postal, args.max_pages)
            print(f"  {len(listings)} annonces extraites")

            if not listings:
                continue

            rows = [_build_insert_row(l, now) for l in listings]

            if args.dry_run:
                print(f"  [dry-run] {len(rows)} annonces prêtes à insérer")
                for r in rows[:3]:
                    print(f"    {r[0]} | {r[2]} | {r[10]} €/mois | {r[9]} m²")
                if len(rows) > 3:
                    print(f"    ... et {len(rows) - 3} autres")
                total_inserted += len(rows)
            else:
                with conn.cursor() as cur:
                    urls = [r[0] for r in rows]
                    cur.execute(
                        "SELECT url_annonce FROM foncier.leboncoin_locations_brut WHERE url_annonce = ANY(%s)",
                        (urls,)
                    )
                    existing = {row[0] for row in cur.fetchall()}

                    execute_batch(cur, _INSERT_SQL, rows, page_size=200)
                    conn.commit()

                    new_count = len(rows) - len(existing)
                    skip_count = len(existing)
                    total_inserted += new_count
                    total_skipped += skip_count
                    print(f"  {new_count} nouvelles, {skip_count} déjà en base")

            # Pause longue entre communes (DataDome)
            if i < len(communes):
                pause = random.uniform(8.0, 15.0)
                print(f"  Pause {pause:.1f}s...")
                time.sleep(pause)

    except KeyboardInterrupt:
        if not args.dry_run:
            conn.commit()
        print("\n\nInterrompu par l'utilisateur.")
    finally:
        conn.close()

    marker = " (dry-run)" if args.dry_run else ""
    print(f"\nTerminé{marker} : {total_inserted} nouvelles annonces, {total_skipped} ignorées")


if __name__ == "__main__":
    main()
