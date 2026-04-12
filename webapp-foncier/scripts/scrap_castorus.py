#!/usr/bin/env python3
"""
Scraping des annonces de mise en vente depuis www.castorus.com → PostgreSQL (foncier.castorus_brut).

Récupère les annonces immobilières publiées sur Castorus pour une ou plusieurs
communes et les insère dans la table foncier.castorus_brut.  Les annonces déjà
présentes en base sont ignorées (ON CONFLICT DO NOTHING sur url_annonce).

Prérequis :
    - Un compte Castorus (gratuit) : identifiants dans config.castorus.json
    - La table foncier.castorus_brut créée (voir sql/postgresql/create_table_castorus_brut.sql)

Usage :
    python scrap_castorus.py --commune Laon --code-postal 02000
    python scrap_castorus.py --commune Laon --code-postal 02000 --with-details
    python scrap_castorus.py --from-db --max-communes 5
    python scrap_castorus.py --dry-run --commune Laon --code-postal 02000
    python scrap_castorus.py --export-communes-cp-court communes_a_rescraper.csv
      → CSV des communes dont le CP en base a moins de 5 chiffres (zéros initiaux perdus)
"""

import argparse
import json
import random
import re
import sys
import time
from datetime import datetime, date as date_type
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

SCRIPT_DIR = Path(__file__).resolve().parent

BASE_URL = "https://www.castorus.com"


def _delimiter_for_csv_sample(sample: str) -> str:
    """Choisit ';' ou ',' d'après la première ligne (fichiers FR souvent en ';')."""
    first = (sample.splitlines() or [""])[0]
    if first.count(";") >= first.count(",") and ";" in first:
        return ";"
    return ","


def _load_communes_from_csv_file(path: str) -> List[Tuple[str, str]]:
    """Lit commune + code_postal (2 premières colonnes), séparateur ; ou , auto."""
    import csv

    with open(path, encoding="utf-8") as f:
        sample = f.read(8192)
        f.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=";,")
            reader = csv.reader(f, dialect)
        except csv.Error:
            reader = csv.reader(f, delimiter=_delimiter_for_csv_sample(sample))
        next(reader, None)  # en-tête ou première ligne ignorée (comportement historique)
        out: List[Tuple[str, str]] = []
        for row in reader:
            if len(row) >= 2:
                a, b = row[0].strip(), row[1].strip()
                if a.lower() == "commune" and b.lower().replace(" ", "_") in (
                    "code_postal",
                    "code-postal",
                ):
                    continue
                out.append((a, b))
    return out


def _normalize_code_postal_fr(cp: Optional[str]) -> str:
    """Normalise un code postal français : uniquement des chiffres, largeur 5 (zfill).

    Les sources (CSV, Excel, vf_communes) perdent souvent le zéro initial (ex. 8230 → 08230).
    Les URLs Castorus attendent toujours 5 chiffres (ex. /recherche/bourg-fidele-08230).
    """
    if cp is None:
        return ""
    digits = "".join(c for c in str(cp).strip() if c.isdigit())
    if not digits:
        return str(cp).strip()
    if len(digits) > 5:
        return digits[:5]
    return digits.zfill(5)


# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────
_CONFIG_DIRS = (SCRIPT_DIR, SCRIPT_DIR.parent, SCRIPT_DIR.parent.parent)

_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
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


def _get_castorus_config() -> dict:
    for base in _CONFIG_DIRS:
        p = base / "config.castorus.json"
        if p.is_file():
            with open(p, encoding="utf-8") as f:
                return json.load(f)
    raise RuntimeError(
        "config.castorus.json introuvable. "
        "Créez-le à partir de config.castorus.json.example avec vos identifiants."
    )


# ─────────────────────────────────────────────────────────────────────────────
# HTTP
# ─────────────────────────────────────────────────────────────────────────────
_session = requests.Session()


def _safe_request(url: str, method: str = "GET", retries: int = 3, **kwargs) -> requests.Response:
    """Requête HTTP avec délai aléatoire, retries et rotation du User-Agent."""
    for attempt in range(retries):
        try:
            time.sleep(random.uniform(1.5, 3.5))
            _session.headers["User-Agent"] = random.choice(_USER_AGENTS)
            if method.upper() == "POST":
                resp = _session.post(url, timeout=30, **kwargs)
            else:
                resp = _session.get(url, timeout=30, **kwargs)
            resp.raise_for_status()
            return resp
        except Exception as exc:
            if attempt < retries - 1:
                wait = 5 * (2 ** attempt)
                print(f"  [!] {exc} — retry dans {wait}s")
                time.sleep(wait)
            else:
                raise


# ─────────────────────────────────────────────────────────────────────────────
# Authentification Castorus
# ─────────────────────────────────────────────────────────────────────────────
_LOGIN_ENDPOINTS = [
    "/connexion",
    "/connection.php",
]


def _login(pseudo: str, password: str) -> bool:
    """Tente de se connecter à Castorus. Retourne True si succès."""
    print(f"Connexion à Castorus (pseudo: {pseudo})...")

    for endpoint in _LOGIN_ENDPOINTS:
        url = BASE_URL + endpoint
        try:
            # D'abord charger la page de connexion (cookies CSRF éventuels)
            _safe_request(url)

            # POST du formulaire
            resp = _safe_request(
                url,
                method="POST",
                data={"pseudo": pseudo, "password": password},
                allow_redirects=True,
            )

            # Vérifier si on est connecté : la page ne contient plus le formulaire de connexion
            # ou contient un indicateur de session (déconnexion, mon compte, etc.)
            body = resp.text.lower()
            if any(kw in body for kw in ("déconnexion", "deconnexion", "mon compte", "mon-compte", "logout")):
                print(f"  Connecté via {endpoint}")
                return True

        except Exception as exc:
            print(f"  [!] Échec connexion via {endpoint}: {exc}")
            continue

    print("  [!] Impossible de se connecter à Castorus.")
    print("      Vérifiez vos identifiants dans config.castorus.json")
    return False


# ─────────────────────────────────────────────────────────────────────────────
# Recherche de commune (API suggestions)
# ─────────────────────────────────────────────────────────────────────────────
def _slug_base(slug: str) -> str:
    """Retire le suffixe code postal d'un slug : 'arcey-21410' → 'arcey', 'x-8230' → 'x'."""
    return re.sub(r"-\d{4,5}$", "", slug)


def _search_commune_slug(commune: str, code_postal: str) -> Optional[str]:
    """Cherche le slug complet Castorus pour une commune via l'API suggestions.

    Retourne le slug complet incluant le code postal (ex: 'laon-02000').
    Gère le cas de communes homonymes avec des codes postaux différents.
    """
    code_postal = _normalize_code_postal_fr(code_postal)
    if not code_postal:
        return None

    try:
        resp = _safe_request(
            f"{BASE_URL}/api/v1/search/suggestions",
            params={"q": commune, "limit": 10},
        )
        data = resp.json()
        results = data if isinstance(data, list) else data.get("results", data.get("data", []))

        # Chercher la commune correspondant au code postal
        for item in results:
            cp_raw = str(item.get("code_postal", item.get("cp", "")))
            cp = _normalize_code_postal_fr(cp_raw)
            if cp == code_postal:
                # Priorité : URL complète de recherche
                urls = item.get("urls", {})
                recherche_url = urls.get("recherche", urls.get("listings", ""))
                if recherche_url:
                    m = re.search(r"/recherche/(.+?)(?:\?|$)", recherche_url)
                    if m:
                        return m.group(1)
                # Fallback : slug → retirer l'éventuel code postal existant et mettre le bon
                slug = item.get("slug")
                if slug:
                    return f"{_slug_base(slug)}-{code_postal}"

        # Pas de match par code postal — ne PAS utiliser le premier résultat
        # car c'est probablement une commune homonyme d'un autre département

    except Exception as exc:
        print(f"  [!] Erreur recherche commune '{commune}': {exc}")

    # Fallback : construire le slug à partir du nom de commune
    slug = commune.lower().strip()
    slug = re.sub(r"[^a-z0-9]+", "-", slug).strip("-")
    full_slug = f"{slug}-{code_postal}"
    print(f"  [i] Slug deviné pour {commune}: {full_slug} (CP normalisé à 5 chiffres)")
    return full_slug


# ─────────────────────────────────────────────────────────────────────────────
# Utilitaires de parsing
# ─────────────────────────────────────────────────────────────────────────────
def _parse_montant(value: Any) -> Optional[int]:
    """Convertit '84 300 EUR' ou '84300' en entier."""
    if not value:
        return None
    s = str(value).strip()
    for ch in ("EUR", "€", "\u00a0", " ", "\u202f"):
        s = s.replace(ch, "")
    s = s.replace(",", ".")
    cleaned = "".join(c for c in s if c.isdigit() or c == ".")
    if not cleaned:
        return None
    try:
        n = float(cleaned)
    except ValueError:
        return None
    result = int(n) if n.is_integer() else int(n) + 1
    BIGINT_MAX = 9223372036854775807
    if result > BIGINT_MAX:
        return None
    return result


def _parse_float(value: Any) -> Optional[float]:
    """Parse '562 EUR/m2' ou '150' en float."""
    if not value:
        return None
    s = str(value).strip()
    for ch in ("EUR/m2", "EUR/m²", "€/m²", "€/m2", "m²", "m2", "%", "\u00a0", " ", "\u202f"):
        s = s.replace(ch, "")
    s = s.replace(",", ".")
    cleaned = "".join(c for c in s if c.isdigit() or c == "." or c == "-")
    if not cleaned or cleaned == "-":
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


_RE_DATE = re.compile(r"(\d{2})[-/](\d{2})[-/](\d{4})")


def _parse_date(text: str) -> Optional[date_type]:
    """Parse '06/04/2026' → date."""
    m = _RE_DATE.search(text or "")
    if not m:
        return None
    dd, mm, yyyy = int(m.group(1)), int(m.group(2)), int(m.group(3))
    try:
        return date_type(yyyy, mm, dd)
    except ValueError:
        return None


_RE_TYPE_BIEN = re.compile(
    r"^(Appartement|Maison|Terrain|Commerce|Local|Immeuble|Parking|Garage|Bureau|Château|Loft|Studio|Duplex|Triplex)",
    re.I,
)


def _extract_type_bien(titre: str) -> Optional[str]:
    """Extrait le type de bien depuis le titre de l'annonce."""
    m = _RE_TYPE_BIEN.match((titre or "").strip())
    if m:
        return m.group(1).capitalize()
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Scraping de la page listing
# ─────────────────────────────────────────────────────────────────────────────
def _scrape_listings(slug: str, code_postal: str) -> List[dict]:
    """Scrape la page de recherche Castorus pour une commune.

    Le slug inclut déjà le code postal (ex: 'laon-02000').
    Retourne la liste des annonces extraites.
    """
    url = f"{BASE_URL}/recherche/{slug}"
    print(f"  Chargement {url}")

    try:
        resp = _safe_request(url)
    except Exception as exc:
        print(f"  [!] Erreur chargement page: {exc}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    listings = []

    # Stratégie 1 : table HTML avec <tr> rows
    tables = soup.find_all("table")
    for table in tables:
        rows = table.find_all("tr")
        for tr in rows:
            tds = tr.find_all("td")
            if len(tds) < 5:
                continue

            listing = _parse_table_row(tds, slug, code_postal)
            if listing:
                listings.append(listing)

    # Stratégie 2 : si pas de table, chercher des cartes/divs
    if not listings:
        listings = _parse_card_layout(soup, slug, code_postal)

    # Stratégie 3 : données JSON embarquées (Alpine.js x-data ou <script>)
    if not listings:
        listings = _parse_json_data(soup, slug, code_postal)

    return listings


def _parse_table_row(tds: list, slug: str, code_postal: str) -> Optional[dict]:
    """Parse une ligne de tableau Castorus.

    Structure attendue (d'après analyse) :
      td[0]: prix (+ indicateur tendance)
      td[1]: lien + titre (ex: <a href="/annonce/...">Appartement 6 pieces 150 m2</a>)
      td[2]: commune
      td[3]: nb pièces
      td[4]: surface
      td[5]: prix/m2
      td[6]: rendement
      td[7]: date
    """
    try:
        # Prix
        prix_text = tds[0].get_text(strip=True)
        prix = _parse_montant(prix_text)

        # Lien + titre
        link = tds[1].find("a") if len(tds) > 1 else None
        url_annonce = None
        titre = None
        ref_castorus = None

        if link:
            href = link.get("href", "")
            url_annonce = BASE_URL + href if href.startswith("/") else href
            titre = link.get_text(strip=True)
            # Extraire la ref : /annonce/laon-02000/ref105718341
            m = re.search(r"ref(\d+)", href)
            if m:
                ref_castorus = m.group(1)
        else:
            # Essayer de trouver le lien ailleurs dans la ligne
            link = tds[0].find("a") or next((td.find("a") for td in tds if td.find("a")), None)
            if link:
                href = link.get("href", "")
                url_annonce = BASE_URL + href if href.startswith("/") else href
                titre = link.get_text(strip=True)
                m = re.search(r"ref(\d+)", href)
                if m:
                    ref_castorus = m.group(1)

        if not url_annonce:
            return None

        # Type de bien (depuis le titre)
        type_bien = _extract_type_bien(titre)

        # Commune
        commune = tds[2].get_text(strip=True) if len(tds) > 2 else None

        # Nb pièces
        nb_pieces = None
        if len(tds) > 3:
            pieces_text = tds[3].get_text(strip=True)
            try:
                nb_pieces = int(pieces_text)
            except (ValueError, TypeError):
                pass

        # Surface
        surface = None
        if len(tds) > 4:
            surface = _parse_float(tds[4].get_text(strip=True))

        # Prix/m²
        prix_m2 = None
        if len(tds) > 5:
            prix_m2 = _parse_float(tds[5].get_text(strip=True))

        # Rendement
        rendement = None
        if len(tds) > 6:
            rendement = _parse_float(tds[6].get_text(strip=True))

        # Date
        date_pub = None
        if len(tds) > 7:
            date_pub = _parse_date(tds[7].get_text(strip=True))
        # Fallback : chercher une date dans toute la ligne
        if not date_pub:
            row_text = " ".join(td.get_text(strip=True) for td in tds)
            date_pub = _parse_date(row_text)

        # Code dept dérivé du code postal
        code_dept = code_postal[:2] if code_postal else None

        return {
            "url_annonce": url_annonce,
            "ref_castorus": ref_castorus,
            "type_bien": type_bien,
            "titre": titre,
            "commune": commune or slug.replace("-", " ").title(),
            "code_postal": code_postal,
            "code_dept": code_dept,
            "nb_pieces": nb_pieces,
            "surface": surface,
            "prix": prix,
            "prix_m2": prix_m2,
            "rendement": rendement,
            "date_publication": date_pub,
        }

    except Exception as exc:
        print(f"  [!] Erreur parsing ligne: {exc}")
        return None


def _parse_card_layout(soup: BeautifulSoup, slug: str, code_postal: str) -> List[dict]:
    """Fallback : parse un layout basé sur des cartes/divs."""
    listings = []

    # Chercher des liens vers /annonce/
    links = soup.select("a[href*='/annonce/']")
    for link in links:
        href = link.get("href", "")
        if not href or "/annonce/" not in href:
            continue

        url_annonce = BASE_URL + href if href.startswith("/") else href
        titre = link.get_text(strip=True)

        m = re.search(r"ref(\d+)", href)
        ref_castorus = m.group(1) if m else None

        # Tenter d'extraire des données du contexte parent
        parent = link.find_parent("div") or link.find_parent("tr") or link.find_parent("li")
        parent_text = parent.get_text(" ", strip=True) if parent else titre

        type_bien = _extract_type_bien(titre)
        prix = _parse_montant(re.search(r"([\d\s.,]+)\s*(?:EUR|€)", parent_text).group(1)) if re.search(r"([\d\s.,]+)\s*(?:EUR|€)", parent_text) else None
        surface = _parse_float(re.search(r"([\d.,]+)\s*m[²2]", parent_text).group(1)) if re.search(r"([\d.,]+)\s*m[²2]", parent_text) else None

        nb_pieces = None
        m_pieces = re.search(r"(\d+)\s*(?:pi[eè]ces?|p\.?|pcs?)", parent_text, re.I)
        if m_pieces:
            nb_pieces = int(m_pieces.group(1))

        code_dept = code_postal[:2] if code_postal else None

        listings.append({
            "url_annonce": url_annonce,
            "ref_castorus": ref_castorus,
            "type_bien": type_bien,
            "titre": titre,
            "commune": slug.replace("-", " ").title(),
            "code_postal": code_postal,
            "code_dept": code_dept,
            "nb_pieces": nb_pieces,
            "surface": surface,
            "prix": prix,
            "prix_m2": round(prix / surface, 2) if prix and surface and surface > 0 else None,
            "rendement": None,
            "date_publication": _parse_date(parent_text),
        })

    return listings


def _parse_json_data(soup: BeautifulSoup, slug: str, code_postal: str) -> List[dict]:
    """Fallback : tenter d'extraire des données JSON depuis les scripts inline."""
    listings = []

    for script in soup.find_all("script"):
        text = script.string or ""
        # Chercher des tableaux JSON d'annonces
        for pattern in [r"annonces\s*[:=]\s*(\[.+?\])", r"listings\s*[:=]\s*(\[.+?\])", r"properties\s*[:=]\s*(\[.+?\])"]:
            m = re.search(pattern, text, re.S)
            if m:
                try:
                    items = json.loads(m.group(1))
                    for item in items:
                        code_dept = code_postal[:2] if code_postal else None
                        listings.append({
                            "url_annonce": BASE_URL + item.get("url", item.get("href", f"/annonce/{slug}-{code_postal}/ref{item.get('id', '')}")),
                            "ref_castorus": str(item.get("id", item.get("ref", ""))),
                            "type_bien": item.get("type", item.get("type_bien")),
                            "titre": item.get("titre", item.get("title", item.get("nom", ""))),
                            "commune": item.get("commune", item.get("ville", slug.replace("-", " ").title())),
                            "code_postal": item.get("code_postal", item.get("cp", code_postal)),
                            "code_dept": code_dept,
                            "nb_pieces": item.get("nb_pieces", item.get("pieces")),
                            "surface": item.get("surface"),
                            "prix": item.get("prix", item.get("price")),
                            "prix_m2": item.get("prix_m2"),
                            "rendement": item.get("rendement", item.get("yield")),
                            "date_publication": _parse_date(str(item.get("date", item.get("date_publication", "")))),
                        })
                except (json.JSONDecodeError, TypeError):
                    pass

    return listings


# ─────────────────────────────────────────────────────────────────────────────
# Scraping page détail (optionnel, pour agence + lien source)
# ─────────────────────────────────────────────────────────────────────────────
def _scrape_detail(url: str) -> dict:
    """Scrape la page détail d'une annonce pour extraire agence, lien source et description."""
    result = {"agence": None, "lien_source": None, "description": None}

    try:
        resp = _safe_request(url)
        soup = BeautifulSoup(resp.text, "html.parser")

        # Agence : chercher dans les meta, schéma, ou texte
        # Schema.org RealEstateListing
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                ld = json.loads(script.string or "")
                if isinstance(ld, dict):
                    # offeredBy ou agent
                    agent = ld.get("offeredBy", ld.get("agent", {}))
                    if isinstance(agent, dict):
                        result["agence"] = agent.get("name")
                    desc = ld.get("description")
                    if desc:
                        result["description"] = desc
            except json.JSONDecodeError:
                pass

        # Lien source : /go/d{id} redirect link
        go_link = soup.find("a", href=re.compile(r"/go/d\d+"))
        if go_link:
            href = go_link.get("href", "")
            result["lien_source"] = BASE_URL + href if href.startswith("/") else href

        # Agence fallback : chercher dans le texte
        if not result["agence"]:
            for el in soup.find_all(["span", "div", "p"]):
                text = el.get_text(strip=True)
                if re.match(r"(?:Agence|Agent|Publi[eé]\s+par)\s*:", text, re.I):
                    result["agence"] = re.sub(r"^(?:Agence|Agent|Publi[eé]\s+par)\s*:\s*", "", text, flags=re.I).strip()
                    break

        # Description fallback
        if not result["description"]:
            desc_el = soup.find("div", class_=re.compile(r"description|detail|annonce", re.I))
            if desc_el:
                result["description"] = desc_el.get_text(" ", strip=True)[:2000]

    except Exception as exc:
        print(f"  [!] Erreur page détail {url}: {exc}")

    return result


# ─────────────────────────────────────────────────────────────────────────────
# Insertion en base
# ─────────────────────────────────────────────────────────────────────────────
_INSERT_COLS = (
    "url_annonce", "ref_castorus", "type_bien", "titre",
    "commune", "code_postal", "code_dept",
    "nb_pieces", "surface", "prix", "prix_m2", "rendement",
    "date_publication", "agence", "lien_source", "description",
    "date_scraping",
)

_INSERT_SQL = (
    f"INSERT INTO foncier.castorus_brut ({', '.join(_INSERT_COLS)}) "
    f"VALUES ({', '.join(['%s'] * len(_INSERT_COLS))}) "
    "ON CONFLICT (url_annonce) DO NOTHING"
)


def _build_insert_row(listing: dict, detail: Optional[dict], now: datetime) -> tuple:
    """Construit le tuple d'insertion depuis un listing + détail optionnel."""
    agence = (detail or {}).get("agence")
    lien_source = (detail or {}).get("lien_source")
    description = (detail or {}).get("description")

    return (
        listing["url_annonce"],
        listing.get("ref_castorus"),
        listing.get("type_bien"),
        listing.get("titre"),
        listing["commune"],
        listing["code_postal"],
        listing.get("code_dept"),
        listing.get("nb_pieces"),
        listing.get("surface"),
        listing.get("prix"),
        listing.get("prix_m2"),
        listing.get("rendement"),
        listing.get("date_publication"),
        agence,
        lien_source,
        description,
        now,
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


def export_communes_cp_moins_de_5_chiffres(out_path: str, db_cfg: dict) -> int:
    """Écrit un CSV (séparateur ;) : commune, code_postal (normalisé), code_postal_brut.

    Utile pour rescraper les communes dont le CP en base a perdu des zéros en tête (ex. 8230).
    Les deux premières colonnes sont directement utilisables avec --from-file.
    """
    import csv

    import psycopg2

    conn = psycopg2.connect(**db_cfg)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT commune, code_postal
                FROM foncier.vf_communes
                WHERE code_postal IS NOT NULL AND commune IS NOT NULL
                ORDER BY commune, code_postal
                """
            )
            rows_out: List[Tuple[str, str, str]] = []
            for commune, cp in cur.fetchall():
                digits = "".join(c for c in str(cp) if c.isdigit())
                if 0 < len(digits) < 5:
                    norm = _normalize_code_postal_fr(cp)
                    rows_out.append((commune, norm, str(cp).strip()))
    finally:
        conn.close()

    with open(out_path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(["commune", "code_postal", "code_postal_brut"])
        w.writerows(rows_out)

    return len(rows_out)


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="Scraping des annonces de vente depuis Castorus → PostgreSQL"
    )
    parser.add_argument("--commune", type=str, help="Nom de la commune à scraper")
    parser.add_argument("--code-postal", type=str, help="Code postal de la commune")
    parser.add_argument("--from-db", action="store_true",
                        help="Scraper les communes présentes dans vf_communes")
    parser.add_argument("--from-file", type=str, default=None,
                        help="Fichier CSV (commune + code_postal) : séparateur ',' ou ';' détecté automatiquement")
    parser.add_argument("--max-communes", type=int, default=None,
                        help="Nombre max de communes à scraper (avec --from-db ou --from-file)")
    parser.add_argument("--with-details", action="store_true",
                        help="Scraper aussi les pages détail (agence, lien source)")
    parser.add_argument("--max-details", type=int, default=50,
                        help="Nombre max de pages détail par commune (défaut 50)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Afficher sans écrire en base")
    parser.add_argument("--no-login", action="store_true",
                        help="Ne pas tenter de se connecter (données limitées)")
    parser.add_argument(
        "--export-communes-cp-court",
        metavar="FICHIER.csv",
        default=None,
        help="Exporte un CSV (;) commune;code_postal;code_postal_brut pour les CP à < 5 chiffres "
        "numériques dans vf_communes, puis quitte (réinjection : --from-file en prenant les 2 1ères colonnes)",
    )
    args = parser.parse_args()

    if args.export_communes_cp_court:
        db_cfg = _get_db_config()
        n = export_communes_cp_moins_de_5_chiffres(args.export_communes_cp_court, db_cfg)
        outp = Path(args.export_communes_cp_court).resolve()
        print(f"{n} ligne(s) écrite(s) : {outp}")
        print("  Réinjection : python scrap_castorus.py --from-file FICHIER.csv")
        print("  (--from-file lit les 2 premières colonnes : commune, code_postal)")
        return

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

    # ── Authentification Castorus ──
    logged_in = False
    if not args.no_login:
        try:
            castorus_cfg = _get_castorus_config()
            logged_in = _login(castorus_cfg["pseudo"], castorus_cfg["password"])
        except Exception as exc:
            print(f"  [!] Erreur config/login Castorus: {exc}")
            print("  [i] Continuation sans authentification (données limitées)")

    # ── Liste des communes à scraper ──
    communes: List[Tuple[str, str]] = []
    if args.commune:
        communes = [(args.commune, args.code_postal)]
    elif args.from_file:
        communes = _load_communes_from_csv_file(args.from_file)
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

    try:
        for i, (commune, code_postal_raw) in enumerate(communes, 1):
            code_postal = _normalize_code_postal_fr(code_postal_raw)
            raw_digits = "".join(c for c in str(code_postal_raw) if c.isdigit())
            if not code_postal:
                print(f"\n[{i}/{len(communes)}] {commune} — [!] code postal invalide : {code_postal_raw!r}")
                continue
            if raw_digits and raw_digits != code_postal:
                print(f"\n[{i}/{len(communes)}] {commune} ({code_postal})  [i] CP normalisé depuis {code_postal_raw!r}")
            else:
                print(f"\n[{i}/{len(communes)}] {commune} ({code_postal})")

            # 1. Trouver le slug
            slug = _search_commune_slug(commune, code_postal)
            if not slug:
                print(f"  [!] Slug introuvable pour {commune}")
                continue

            # 2. Scraper les annonces
            listings = _scrape_listings(slug, code_postal)
            print(f"  {len(listings)} annonces extraites")

            if not listings:
                continue

            # 3. Optionnel : scraper les détails
            details: Dict[str, dict] = {}
            if args.with_details:
                detail_count = min(len(listings), args.max_details)
                print(f"  Scraping de {detail_count} pages détail...")
                for j, listing in enumerate(listings[:detail_count], 1):
                    url = listing["url_annonce"]
                    details[url] = _scrape_detail(url)
                    if j % 10 == 0:
                        print(f"    [{j}/{detail_count}] détails scrapés")

            # 4. Insérer en base
            rows = [
                _build_insert_row(l, details.get(l["url_annonce"]), now)
                for l in listings
            ]

            if args.dry_run:
                print(f"  [dry-run] {len(rows)} annonces prêtes à insérer")
                for r in rows[:5]:
                    print(f"    {r[0]} | {r[2]} | {r[9]} € | {r[8]} m² | {r[12]}")
                if len(rows) > 5:
                    print(f"    ... et {len(rows) - 5} autres")
                total_inserted += len(rows)
            else:
                with conn.cursor() as cur:
                    # Compter les existants avant insertion
                    urls = [r[0] for r in rows]
                    cur.execute(
                        "SELECT url_annonce FROM foncier.castorus_brut WHERE url_annonce = ANY(%s)",
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

            # Pause entre communes
            if i < len(communes):
                pause = random.uniform(3.0, 6.0)
                print(f"  Pause {pause:.1f}s...")
                time.sleep(pause)

    except KeyboardInterrupt:
        if not args.dry_run:
            conn.commit()
        print("\n\nInterrompu par l'utilisateur.")
    finally:
        conn.close()

    marker = " (dry-run)" if args.dry_run else ""
    print(f"\nTerminé{marker} : {total_inserted} nouvelles annonces, {total_skipped} ignorées (déjà en base)")


if __name__ == "__main__":
    main()
